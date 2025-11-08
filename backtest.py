#!/usr/bin/env python3
# backtest_ibkr_stop.py
# Strategy:
#   • Look at first 15m bar (09:30). If green -> LONG; if red -> SHORT; if equal -> SKIP
#   • Enter at 09:45 close.
#   • Exit early at STOP if hit; otherwise exit at 11:30 close.
#
# Adds:
#   • Percentage stop-loss (default 0.5%).
#   • IBKR commissions (Pro Fixed / Pro Tiered / Lite), per-side min & 1% cap (if applicable).
#   • Per-share slippage (per side).
#   • $100 starting equity, fractional shares, compounding.
#
# Output:
#   • results_ibkr_stop.xlsx with detailed trades (costs + stop info) and summary.

from __future__ import annotations
import math, sys, argparse
from pathlib import Path
import pandas as pd
import numpy as np

# -------------------- DEFAULTS (edit if you like) --------------------
CSV_PATH_DEFAULT       = "DIA_15m_6M.csv"      # CSV sits next to this script
OUT_XLS_DEFAULT        = "results_ibkr_stop.xlsx"

FIRST_BAR_TIME         = "09:30"
ENTRY_TIME             = "09:45"
EXIT_TIME              = "11:30"
TZ_LOCAL               = "America/New_York"

# Stop-loss (percentage). Example: 0.005 = 0.5%
STOP_PCT_DEFAULT       = 0.005

# Starting equity & sizing
START_EQUITY           = 100.00     # start with $100
ALLOC_PCT              = 1.00       # allocate 100% of equity per trade
FRACTIONAL_SHARES      = True       # allow fractional shares
MIN_FRACTIONAL_SHARES  = 0.0001

# Slippage (per share per side)
SLIPPAGE_PER_SHARE     = 0.00

# Optional pass-through (reg/exchange/clearing) fees per share per side (for Tiered)
EXTRA_FEES_PER_SHARE   = 0.00

# -------- IBKR Pricing Plan --------
# Choose one: "pro_fixed", "pro_tiered", "lite"
PRICING_PLAN           = "pro_fixed"

# Pro Fixed constants
PF_RATE_PER_SHARE      = 0.005
PF_MIN_PER_ORDER       = 1.00
PF_MAX_RATE_OF_TRADE   = 0.01   # 1% cap (per side)

# Pro Tiered constants
PT_MIN_PER_ORDER       = 0.35
PT_MAX_RATE_OF_TRADE   = 0.01   # 1% cap (per side)
PT_TIERS = [
    (300_000,            0.0035),
    (3_000_000,          0.0020),
    (20_000_000,         0.0015),
    (100_000_000,        0.0010),
    (float("inf"),       0.0005),
]

# Lite constants
LITE_RATE_PER_SHARE    = 0.002
LITE_MIN_PER_ORDER     = 0.003
# --------------------------------------------------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="DIA 15m rule backtest with IBKR pricing and % stop-loss.")
    ap.add_argument("--input", default=CSV_PATH_DEFAULT, help="CSV path (default: DIA_15m_6M.csv)")
    ap.add_argument("--output", default=OUT_XLS_DEFAULT, help="Output xlsx (default: results_ibkr_stop.xlsx)")
    ap.add_argument("--stop", type=float, default=STOP_PCT_DEFAULT, help="Stop-loss percent as decimal (0.005 = 0.5%)")
    return ap.parse_args()

def excel_engine():
    try:
        import xlsxwriter  # noqa
        return "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa
            return "openpyxl"
        except Exception:
            return None

def load_data(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        sys.exit(f"[ERROR] CSV not found: {csv_path.resolve()}")
    df = pd.read_csv(csv_path)
    if "ate" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"ate":"date"})
    need = {"date","open","high","low","close"}
    miss = need - set(df.columns)
    if miss:
        sys.exit(f"[ERROR] Missing columns: {miss}")
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    if df["date"].isna().any():
        bad = df.loc[df["date"].isna(), "date"].head(3).tolist()
        sys.exit(f"[ERROR] Failed to parse some timestamps. Examples: {bad}")
    try:
        df["date_local"] = df["date"].dt.tz_convert(TZ_LOCAL).dt.tz_localize(None)
    except Exception:
        df["date_local"] = df["date"].dt.tz_localize(None)
    df["d"] = df["date_local"].dt.date
    df["t"] = df["date_local"].dt.strftime("%H:%M")
    df["month_key"] = df["date_local"].dt.to_period("M").astype(str)
    return df.sort_values("date_local").reset_index(drop=True)

def rate_for_tiered(monthly_shares_before: float) -> float:
    for threshold, rate in PT_TIERS:
        if monthly_shares_before <= threshold:
            return rate
    return PT_TIERS[-1][1]

def commission_one_side(shares: float, price: float, plan: str, monthly_shares_before: float) -> float:
    if shares <= 0 or price <= 0:
        return 0.0
    trade_value = price * shares
    if plan == "pro_fixed":
        base = PF_RATE_PER_SHARE * shares
        base = max(base, PF_MIN_PER_ORDER)
        base = min(base, PF_MAX_RATE_OF_TRADE * trade_value)
        return base
    if plan == "pro_tiered":
        rate = rate_for_tiered(monthly_shares_before)
        base = rate * shares
        base = max(base, PT_MIN_PER_ORDER)
        base = min(base, PT_MAX_RATE_OF_TRADE * trade_value)
        return base
    if plan == "lite":
        base = LITE_RATE_PER_SHARE * shares
        base = max(base, LITE_MIN_PER_ORDER)
        return base
    raise ValueError(f"Unknown PRICING_PLAN '{plan}'")

def full_side_cost(shares: float, price: float, plan: str, monthly_shares_before: float) -> float:
    comm = commission_one_side(shares, price, plan, monthly_shares_before)
    slip = SLIPPAGE_PER_SHARE * shares
    extra = EXTRA_FEES_PER_SHARE * shares
    return comm + slip + extra

def shares_for_trade(equity: float, price: float) -> float:
    dollars = equity * ALLOC_PCT
    if dollars <= 0 or price <= 0:
        return 0.0
    raw = dollars / price
    if FRACTIONAL_SHARES:
        return max(raw, 0.0)
    return float(max(int(raw), 0))

def run(csv_path: Path, out_xls: Path, stop_pct: float):
    print(f"[INFO] Reading: {csv_path.resolve()}")
    df = load_data(csv_path)

    equity = START_EQUITY
    monthly_vol = {}  # "YYYY-MM" -> cumulative shares this month (for tiered)
    rows = []

    # Precompute day index positions to scan bars between 09:45 and 11:30
    for day, g in df.groupby("d", sort=True):
        g = g.sort_values("date_local")
        fb = g[g["t"] == FIRST_BAR_TIME]
        en = g[g["t"] == ENTRY_TIME]
        ex = g[g["t"] == EXIT_TIME]
        month_key = g["month_key"].iloc[0]
        if month_key not in monthly_vol:
            monthly_vol[month_key] = 0.0

        if fb.empty or en.empty or ex.empty:
            rows.append({
                "date": day, "signal": "skip",
                "first_bar_open": np.nan, "first_bar_close": np.nan,
                "entry_price": np.nan, "exit_price": np.nan, "exit_reason": "missing",
                "shares": 0.0,
                "gross_points": np.nan, "gross_pnl_$": 0.0, "gross_ret_pct": np.nan,
                "entry_cost_$": 0.0, "exit_cost_$": 0.0, "total_cost_$": 0.0,
                "net_pnl_$": 0.0, "net_ret_pct": 0.0,
                "start_equity_$": equity, "end_equity_$": equity,
                "month_key": month_key, "note": "Missing 09:30/09:45/11:30"
            })
            continue

        f_open  = float(fb.iloc[0]["open"])
        f_close = float(fb.iloc[0]["close"])
        entry   = float(en.iloc[0]["close"])
        exit_1130 = float(ex.iloc[0]["close"])

        # Determine signal
        if math.isclose(f_close, f_open):
            rows.append({
                "date": day, "signal": "skip",
                "first_bar_open": f_open, "first_bar_close": f_close,
                "entry_price": np.nan, "exit_price": np.nan, "exit_reason": "doji_skip",
                "shares": 0.0,
                "gross_points": np.nan, "gross_pnl_$": 0.0, "gross_ret_pct": np.nan,
                "entry_cost_$": 0.0, "exit_cost_$": 0.0, "total_cost_$": 0.0,
                "net_pnl_$": 0.0, "net_ret_pct": 0.0,
                "start_equity_$": equity, "end_equity_$": equity,
                "month_key": month_key, "note": "Doji first bar"
            })
            continue

        signal = "long" if f_close > f_open else "short"
        dir_ = 1 if signal == "long" else -1

        # Shares sized to equity
        sh = shares_for_trade(equity, entry)
        if FRACTIONAL_SHARES and sh < MIN_FRACTIONAL_SHARES:
            rows.append({
                "date": day, "signal": "skip",
                "first_bar_open": f_open, "first_bar_close": f_close,
                "entry_price": entry, "exit_price": np.nan, "exit_reason": "too_small",
                "shares": 0.0,
                "gross_points": 0.0, "gross_pnl_$": 0.0, "gross_ret_pct": 0.0,
                "entry_cost_$": 0.0, "exit_cost_$": 0.0, "total_cost_$": 0.0,
                "net_pnl_$": 0.0, "net_ret_pct": 0.0,
                "start_equity_$": equity, "end_equity_$": equity,
                "month_key": month_key, "note": "Equity too small for min fractional lot"
            })
            continue

        # Entry costs
        entry_cost_before = monthly_vol[month_key]
        entry_cost = full_side_cost(sh, entry, PRICING_PLAN, entry_cost_before)
        monthly_vol[month_key] += sh  # add entry side shares

        # Determine stop level
        if signal == "long":
            stop_level = entry * (1 - stop_pct)
        else:
            stop_level = entry * (1 + stop_pct)

        # Scan bars AFTER 09:45 until 11:30 for stop hit
        # Eligible bars: those with times strictly after ENTRY_TIME and up to and including EXIT_TIME
        subsequent = g[(g["date_local"] > g[g["t"] == ENTRY_TIME]["date_local"].iloc[0]) &
                       (g["date_local"] <= g[g["t"] == EXIT_TIME]["date_local"].iloc[0])]
        exit_price = exit_1130
        exit_reason = "time_exit"

        for _, bar in subsequent.iterrows():
            high, low, close = float(bar["high"]), float(bar["low"]), float(bar["close"])
            if signal == "long":
                if low <= stop_level:
                    exit_price = stop_level  # assume stop market fill at level
                    exit_reason = "stop_hit"
                    break
            else:  # short
                if high >= stop_level:
                    exit_price = stop_level
                    exit_reason = "stop_hit"
                    break

        # Gross PnL before exit costs
        gross_points = dir_ * (exit_price - entry)
        gross_ret_pct = dir_ * (exit_price / entry - 1.0)
        gross_pnl_dol = sh * gross_points

        # Exit costs (use exit price actually used)
        exit_cost_before = monthly_vol[month_key]
        exit_cost = full_side_cost(sh, exit_price, PRICING_PLAN, exit_cost_before)
        monthly_vol[month_key] += sh  # add exit side shares

        total_cost = entry_cost + exit_cost
        net_pnl_dol = gross_pnl_dol - total_cost
        deployed = equity * ALLOC_PCT
        net_ret_pct = (net_pnl_dol / deployed) if deployed > 0 else 0.0

        start_eq = equity
        equity = equity + net_pnl_dol  # compound

        rows.append({
            "date": day, "signal": signal,
            "first_bar_open": f_open, "first_bar_close": f_close,
            "entry_price": entry, "exit_price": exit_price, "exit_reason": exit_reason,
            "stop_pct": stop_pct, "stop_level": stop_level,
            "shares": sh,
            "gross_points": gross_points, "gross_pnl_$": gross_pnl_dol, "gross_ret_pct": gross_ret_pct,
            "entry_cost_$": entry_cost, "exit_cost_$": exit_cost, "total_cost_$": total_cost,
            "net_pnl_$": net_pnl_dol, "net_ret_pct": net_ret_pct,
            "start_equity_$": start_eq, "end_equity_$": equity,
            "month_key": month_key, "note": ""
        })

    trades = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    trades["equity_curve_$"] = trades["end_equity_$"].where(trades["end_equity_$"].notna()).ffill()
    trades.loc[trades["equity_curve_$"].isna(), "equity_curve_$"] = START_EQUITY

    # Summary
    valid = trades[trades["signal"].isin(["long","short"])].copy()
    n_trades = len(valid)
    wins = int((valid["net_pnl_$"] > 0).sum())
    losses = int((valid["net_pnl_$"] < 0).sum())
    win_rate = (wins / n_trades * 100.0) if n_trades else np.nan

    ending_equity = float(trades["equity_curve_$"].iloc[-1]) if len(trades) else START_EQUITY
    total_return_pct = (ending_equity / START_EQUITY - 1.0) * 100.0 if START_EQUITY > 0 else np.nan

    rollmax = trades["equity_curve_$"].cummax()
    dd = trades["equity_curve_$"] / rollmax - 1.0
    max_dd_pct = float(dd.min() * 100.0) if len(dd) else np.nan

    stops_hit = int((trades["exit_reason"] == "stop_hit").sum())

    summary = pd.DataFrame({
        "metric": [
            "pricing_plan", "stop_pct", "start_equity_$", "ending_equity_$", "total_return_%",
            "trades", "wins", "losses", "win_rate_%", "stops_triggered", "max_drawdown_%"
        ],
        "value": [
            PRICING_PLAN, stop_pct, round(START_EQUITY, 2), round(ending_equity, 2), round(total_return_pct, 4),
            n_trades, wins, losses,
            round(win_rate, 2) if pd.notnull(win_rate) else np.nan,
            stops_hit, round(max_dd_pct, 4) if pd.notnull(max_dd_pct) else np.nan
        ]
    })

    # Write output
    eng = excel_engine()
    out_xls.parent.mkdir(parents=True, exist_ok=True)
    if eng:
        with pd.ExcelWriter(out_xls, engine=eng) as w:
            trades.to_excel(w, index=False, sheet_name="Daily Trades (Net + Stop)")
            summary.to_excel(w, index=False, sheet_name="Summary")
        print(f"[DONE] Wrote Excel → {out_xls.resolve()}")
    else:
        trades.to_csv("results_ibkr_stop_trades.csv", index=False)
        summary.to_csv("results_ibkr_stop_summary.csv", index=False)
        print("[DONE] Wrote CSVs → results_ibkr_stop_trades.csv, results_ibkr_stop_summary.csv")

    print(f"[ANSWER] Start $100 → End ${ending_equity:.2f}  |  Stop {stop_pct*100:.2f}%  |  Plan {PRICING_PLAN}")

if __name__ == "__main__":
    args = parse_args()
    here = Path(__file__).resolve().parent
    run(here / args.input, here / args.output, stop_pct=float(args.stop))
