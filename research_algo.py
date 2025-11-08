#!/usr/bin/env python3
"""
Point-based continuation (DIA 15m) with a fixed stop in points.

Rule:
- First bar = 09:30–09:45 ET.
- Entry = Close of first bar (09:45 time).
- Direction:
    GREEN (close > open) => measure up (long-bias)
    RED   (close < open) => measure down (short-bias)
    DOJI  (close == open) => sign=0 (skip or treat as 0)
- Exit = Close of 11:30 bar.
- Stop (points): If price moves >= stop_pts against direction at any time from 10:00..11:30,
  record gain = -stop_pts instead of the normal gain.

Outputs:
- Excel: 'Daily' (per day details) and 'Summary' (stats), no P/L or fees—points only.
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

AFTER_TIMES = ["10:00","10:15","10:30","10:45","11:00","11:15","11:30"]

def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    ts_col = "date" if "date" in df.columns else ("datetime" if "datetime" in df.columns else None)
    if ts_col is None:
        raise ValueError("CSV must have a 'date' or 'datetime' column.")

    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df["date_et"] = df[ts_col].dt.tz_convert("America/New_York")
    df["day"] = df["date_et"].dt.date
    df["time_et"] = df["date_et"].dt.strftime("%H:%M")

    need = {"open","high","low","close"}
    if not need.issubset(set(df.columns)):
        raise ValueError(f"Missing columns {sorted(need)}; got {df.columns.tolist()}")
    return df

def compute_daily_points(df: pd.DataFrame, stop_pts: float = 1.0, drop_doji: bool=False) -> pd.DataFrame:
    rows = []
    for day, g in df.groupby("day"):
        g = g.sort_values("date_et")
        first = g[g["time_et"] == "09:30"]
        exitb = g[g["time_et"] == "11:30"]
        if first.empty or exitb.empty:
            continue

        f = first.iloc[0]
        e = exitb.iloc[0]

        o0930 = float(f["open"])
        c0930 = float(f["close"])
        entry_px = c0930
        exit_px = float(e["close"])

        # Determine direction
        if c0930 > o0930:
            sign = 1
            signal = "GREEN → measure up"
        elif c0930 < o0930:
            sign = -1
            signal = "RED → measure down"
        else:
            sign = 0
            signal = "DOJI → no measurement"

        if drop_doji and sign == 0:
            continue

        # Check stop across subsequent bars up to 11:30
        intrawindow = g[g["time_et"].isin(AFTER_TIMES)].copy()
        intrawindow = intrawindow.sort_values("date_et")

        stop_hit = False
        stop_time = None

        if sign == 1:
            # Long-bias: stop if low <= entry - stop_pts
            for _, r in intrawindow.iterrows():
                low = float(r["low"])
                if low <= entry_px - stop_pts:
                    stop_hit = True
                    stop_time = r["time_et"]
                    break
        elif sign == -1:
            # Short-bias: stop if high >= entry + stop_pts
            for _, r in intrawindow.iterrows():
                high = float(r["high"])
                if high >= entry_px + stop_pts:
                    stop_hit = True
                    stop_time = r["time_et"]
                    break

        if stop_hit:
            gain_pts = -float(stop_pts)
            exit_used = f"STOP@{stop_time}"
            exit_px_used = np.nan  # we don't know exact fill; modeling as fixed -stop
        else:
            # No stop: use 11:30 close in direction-normalized way
            gain_pts = (exit_px - entry_px) if sign == 1 else ((entry_px - exit_px) if sign == -1 else 0.0)
            exit_used = "11:30"
            exit_px_used = exit_px

        rows.append({
            "Date": day,
            "FirstBar_Open(09:30)": o0930,
            "FirstBar_Close(09:30)": c0930,
            "Entry_Time": "09:45",
            "Entry_Px": entry_px,
            "Exit": exit_used,                # "STOP@HH:MM" or "11:30"
            "Exit_Px(if no stop)": exit_px_used,
            "Signal": signal,
            "Sign": sign,
            "Stop_Points": stop_pts,
            "Stop_Hit": bool(stop_hit),
            "Gain_Points": float(gain_pts)
        })

    daily = pd.DataFrame(rows).sort_values("Date").reset_index(drop=True)
    if not daily.empty:
        daily["Cumulative_Gain_Points"] = daily["Gain_Points"].cumsum()
    return daily

def summarize_points(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame({"Metric": [], "Value": []})

    s = daily["Gain_Points"].astype(float)
    n = int(s.shape[0])
    pos = int((s > 0).sum())
    neg = int((s < 0).sum())
    zero = int((s == 0).sum())
    pos_pct = (100.0 * pos / n) if n else np.nan
    stop_days = int(daily["Stop_Hit"].sum())

    # Split by first-bar color
    green = s[daily["Sign"] == 1]
    red   = s[daily["Sign"] == -1]

    rows = [
        ("N (days)", n),
        ("Sum (points)", s.sum()),
        ("Mean (points)", s.mean()),
        ("Median (points)", s.median()),
        ("Std Dev (points)", s.std(ddof=1) if n > 1 else np.nan),
        ("Skewness", s.skew() if n > 2 else np.nan),
        ("Excess Kurtosis", s.kurt() if n > 3 else np.nan),
        ("Min (points)", s.min()),
        ("Max (points)", s.max()),
        ("Positive days (%)", pos_pct),
        ("Positive days (count)", pos),
        ("Negative days (count)", neg),
        ("Zero days (count)", zero),
        ("Stop hit (count)", stop_days),
        ("Stop hit (%)", (100.0 * stop_days / n) if n else np.nan),
        ("Green-first mean (pts)", green.mean() if len(green) else np.nan),
        ("Red-first mean (pts)", red.mean() if len(red) else np.nan),
    ]
    return pd.DataFrame(rows, columns=["Metric","Value"])

def main():
    ap = argparse.ArgumentParser(description="DIA 15m continuation (points) with 1-point stop.")
    ap.add_argument("-i", "--input", default="dia_15m_6m.csv", help="Path to CSV")
    ap.add_argument("-o", "--output", default="DIA_continuation_points_with_stop.xlsx", help="Output Excel")
    ap.add_argument("--stop", type=float, default=1.0, help="Stop size in points (default: 1.0)")
    ap.add_argument("--drop-doji", action="store_true", help="Drop days with first bar close == open")
    args = ap.parse_args()

    df = load_data(Path(args.input))
    daily = compute_daily_points(df, stop_pts=args.stop, drop_doji=args.drop_doji)
    summary = summarize_points(daily)

    # Console
    print("\n=== Continuation (Point Gains) with Stop Summary ===")
    with pd.option_context("display.max_rows", None, "display.max_colwidth", 180):
        print(summary.to_string(index=False))
    if not daily.empty:
        print(f"\nTotal points: {daily['Gain_Points'].sum():.3f}")
        print(f"Stops hit: {int(daily['Stop_Hit'].sum())} / {daily.shape[0]} days")

    # Excel
    with pd.ExcelWriter(args.output, engine="xlsxwriter") as w:
        daily.to_excel(w, sheet_name="Daily", index=False)
        summary.to_excel(w, sheet_name="Summary", index=False)

    print(f"\n[OK] Wrote: {Path(args.output).resolve()}")

if __name__ == "__main__":
    main()