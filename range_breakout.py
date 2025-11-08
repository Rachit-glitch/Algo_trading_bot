#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# -------- Config --------
INPUT_FILE  = Path("/Users/rachitsanjel/ibkr_algo/DIA_15m_6M.csv")
OUTPUT_FILE = Path("/Users/rachitsanjel/ibkr_algo/DIA_range_breakout_1to1_results.xlsx")

ENTRY_TIME  = "09:30:00"
EXIT_TIME   = "11:30:00"

# -------- Load Data --------
df = pd.read_csv(INPUT_FILE, parse_dates=[0])
df.rename(columns={df.columns[0]: "Datetime"}, inplace=True)
df["Date"] = df["Datetime"].dt.date
df["Time"] = df["Datetime"].dt.strftime("%H:%M:%S")

results = []

for date, group in df.groupby("Date"):
    g = group.sort_values("Datetime")
    first = g[g["Time"] == ENTRY_TIME]
    if first.empty:
        continue

    # First 15-min candle
    open_ = first["open"].values[0]
    high_ = first["high"].values[0]
    low_  = first["low"].values[0]
    close_ = first["close"].values[0]

    x = high_ - low_   # candle range
    direction = "LONG" if close_ > open_ else "SHORT"
    entry = close_

    # --- 1:1 setup ---
    if direction == "LONG":
        tp = entry + x      # target = +1x
        sl = entry - x      # stop = -1x
    else:
        tp = entry - x
        sl = entry + x

    # Filter rest of session till 11:30
    session = g[(g["Time"] > ENTRY_TIME) & (g["Time"] <= EXIT_TIME)]

    result, gain = "NO HIT", 0.0
    for _, row in session.iterrows():
        high, low = row["high"], row["low"]
        if direction == "LONG":
            if high >= tp:
                result, gain = "HIT TP", x
                break
            elif low <= sl:
                result, gain = "HIT SL", -x
                break
        else:
            if low <= tp:
                result, gain = "HIT TP", x
                break
            elif high >= sl:
                result, gain = "HIT SL", -x
                break

    results.append({
        "Date": date,
        "Direction": direction,
        "Open": open_,
        "Close": close_,
        "Range (x)": round(x, 2),
        "TP": round(tp, 2),
        "SL": round(sl, 2),
        "Result": result,
        "Gain Points": round(gain, 2),
    })

# -------- Output & Stats --------
res_df = pd.DataFrame(results)
res_df["Cum Gain"] = res_df["Gain Points"].cumsum()

stats = {
    "Total Days": len(res_df),
    "Winning Days": (res_df["Gain Points"] > 0).sum(),
    "Losing Days": (res_df["Gain Points"] < 0).sum(),
    "Win Rate (%)": 100 * (res_df["Gain Points"] > 0).mean(),
    "Mean Gain": res_df["Gain Points"].mean(),
    "Median Gain": res_df["Gain Points"].median(),
    "Total Gain Points": res_df["Gain Points"].sum(),
}

print("\n=== Range Breakout Strategy Summary (1:1 RR) ===")
for k, v in stats.items():
    print(f"{k}: {v:.2f}" if isinstance(v, float) else f"{k}: {v}")

with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as w:
    res_df.to_excel(w, sheet_name="Daily Results", index=False)
    pd.DataFrame([stats]).to_excel(w, sheet_name="Summary", index=False)

print(f"\n✅ Saved results to {OUTPUT_FILE}")