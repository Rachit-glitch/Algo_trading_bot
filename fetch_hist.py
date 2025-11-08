


from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from ib_insync import IB, Future, util

HOST, PORT, CID = "127.0.0.1", 7497, 23  # change CID if needed
DURATION = "6 M"
BAR_SIZE = "15 mins"
USE_RTH = False        # futures: include the full 23h Globex
WHAT_SEQ = ["TRADES", "MIDPOINT"]  # try TRADES first, then fallback
OUT = Path("Dia_15m_6m.csv")

def pick_front_mym(ib: IB) -> Future:
    # Try CBOT first; some installs list under CME, so try that next.
    for exch in ("CBOT", "CME"):
        tmpl = Future(symbol="", exchange=exch, currency="USD")
        details = ib.reqContractDetails(tmpl)
        if not details:
            continue
        # pick the nearest non-expired contract by lastTradeDateOrContractMonth
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        cands = []
        for d in details:
            c = d.contract
            exp = c.lastTradeDateOrContractMonth or ""
            if exp and exp >= today[:len(exp)]:  # keep today or later
                cands.append(c)
        if not cands:
            cands = [d.contract for d in details]
        cands.sort(key=lambda c: c.lastTradeDateOrContractMonth or "99999999")
        front = ib.qualifyContracts(cands[0])[0]
        return front
    raise RuntimeError("No MYM contract details found on CBOT or CME. Check symbol/exchange/permissions.")

def fetch_bars(ib: IB, contract: Future, what: str):
    print(f"[INFO] Requesting {DURATION} {BAR_SIZE} ({what}) useRTH={USE_RTH} …")
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=DURATION,
            barSizeSetting=BAR_SIZE,
            whatToShow=what,
            useRTH=USE_RTH,
            formatDate=1,
            keepUpToDate=False,
        )
        return util.df(bars)
    except Exception as e:
        print(f"[WARN] reqHistoricalData failed for {what}: {e}")
        return None

def main():
    ib = IB()
    ib.RequestTimeout = 120  # be generous
    print(f"[INFO] Connecting {HOST}:{PORT} clientId={CID} …")
    ib.connect(HOST, PORT, clientId=CID, readonly=True)

    # If you don't have live CME data, use delayed:
    ib.reqMarketDataType(3)  # 1=real, 3=delayed

    front = pick_front_mym(ib)
    print(f"[INFO] Using front contract: {front.symbol} {front.lastTradeDateOrContractMonth} @ {front.exchange} (localSymbol={front.localSymbol})")

    df = None
    for what in WHAT_SEQ:
        df = fetch_bars(ib, front, what)
        if df is not None and len(df) > 0 and not ((df["volume"] == 0).all() if "volume" in df.columns else False):
            out = OUT
            df.to_csv(out, index=False)
            print(f"[DONE] Saved {out.resolve()}  rows={len(df)}  whatToShow={what}")
            break

    if df is None or len(df) == 0:
        print("[ERROR] IBKR returned no bars. Likely causes:")
        print("  • No CME data permissions (enable delayed data or subscribe).")
        print("  • Asking for too long a duration with small bar size (pacing).")
        print("  • Network/API hiccup; try again or shorten duration (e.g., '3 M').")

    ib.disconnect()

if __name__ == "__main__":
    main()
