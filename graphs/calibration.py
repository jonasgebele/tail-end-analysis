import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

# ---------- Helper Functions ----------

def parse_iso(ts_str):
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None

def get_price_before_close(market, days):
    """Return yes-price (0–1) from candlesticks ~N days before close."""
    close_dt = parse_iso(market.get("close_time"))
    if not close_dt:
        return None
    target_ts = int((close_dt - timedelta(days=days)).timestamp())
    
    cands = market.get("candlesticks", [])
    if not cands:
        return None
    
    # Find closest candlestick BEFORE or equal to target_ts
    best = None
    best_diff = float("inf")
    for c in cands:
        ts = c.get("end_period_ts")
        if not ts:
            continue
        diff = target_ts - ts
        if diff >= 0 and diff < best_diff:
            best = c
            best_diff = diff
    if not best:
        return None

    close_dollars = best.get("price", {}).get("close_dollars")
    if close_dollars is None:
        return None
    try:
        p = float(close_dollars)
        return np.clip(p, 0.0, 1.0)
    except Exception:
        return None


# ---------- Main ----------

def main():
    client = MongoClient("mongodb://localhost:27017")
    db = client["tail-end-analysis"]
    
    collections = ["step_33"]
    time_periods = [
        (1, "1 day"),
        (7, "1 week"),
        (28, "4 weeks")
    ]
    
    all_data = {}  # Key: (collection, time_label)
    
    # Process each collection
    for col_name in collections:
        col = db[col_name]
        print(f"\n=== Processing collection: {col_name} ===")
        print("Loading finalized markets...")
        markets = list(col.find({"status": "finalized"}))
        print(f"Loaded {len(markets)} finalized markets.")
        
        # Process markets for all time periods
        for days, time_label in time_periods:
            rows = []
            for m in tqdm(markets, desc=f"Processing {col_name} - {time_label}", unit="mkt", leave=False):
                result = (m.get("result") or "").lower()
                if result not in {"yes", "no"}:
                    continue
                p = get_price_before_close(m, days)
                if p is None:
                    continue
                y = 1 if result == "yes" else 0
                rows.append({"ticker": m.get("ticker"), "prob": p, "outcome": y})
            
            df = pd.DataFrame(rows)
            print(f"{col_name} - {time_label}: {len(df)} valid rows")
            all_data[(col_name, time_label)] = df

    # Check if we have any data
    total_rows = sum(len(df) for df in all_data.values())
    if total_rows == 0:
        print("No valid data found. Cannot compute calibration.")
        print("Possible reasons:")
        print("  - No finalized markets in the database")
        print("  - Markets missing 'result' field (yes/no)")
        print("  - Markets missing candlestick data or close_time")
        return

    # ---------- Compute calibration for each collection and time period ----------
    n_bins = 20
    calib_data = {}
    
    for (col_name, time_label), df in all_data.items():
        if len(df) == 0:
            continue
            
        df["bin"] = pd.cut(df["prob"], bins=np.linspace(0, 1, n_bins + 1), include_lowest=True)
        calib = (
            df.groupby("bin")
              .agg(mean_p=("prob", "mean"), freq_yes=("outcome", "mean"), n=("outcome", "count"))
              .reset_index()
        )
        
        # Compute metrics
        calib["abs_err"] = (calib["mean_p"] - calib["freq_yes"]).abs()
        ece = np.sum(calib["abs_err"] * calib["n"]) / calib["n"].sum()
        mce = calib["abs_err"].mean()
        brier = ((df["prob"] - df["outcome"]) ** 2).mean()
        
        label = f"{col_name} - {time_label}"
        print(f"\n{label}:")
        print(f"  ECE = {ece:.4f}, MCE = {mce:.4f}, Brier = {brier:.4f}")
        
        calib_data[label] = calib

    # ---------- Plot ----------
    plt.figure(figsize=(8, 6))
    
    # Define colors for time periods
    colors = {"1 day": "blue", "1 week": "green", "4 weeks": "orange"}
    
    for label, calib in calib_data.items():
        # Extract time label (removing "step_33 - " prefix)
        time_label = label.replace("step_33 - ", "")
        color = colors.get(time_label, "black")
        
        plt.plot(calib["mean_p"], calib["freq_yes"], "o-", 
                label=time_label, color=color, markersize=5, linewidth=2)
    
    plt.plot([0, 1], [0, 1], "--", color="gray", label="Perfect calibration", linewidth=1.5, alpha=0.5)
    plt.xlabel("Predicted probability")
    plt.ylabel("Empirical frequency of 'Yes'")
    plt.title("Kalshi Market Calibration (Multiple Time Periods Before Resolution)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save the plot
    output_path = "calibration_plot.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")
    
    plt.show()


if __name__ == "__main__":
    main()