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
    col = db["step_33"]

    print("Loading finalized markets...")
    markets = list(col.find({"status": "finalized"}))
    print(f"Loaded {len(markets)} finalized markets.")

    time_periods = [
        (1, "1 day"),
        (7, "1 week"),
        (28, "4 weeks")
    ]
    
    all_data = {}
    
    # Process markets for all time periods
    for days, label in time_periods:
        rows = []
        for m in tqdm(markets, desc=f"Processing {label}", unit="mkt", leave=False):
            result = (m.get("result") or "").lower()
            if result not in {"yes", "no"}:
                continue
            p = get_price_before_close(m, days)
            if p is None:
                continue
            y = 1 if result == "yes" else 0
            rows.append({"ticker": m.get("ticker"), "prob": p, "outcome": y})
        
        df = pd.DataFrame(rows)
        print(f"{label}: {len(df)} valid rows")
        all_data[label] = df

    # Check if we have any data
    total_rows = sum(len(df) for df in all_data.values())
    if total_rows == 0:
        print("No valid data found. Cannot compute calibration.")
        print("Possible reasons:")
        print("  - No finalized markets in the database")
        print("  - Markets missing 'result' field (yes/no)")
        print("  - Markets missing candlestick data or close_time")
        return

    # ---------- Compute calibration for each time period ----------
    n_bins = 10  # Fewer bins for clearer bar charts
    calib_data = {}
    
    for label, df in all_data.items():
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
        
        print(f"\n{label}:")
        print(f"  ECE = {ece:.4f}, MCE = {mce:.4f}, Brier = {brier:.4f}")
        
        calib_data[label] = calib

    # ---------- Create bar chart plots ----------
    colors = {"1 day": "blue", "1 week": "green", "4 weeks": "orange"}
    n_periods = len(calib_data)
    
    fig, axes = plt.subplots(1, n_periods, figsize=(6 * n_periods, 6), sharey=True)
    if n_periods == 1:
        axes = [axes]
    
    for idx, (label, calib) in enumerate(calib_data.items()):
        ax = axes[idx]
        color = colors.get(label, "black")
        
        # Bin centers for x-axis
        bin_centers = calib["mean_p"].values
        bin_width = 1.0 / n_bins
        
        # Plot bars showing predicted vs actual
        x_pos = np.arange(len(calib))
        width = 0.35
        
        # Bars for predicted probability
        bars1 = ax.bar(x_pos - width/2, calib["mean_p"], width, 
                      label="Predicted", color=color, alpha=0.7, edgecolor='black', linewidth=0.5)
        # Bars for actual frequency
        bars2 = ax.bar(x_pos + width/2, calib["freq_yes"], width, 
                      label="Actual", color=color, alpha=0.4, edgecolor='black', linewidth=0.5)
        
        # Perfect calibration line
        ax.plot([-0.5, len(calib)-0.5], [0, 1], "--", color="gray", 
               label="Perfect calibration", linewidth=1.5, alpha=0.5)
        
        # Labels and formatting
        ax.set_xlabel("Probability Bin", fontsize=11)
        ax.set_ylabel("Probability / Frequency" if idx == 0 else "", fontsize=11)
        ax.set_title(f"{label} Before Close\n(ECE: {np.sum(calib['abs_err'] * calib['n']) / calib['n'].sum():.4f})", fontsize=12)
        ax.set_xticks(x_pos)
        ax.set_xticklabels([f"{p:.2f}" for p in calib["mean_p"]], rotation=45, ha='right')
        ax.set_ylim(0, 1)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add sample size annotations on bars
        for i, (bar1, bar2, n) in enumerate(zip(bars1, bars2, calib["n"])):
            height1 = bar1.get_height()
            height2 = bar2.get_height()
            max_height = max(height1, height2)
            if max_height > 0.05:  # Only annotate if bar is tall enough
                ax.text(i, max_height + 0.02, f"n={n}", ha='center', va='bottom', 
                       fontsize=7, rotation=0)
    
    plt.suptitle("Kalshi Market Calibration: Predicted vs Actual (Bar Chart)", 
                fontsize=14, y=1.02)
    plt.tight_layout()
    
    # Save the plot
    output_path = "calibration_bars.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nBar chart saved to: {output_path}")
    
    plt.show()
    
    # ---------- Create additional plot: Calibration Error Bars ----------
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = np.arange(len(time_periods))
    width = 0.25
    
    # Prepare data for grouped bars
    bin_indices = np.arange(n_bins)
    all_errors = []
    period_labels = []
    
    for label, calib in calib_data.items():
        errors = calib["abs_err"].values
        all_errors.append(errors)
        period_labels.append(label)
    
    # Plot error bars for each time period
    for i, (label, errors) in enumerate(zip(period_labels, all_errors)):
        color = colors.get(label, "black")
        # Calculate x positions for each bin across time periods
        x_positions = np.arange(n_bins) * len(time_periods) + i * width
        bars = ax.bar(x_positions, errors, width, label=label, color=color, 
                     alpha=0.7, edgecolor='black', linewidth=0.5)
    
    # Set x-axis labels
    ax.set_xlabel("Probability Bin", fontsize=11)
    ax.set_ylabel("Absolute Calibration Error", fontsize=11)
    ax.set_title("Calibration Error by Bin and Time Period", fontsize=12)
    
    # Set x-tick positions and labels
    bin_centers = []
    for i in range(n_bins):
        bin_centers.append(i * len(time_periods) + width * (len(time_periods) - 1) / 2)
    ax.set_xticks(bin_centers)
    # Use the first calibration data to get bin labels
    if calib_data:
        first_calib = list(calib_data.values())[0]
        ax.set_xticklabels([f"{p:.2f}" for p in first_calib["mean_p"]], rotation=45, ha='right')
    
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    # Save the error plot
    output_path_errors = "calibration_error_bars.png"
    plt.savefig(output_path_errors, dpi=300, bbox_inches='tight')
    print(f"Error bar chart saved to: {output_path_errors}")
    
    plt.show()


if __name__ == "__main__":
    main()

