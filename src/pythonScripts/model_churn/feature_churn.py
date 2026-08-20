import os
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def resolve_repo_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "sample").exists():
            return p
    return start.parents[2]


def compute_user_features(events_path: str = None, output_path: str = None) -> pd.DataFrame:
    repo_root = resolve_repo_root(BASE_DIR)

    if events_path is None:
        events_path = repo_root / "sample" / "events.csv"
    if output_path is None:
        output_path = repo_root / "DB" / "user_value_features.csv"

    print(f"Reading events from: {events_path}")
    df = pd.read_csv(events_path, parse_dates=["timestamp"])

    # Keep only logged-in users (skip anonymous visitors)
    df = df[df["user_id"].notna() & (df["user_id"].astype(str).str.strip() != "")]
    df["user_id"] = df["user_id"].astype(str)

    # Derive session_id from user+date if not present (e.g. events consumed directly from Kafka)
    if "session_id" not in df.columns:
        df["session_id"] = df["user_id"] + "_" + pd.to_datetime(df["timestamp"]).dt.date.astype(str)

    snapshot_time = df["timestamp"].max()

    # ---------------------------------------------------------------------------
    # Per-user aggregations
    # rec_shown/rec_clicked are 0 if source file only contains user-events (not recommendation-actions topic)
    # ---------------------------------------------------------------------------
    agg = df.groupby("user_id").agg(
        total_events=("event_type", "count"),
        view_count=("event_type", lambda x: (x == "view").sum()),
        add_count=("event_type", lambda x: (x == "add_to_cart").sum()),
        remove_count=("event_type", lambda x: (x == "delete_from_cart").sum()),
        transaction_count=("event_type", lambda x: (x == "transaction").sum()),
        refund_count=("event_type", lambda x: (x == "refund_request").sum()),
        rec_shown=("event_type", lambda x: (x == "recommendation_shown").sum()),
        rec_clicked=("event_type", lambda x: (x == "recommendation_clicked").sum()),
        last_event_time=("timestamp", "max"),
        first_event_time=("timestamp", "min"),
        unique_sessions=("session_id", "nunique"),
        unique_products=("product_id", "nunique"),
    ).reset_index()

    # Per-session cart behaviour: sessions where user added but never transacted
    session_events = df[df["event_type"].isin(["add_to_cart", "transaction"])].copy()
    session_summary = session_events.groupby(["user_id", "session_id"])["event_type"].apply(
        lambda x: pd.Series({
            "had_add": (x == "add_to_cart").any(),
            "had_tx": (x == "transaction").any(),
        })
    ).unstack(fill_value=False).reset_index()

    abandon = session_summary[session_summary["had_add"] & ~session_summary["had_tx"]]
    abandon_counts = abandon.groupby("user_id").size().reset_index(name="abandoned_cart_sessions")
    agg = agg.merge(abandon_counts, on="user_id", how="left")
    agg["abandoned_cart_sessions"] = agg["abandoned_cart_sessions"].fillna(0)

    # ---------------------------------------------------------------------------
    # Derived rate features
    # ---------------------------------------------------------------------------
    agg["conversion_rate"] = np.where(
        agg["add_count"] > 0, agg["transaction_count"] / agg["add_count"], 0.0
    )
    agg["removal_rate"] = np.where(
        agg["add_count"] > 0, agg["remove_count"] / agg["add_count"], 0.0
    )
    agg["cart_abandon_rate"] = np.where(
        agg["unique_sessions"] > 0,
        agg["abandoned_cart_sessions"] / agg["unique_sessions"],
        0.0,
    )
    agg["recommendation_ctr"] = np.where(
        agg["rec_shown"] > 0, agg["rec_clicked"] / agg["rec_shown"], 0.0
    )

    last_tx_by_user = (
        df[df["event_type"] == "transaction"]
        .groupby("user_id")["timestamp"]
        .max()
        .rename("last_transaction_time")
    )
    last_tx_by_user = pd.to_datetime(last_tx_by_user).reindex(agg["user_id"])  # align by user_id

    agg["days_since_last_tx"] = (
        (snapshot_time - last_tx_by_user)
        .dt.days
        .fillna(999)
        .clip(upper=999)
        .astype(int)
    )
    agg["active_days"] = (
        (agg["last_event_time"] - agg["first_event_time"]).dt.days.fillna(0) + 1
    )
    agg["events_per_day"] = agg["total_events"] / agg["active_days"]

    # ---------------------------------------------------------------------------
    # Rule-based value label (ground truth for supervised learning)
    # 0 = Low, 1 = Medium, 2 = High
    # ---------------------------------------------------------------------------
    score = (
        agg["transaction_count"] * 3.0
        + agg["conversion_rate"] * 2.0
        + agg["recommendation_ctr"] * 0.5
        - agg["removal_rate"] * 1.5
        - agg["cart_abandon_rate"] * 1.0
        - (agg["refund_count"] / (agg["transaction_count"] + 1)) * 2.0
        - np.log1p(agg["days_since_last_tx"]) * 0.3
    )

    score = pd.to_numeric(score, errors="coerce").fillna(0.0)
    score_rank = score.rank(pct=True, method="average").fillna(0.5)
    agg["value_label"] = np.select(
        [score_rank <= 0.33, score_rank <= 0.66],
        [0, 1],
        default=2,
    ).astype(int)
    agg["value_score"] = score

    feature_cols = [
        "user_id", "transaction_count", "add_count", "remove_count", "view_count",
        "refund_count", "unique_sessions", "unique_products", "conversion_rate",
        "removal_rate", "cart_abandon_rate", "recommendation_ctr",
        "days_since_last_tx", "events_per_day", "active_days",
        "value_score", "value_label",
    ]
    result = agg[feature_cols]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result.to_csv(output_path, index=False)
    print(f"Saved {len(result)} user feature rows to: {output_path}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--output-path", default=None)
    args = parser.parse_args()
    compute_user_features(events_path=args.events_path, output_path=args.output_path)
