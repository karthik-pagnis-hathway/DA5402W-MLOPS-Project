import os
import pickle
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlflow_registry import promote_if_best
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import (
    classification_report,
    accuracy_score,
    f1_score,
    confusion_matrix,
)
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

BASE_DIR = Path(__file__).resolve().parent

FEATURE_COLS = [
    "transaction_count", "add_count", "remove_count", "view_count",
    "refund_count", "unique_sessions", "unique_products", "conversion_rate",
    "removal_rate", "cart_abandon_rate", "recommendation_ctr",
    "days_since_last_tx", "events_per_day", "active_days",
]
LABEL_MAP = {0: "Low", 1: "Medium", 2: "High"}


def resolve_repo_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "sample").exists():
            return p
    return start.parents[2]


def train(
    features_path: str = None,
    model_output_path: str = None,
    mlflow_tracking_uri: str = None,
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 100,
    max_depth: int = 8,
):
    repo_root = resolve_repo_root(BASE_DIR)

    if features_path is None:
        features_path = repo_root / "DB" / "user_value_features.csv"
    if model_output_path is None:
        model_output_path = repo_root / "artifacts" / "churn_model.pkl"
    if mlflow_tracking_uri is None:
        mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment("customer_value_scoring")

    print(f"Loading features from: {features_path}")
    df = pd.read_csv(features_path)
    df = df.dropna(subset=["value_label"])

    X = df[FEATURE_COLS].fillna(0)
    y = df["value_label"].astype(int)

    print(f"Dataset: {len(df)} users | Label distribution:\n{y.value_counts().sort_index()}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    with mlflow.start_run(run_name="customer_value_rf"):
        params = {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "random_state": random_state,
            "test_size": test_size,
        }
        mlflow.log_params(params)

        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("clf", RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state,
                class_weight="balanced",
                n_jobs=-1,
            )),
        ])

        pipeline.fit(X_train, y_train)

        y_pred = pipeline.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        f1_macro = f1_score(y_test, y_pred, average="macro")
        f1_weighted = f1_score(y_test, y_pred, average="weighted")

        # Cross-validation score on full dataset
        cv_scores = cross_val_score(pipeline, X, y, cv=5, scoring="f1_macro")

        mlflow.log_metrics({
            "accuracy": acc,
            "f1_macro": f1_macro,
            "f1_weighted": f1_weighted,
            "cv_f1_mean": cv_scores.mean(),
            "cv_f1_std": cv_scores.std(),
        })

        # Log per-class precision/recall
        report = classification_report(y_test, y_pred, output_dict=True)
        for cls_idx, cls_name in LABEL_MAP.items():
            cls_key = str(cls_idx)
            if cls_key in report:
                mlflow.log_metrics({
                    f"{cls_name}_precision": report[cls_key]["precision"],
                    f"{cls_name}_recall": report[cls_key]["recall"],
                    f"{cls_name}_f1": report[cls_key]["f1-score"],
                })

        # Feature importance
        importances = pipeline.named_steps["clf"].feature_importances_
        for feat, imp in sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1]):
            mlflow.log_metric(f"importance_{feat}", round(float(imp), 4))

        print(f"\nAccuracy: {acc:.4f} | F1 Macro: {f1_macro:.4f} | CV F1: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

        unique_test_labels = sorted(pd.unique(y_test))
        if len(unique_test_labels) == 1:
            print(f"\nClassification Report (single-class validation split):\n{classification_report(y_test, y_pred, labels=unique_test_labels, target_names=[LABEL_MAP[i] for i in unique_test_labels])}")
        else:
            print(f"\nClassification Report:\n{classification_report(y_test, y_pred, labels=[0, 1, 2], target_names=['Low','Medium','High'])}")

        # Save and log model artifact
        os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
        payload = {
            "pipeline": pipeline,
            "feature_cols": FEATURE_COLS,
            "label_map": LABEL_MAP,
            "metrics": {"accuracy": acc, "f1_macro": f1_macro},
        }
        with open(model_output_path, "wb") as f:
            pickle.dump(payload, f)

        mlflow.log_artifact(str(model_output_path), artifact_path="model")
        mlflow.sklearn.log_model(
            pipeline,
            artifact_path="sklearn_model",
            registered_model_name="CustomerValueScorer",
            input_example=X_test.iloc[:3],
        )

        run_id = mlflow.active_run().info.run_id
        print(f"\nMLflow run: {run_id}")
        print(f"Model saved to: {model_output_path}")

        promote_if_best(
            model_name="CustomerValueScorer",
            new_run_id=run_id,
            metric_name="f1_macro",
            higher_is_better=True,
        )

    return {"accuracy": acc, "f1_macro": f1_macro, "run_id": run_id}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-path", default=None)
    parser.add_argument("--model-output", default=None)
    parser.add_argument("--mlflow-uri", default=None)
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument("--test-size", type=float, default=0.2)
    args = parser.parse_args()

    train(
        features_path=args.features_path,
        model_output_path=args.model_output,
        mlflow_tracking_uri=args.mlflow_uri,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        test_size=args.test_size,
    )
