# ==============================================================================
# Apriori Model Execution & Pickle Serialization Script
# ==============================================================================
# Requirements: pip install pandas mlxtend
# Description:
# 1. Reads 'extracted_features.csv' from local machine.
# 2. Preprocesses cart_product_id_list into a transaction basket.
# 3. Applies Apriori to extract frequent itemsets and association rules.
# 4. Saves the association rules to 'apriori_rules.csv'.
# 5. Serializes and saves the trained Apriori model / rules to 'apriori_model.pkl'.
# ==============================================================================

import pandas as pd
import json
import os
import pickle
import argparse
from pathlib import Path
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sklearn.model_selection import train_test_split

try:
    import mlflow
    import mlflow.pyfunc
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit("mlflow is not installed. Install it with: pip install mlflow") from exc

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlflow_registry import promote_if_best

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def resolve_repo_root(start_file: Path) -> Path:
    current = start_file.resolve()
    candidates = [
        Path("/opt/airflow"),
        Path("/artifacts"),
        Path("/workspace"),
        Path("/app"),
        *[current, *current.parents],
    ]
    for parent in candidates:
        if (parent / "artifacts").exists() or (parent / "DB").exists() and (parent / "src").exists():
            return parent
    if Path("/opt/airflow").exists():
        return Path("/opt/airflow")
    return Path.cwd().resolve()

def run_apriori_and_save(
    features_csv: str = "extracted_features.csv", 
    min_support: float = 0.001, 
    min_confidence: float = 0.1,
    rules_output_csv: str = "apriori_rules.csv",
    model_pickle_file: str = "apriori_model.pkl",
    test_size: float = 0.3,
    random_state: int = 42,
    mlflow_tracking_uri: str = None,
):
    """
    Applies Apriori algorithm to extracted_features.csv, exports rules to CSV,
    saves the model pickle, tracks metrics in MLflow, and promotes to registry
    if avg_lift beats the current @champion.
    """
    repo_root = resolve_repo_root(Path(__file__))
    if features_csv is None:
        features_csv = repo_root / "DB" / "extracted_features.csv"
    if model_pickle_file is None:
        model_pickle_file = repo_root / "artifacts" / "apriori_model.pkl"
    model_pickle_file = Path(model_pickle_file)
    model_pickle_file.parent.mkdir(parents=True, exist_ok=True)

    if mlflow_tracking_uri is None:
        mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment("apriori_recommendation")
    if rules_output_csv is None:
        rules_output_csv = repo_root / "artifacts" / "apriori_rules.csv"
    rules_output_csv = Path(rules_output_csv)
    rules_output_csv.parent.mkdir(parents=True, exist_ok=True)
    if not os.path.exists(features_csv):
        print(f"Error: '{features_csv}' not found in current directory ({os.getcwd()}).")
        print("Please place 'extracted_features.csv' in the same folder as this script.")
        return

    print(f"Loading extracted dataset: {features_csv}...")
    df = pd.read_csv(features_csv)

    # 1. Parse JSON cart_product_id_list strings into Python lists
    transactions = []
    for item in df['cart_product_id_list']:
        if isinstance(item, str):
            try:
                p_list = json.loads(item)
            except:
                p_list = [p.strip(" []'\"") for p in item.split(",") if p.strip(" []'\"")]
        elif isinstance(item, list):
            p_list = item
        else:
            p_list = []
            
        if p_list:
            transactions.append([str(p) for p in p_list if p])

    total_transactions = len(transactions)
    print(f"Total valid baskets found: {total_transactions}")
    if total_transactions < 2:
        print("Not enough transactions for train/test split.")
        return

    # 2. Perform 70:30 Train/Test Split
    # We set shuffle=True to ensure randomness (recommended unless temporal order matters)
    train_transactions, test_transactions = train_test_split(
        transactions, test_size=test_size, random_state=random_state
    )

    print(f"Training transactions (70%): {len(train_transactions)}")
    print(f"Testing transactions (30%): {len(test_transactions)}")

    # 2. One-Hot Encoding for Apriori
    print("One-hot encoding transactions...")
    te = TransactionEncoder()
    te_ary_train = te.fit(train_transactions).transform(train_transactions)
    basket_df_train = pd.DataFrame(te_ary_train, columns=te.columns_)

    # 3. Apply Apriori Algorithm for Frequent Itemsets
    print(f"Running Apriori algorithm (min_support = {min_support})...")
    frequent_itemsets = apriori(basket_df_train, min_support=min_support, use_colnames=True)
    
    if frequent_itemsets.empty:
        print("No frequent itemsets found with min_support =", min_support)
        print("Tip: Lower the min_support value (e.g. min_support=0.005).")
        return

    print(f"Found {len(frequent_itemsets)} frequent itemsets.")

    # 4. Generate Association Rules
    print(f"Generating Association Rules (min_confidence = {min_confidence})...")
    rules = association_rules(
    frequent_itemsets, 
    metric="confidence", 
    min_threshold=min_confidence)

    if not rules.empty:
        # Convert frozensets to standard lists for clean CSV export
        rules_export = rules.copy()
        rules_export['antecedents'] = rules_export['antecedents'].apply(lambda x: list(x))
        rules_export['consequents'] = rules_export['consequents'].apply(lambda x: list(x))
        
        # Sort by Lift descending
        rules_export = rules_export.sort_values(by='lift', ascending=False)

        # 5. Export Association Rules to CSV
        rules_export.to_csv(rules_output_csv, index=False)
        print(f"Association Rules saved to CSV: '{rules_output_csv}'")

        # 6. Save Model artifact object to Pickle (.pkl) File
        model_payload = {
            'encoder': te,
            'frequent_itemsets': frequent_itemsets,
            'rules': rules,
            'parameters': {
                'min_support': min_support,
                'min_confidence': min_confidence,
                'total_transactions_original': total_transactions,
                'train_size': len(train_transactions),
                'test_size': len(test_transactions),
                'random_state': random_state
            }
        }
        
        # Save a version-safe payload: encode pandas objects to Python-native types
        # before pickling so the file remains readable across pandas versions.
        safe_model_payload = {
            'frequent_itemsets': frequent_itemsets,
            'rules': rules_export,
            'parameters': model_payload['parameters'],
            'encoder_columns': list(te.columns_),
        }
        with open(model_pickle_file, 'wb') as f:
            pickle.dump(safe_model_payload, f)

        print(f"Model serialized & saved as pickle file: '{model_pickle_file}'")
        print("=== Top 10 Association Rules by Lift ===")
        print(rules_export[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))

        # MLflow tracking + registry promotion
        avg_lift = float(rules['lift'].mean())
        avg_confidence = float(rules['confidence'].mean())
        avg_support = float(rules['support'].mean())

        with mlflow.start_run(run_name="apriori_training") as run:
            mlflow.log_params({
                "min_support": min_support,
                "min_confidence": min_confidence,
                "test_size": test_size,
                "random_state": random_state,
            })
            mlflow.log_metrics({
                "frequent_itemsets_count": len(frequent_itemsets),
                "association_rules_count": len(rules),
                "avg_lift": avg_lift,
                "avg_confidence": avg_confidence,
                "avg_support": avg_support,
                "train_transactions": len(train_transactions),
                "test_transactions": len(test_transactions),
            })
            mlflow.log_artifact(str(model_pickle_file), artifact_path="model")
            mlflow.log_artifact(str(rules_output_csv), artifact_path="model")

            # Register pyfunc model from predict script and promote if best avg_lift
            try:
                from apriori_mlflow_predict import AprioriRecommender
                mlflow.pyfunc.log_model(
                    artifact_path="sklearn_model",
                    python_model=AprioriRecommender(),
                    artifacts={"apriori_model": str(model_pickle_file)},
                    registered_model_name="AprioriRecommender",
                )
                promote_if_best(
                    model_name="AprioriRecommender",
                    new_run_id=run.info.run_id,
                    metric_name="avg_lift",
                    higher_is_better=True,
                )
            except Exception as e:
                print(f"WARNING: Model registry promotion failed: {e}")

            print(f"MLflow run: {run.info.run_id} | avg_lift={avg_lift:.4f}")
    else:
        print("No association rules met the minimum confidence threshold.")

def log_apriori_artifacts(
    model_path: Path | None = None,
    rules_path: Path | None = None,
    tracking_uri: str | None = None,
) -> None:
    repo_root = resolve_repo_root(Path(__file__))

    if model_path is None:
        model_path = repo_root / "artifacts" / "apriori_model.pkl"
    if rules_path is None:
        rules_path = repo_root / "artifacts" / "apriori_rules.csv"

    model_path = Path(model_path)
    rules_path = Path(rules_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    rules_path.parent.mkdir(parents=True, exist_ok=True)

    if not model_path.exists():
        raise FileNotFoundError(f"Apriori model not found at: {model_path}")

    if tracking_uri is None:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")

    mlflow.set_tracking_uri(tracking_uri)

    try:
        with model_path.open("rb") as fh:
            model_payload = pickle.load(fh)
    except Exception as exc:
        try:
            model_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise TypeError(
            f"Could not load Apriori artifact {model_path}. The pickle appears stale or incompatible with this pandas version. "
            f"The file was removed so it can be rebuilt. Retrain the model and rerun this task: {exc}"
        ) from exc

    params = model_payload.get("parameters", {})
    rules = model_payload.get("rules")
    frequent_itemsets = model_payload.get("frequent_itemsets")

    if rules is None:
        raise ValueError("The Apriori model payload does not contain association rules")

    rules_export = rules.copy()
    rules_export["antecedents"] = rules_export["antecedents"].apply(lambda x: list(x))
    rules_export["consequents"] = rules_export["consequents"].apply(lambda x: list(x))
    rules_export = rules_export.sort_values(by="lift", ascending=False)
    rules_export.to_csv(rules_path, index=False)

    with mlflow.start_run(run_name="apriori_training") as run:
        mlflow.log_param("min_support", params.get("min_support"))
        mlflow.log_param("min_confidence", params.get("min_confidence"))
        mlflow.log_param("total_transactions", params.get("total_transactions"))
        mlflow.log_metric("frequent_itemsets_count", len(frequent_itemsets) if frequent_itemsets is not None else 0)
        mlflow.log_metric("association_rules_count", len(rules))

        mlflow.log_artifact(str(model_path), artifact_path="artifacts")
        mlflow.log_artifact(str(rules_path), artifact_path="artifacts")

        print(f"MLflow run started: {run.info.run_id}")
        print(f"Logged model artifact: {model_path}")
        print(f"Logged rules artifact: {rules_path}")
        print(f"Tracking URI: {tracking_uri}")
        

if __name__ == "__main__":
    # Execute Apriori and save results locally
    input_path = os.path.join(BASE_DIR, "../../..", "DB", "extracted_features.csv")

    parser = argparse.ArgumentParser(description="Upload Apriori artifacts to MLflow")
    parser.add_argument("--model-path", type=str, default=None, help="Path to apriori_model.pkl")
    parser.add_argument("--rules-path", type=str, default=None, help="Path to apriori_rules.csv")
    parser.add_argument("--input-path", type=str, default=None, help="Path to extracted_features.csv")
    parser.add_argument("--tracking-uri", type=str, default=None, help="MLflow tracking server URI")
    args = parser.parse_args()

    model_path = Path(args.model_path) if args.model_path else None
    rules_path = Path(args.rules_path) if args.rules_path else None
    input_path = Path(args.input_path) if args.input_path else None

    run_apriori_and_save(
        features_csv=input_path if input_path else None,
        min_support=0.000000000000000001,
        min_confidence=0.1,
        rules_output_csv=rules_path,
        model_pickle_file=model_path,
        mlflow_tracking_uri=args.tracking_uri,
    )

    log_apriori_artifacts(
        model_path=model_path,
        rules_path=rules_path,
        tracking_uri=args.tracking_uri,
    )
