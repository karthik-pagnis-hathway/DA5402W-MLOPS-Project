import argparse
import json
import os
import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any

import mlflow
import mlflow.pyfunc
import pandas as pd


class AprioriRecommender(mlflow.pyfunc.PythonModel):
    def load_context(self, context: Any) -> None:
        model_path = context.artifacts["apriori_model"]
        with open(model_path, "rb") as fh:
            payload = pickle.load(fh)

        self.payload = payload
        self.rules = payload.get("rules").copy()
        self.rules["antecedents"] = self.rules["antecedents"].apply(lambda x: set(x))
        self.rules["consequents"] = self.rules["consequents"].apply(lambda x: set(x))
        self.rules = self.rules.sort_values(["lift", "confidence"], ascending=False).reset_index(drop=True)

    def _normalize_items(self, items: Any) -> list[str]:
        if items is None:
            return []
        if isinstance(items, str):
            try:
                parsed = json.loads(items)
            except json.JSONDecodeError:
                parsed = [items]
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
            return [str(parsed).strip()]
        if isinstance(items, (list, tuple, set)):
            return [str(x).strip() for x in items if str(x).strip()]
        if isinstance(items, pd.Series):
            return [str(x).strip() for x in items.tolist() if str(x).strip()]
        return [str(items).strip()]

    def _prepare_baskets(self, model_input: Any) -> list[list[str]]:
        if isinstance(model_input, pd.DataFrame):
            if model_input.empty:
                return []
            if len(model_input.columns) == 1:
                return [self._normalize_items(row[0]) for _, row in model_input.iterrows()]
            return [self._normalize_items(row.to_dict()) for _, row in model_input.iterrows()]

        if isinstance(model_input, (list, tuple)):
            if not model_input:
                return []
            first = model_input[0]
            if isinstance(first, (list, tuple, set, str, pd.Series)):
                return [self._normalize_items(item) for item in model_input]
            return [self._normalize_items(model_input)]

        if isinstance(model_input, dict):
            items = model_input.get("items", model_input.get("input", []))
            return [self._normalize_items(items)]

        return [self._normalize_items(model_input)]

    def predict(self, context: Any, model_input: Any, params: Any | None = None) -> pd.DataFrame:
        baskets = self._prepare_baskets(model_input)
        predictions: list[list[str]] = []

        for basket in baskets:
            basket_set = set(basket)
            ranked_rules = self.rules[
                self.rules["antecedents"].apply(lambda antecedents: antecedents.issubset(basket_set))
            ]

            candidate_items: list[tuple[str, float]] = []
            for _, row in ranked_rules.iterrows():
                for item in row["consequents"]:
                    if item in basket_set:
                        continue
                    candidate_items.append((str(item), float(row.get("lift", 0.0))))

            seen: set[str] = set()
            ordered_items: list[str] = []
            for item, _ in sorted(candidate_items, key=lambda x: x[1], reverse=True):
                if item not in seen:
                    seen.add(item)
                    ordered_items.append(item)

            predictions.append(ordered_items[:5])

        return pd.DataFrame({"predicted_items": predictions})


def resolve_repo_root(start_file: Path) -> Path:
    current = start_file.resolve()
    for parent in [current, *current.parents]:
        if (parent / "artifacts").exists():
            return parent
    return current.parents[-1]


def log_apriori_pyfunc_model(model_path: Path, tracking_uri: str, run_name: str = "apriori_recommender") -> str:
    mlflow.set_tracking_uri(tracking_uri)

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=AprioriRecommender(),
            artifacts={"apriori_model": str(model_path)},
            pip_requirements=["mlflow", "pandas"],
        )
        print(f"MLflow run created: {run.info.run_id}")
        print(f"Model URI: runs:/{run.info.run_id}/model")
        return f"runs:/{run.info.run_id}/model"


def serve_model(model_uri: str, host: str = "127.0.0.1", port: int = 5001) -> None:
    venv_scripts_dir = str(Path(sys.executable).parent)
    env = os.environ.copy()
    env["PATH"] = os.pathsep.join([venv_scripts_dir, env.get("PATH", "")])
    env["VIRTUAL_ENV"] = str(Path(sys.executable).resolve().parent)

    cmd = [
        sys.executable,
        "-m",
        "mlflow",
        "models",
        "serve",
        "-m",
        model_uri,
        "--host",
        host,
        "--port",
        str(port),
        "--no-conda",
    ]
    print("Starting MLflow model server with:")
    print(" ".join(cmd))
    subprocess.Popen(cmd, env=env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log an Apriori MLflow prediction model. Use --serve only for local testing, not in the DAG.")
    parser.add_argument("--model-path", type=str, default=None, help="Path to apriori_model.pkl")
    parser.add_argument("--tracking-uri", type=str, default="http://mlflow:5000", help="MLflow tracking server URI")
    parser.add_argument("--serve", action="store_true", help="Serve the model after logging it (local testing only)")
    parser.add_argument("--port", type=int, default=5001, help="Port for the MLflow model server")
    args = parser.parse_args()

    repo_root = resolve_repo_root(Path(__file__))
    model_path = Path(args.model_path) if args.model_path else repo_root / "artifacts" / "apriori_model.pkl"

    if not model_path.exists():
        raise FileNotFoundError(f"Apriori model not found at: {model_path}")

    model_uri = log_apriori_pyfunc_model(model_path=model_path, tracking_uri=args.tracking_uri)

    if args.serve:
        serve_model(model_uri=model_uri, port=args.port)
