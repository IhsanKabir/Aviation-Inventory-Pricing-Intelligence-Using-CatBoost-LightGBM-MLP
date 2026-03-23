"""
Automated hyperparameter tuning for ML models using Optuna.

This tool finds optimal model parameters per route/target combination
to maximize prediction accuracy. Results are saved to model_config_overrides.json.
"""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL


def parse_args():
    parser = argparse.ArgumentParser(description="Tune hyperparameters for ML models")
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--model", choices=["catboost", "lightgbm"], default="catboost")
    parser.add_argument("--target", required=True, help="Target column to optimize for")
    parser.add_argument("--airline", help="Filter to specific airline")
    parser.add_argument("--route", help="Route in format ORIGIN-DESTINATION (e.g., DAC-DXB)")
    parser.add_argument("--n-trials", type=int, default=50, help="Number of Optuna trials")
    parser.add_argument("--train-days", type=int, default=28, help="Training window in days")
    parser.add_argument("--val-days", type=int, default=7, help="Validation window in days")
    parser.add_argument("--output", default="config/model_config_overrides.json", help="Output JSON file")
    parser.add_argument("--random-seed", type=int, default=42)
    return parser.parse_args()


def load_training_data(engine, target: str, airline: str = None, route: str = None, train_days: int = 28):
    """Load training data from database."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=train_days)

    where_clauses = [f"event_day >= '{start_date}'", f"event_day <= '{end_date}'"]

    if airline:
        where_clauses.append(f"airline = '{airline}'")

    if route:
        origin, destination = route.split("-")
        where_clauses.append(f"origin = '{origin}'")
        where_clauses.append(f"destination = '{destination}'")

    where_clause = " AND ".join(where_clauses)

    query = f"""
    SELECT
        event_day,
        airline,
        origin,
        destination,
        cabin,
        {target} as target,
        departure_day
    FROM daily_change_events
    WHERE {where_clause}
    ORDER BY event_day
    """

    df = pd.read_sql(query, engine)
    return df


def prepare_features(df: pd.DataFrame):
    """Prepare basic features for training."""
    df = df.copy()

    # Lag features
    for lag in [1, 2, 3, 7]:
        df[f"lag_{lag}"] = df.groupby(["airline", "origin", "destination", "cabin"])["target"].shift(lag)

    # Rolling mean features
    for window in [3, 7]:
        df[f"rolling_mean_{window}"] = (
            df.groupby(["airline", "origin", "destination", "cabin"])["target"]
            .rolling(window=window, min_periods=1)
            .mean()
            .reset_index(level=[0, 1, 2, 3], drop=True)
        )

    # Day of week
    df["dow"] = pd.to_datetime(df["event_day"]).dt.dayofweek

    # Days to departure
    df["days_to_departure"] = (
        pd.to_datetime(df["departure_day"]) - pd.to_datetime(df["event_day"])
    ).dt.days

    # Fill NaN with 0
    feature_cols = [c for c in df.columns if c.startswith(("lag_", "rolling_", "dow", "days_"))]
    df[feature_cols] = df[feature_cols].fillna(0)

    return df, feature_cols


def objective_catboost(trial, X_train, y_train, X_val, y_val):
    """Optuna objective function for CatBoost."""
    try:
        from catboost import CatBoostRegressor
    except ImportError:
        raise ImportError("CatBoost is required. Install with: pip install catboost")

    params = {
        "iterations": trial.suggest_int("iterations", 100, 500),
        "depth": trial.suggest_int("depth", 4, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1, 10),
        "random_seed": 42,
        "verbose": False,
    }

    model = CatBoostRegressor(**params)
    model.fit(X_train, y_train, verbose=False)

    pred = model.predict(X_val)
    mae = np.mean(np.abs(pred - y_val))

    return mae


def objective_lightgbm(trial, X_train, y_train, X_val, y_val):
    """Optuna objective function for LightGBM."""
    try:
        from lightgbm import LGBMRegressor
    except ImportError:
        raise ImportError("LightGBM is required. Install with: pip install lightgbm")

    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 500),
        "num_leaves": trial.suggest_int("num_leaves", 20, 50),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 30),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 0.0, 1.0),
        "reg_lambda": trial.suggest_float("reg_lambda", 0.0, 1.0),
        "random_state": 42,
        "verbose": -1,
    }

    model = LGBMRegressor(**params)
    model.fit(X_train, y_train)

    pred = model.predict(X_val)
    mae = np.mean(np.abs(pred - y_val))

    return mae


def tune_hyperparameters(args):
    """Run hyperparameter tuning with Optuna."""
    try:
        import optuna
    except ImportError:
        raise ImportError("Optuna is required. Install with: pip install optuna")

    # Load data
    print(f"Loading training data for target: {args.target}")
    engine = create_engine(args.database_url)
    df = load_training_data(
        engine,
        args.target,
        airline=args.airline,
        route=args.route,
        train_days=args.train_days
    )

    if len(df) < 50:
        print(f"Insufficient data: {len(df)} rows. Need at least 50 rows.")
        return None

    print(f"Loaded {len(df)} rows")

    # Prepare features
    df, feature_cols = prepare_features(df)

    # Split train/val
    split_idx = len(df) - args.val_days
    train_df = df.iloc[:split_idx]
    val_df = df.iloc[split_idx:]

    X_train = train_df[feature_cols]
    y_train = train_df["target"]
    X_val = val_df[feature_cols]
    y_val = val_df["target"]

    print(f"Train size: {len(X_train)}, Val size: {len(X_val)}")

    # Create Optuna study
    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=args.random_seed))

    # Select objective function
    if args.model == "catboost":
        objective = lambda trial: objective_catboost(trial, X_train, y_train, X_val, y_val)
    else:
        objective = lambda trial: objective_lightgbm(trial, X_train, y_train, X_val, y_val)

    # Run optimization
    print(f"Starting hyperparameter tuning with {args.n_trials} trials...")
    study.optimize(objective, n_trials=args.n_trials, show_progress_bar=True)

    # Get best parameters
    best_params = study.best_params
    best_value = study.best_value

    print(f"\nBest MAE: {best_value:.4f}")
    print(f"Best parameters: {json.dumps(best_params, indent=2)}")

    return {
        "model": args.model,
        "target": args.target,
        "best_params": best_params,
        "best_mae": float(best_value),
        "n_trials": args.n_trials,
        "tuned_at": datetime.now().isoformat(),
        "filter": {
            "airline": args.airline,
            "route": args.route,
        }
    }


def save_config_override(result: dict, output_path: str):
    """Save tuned parameters to config file."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Load existing config if it exists
    if output_file.exists():
        with open(output_file) as f:
            config = json.load(f)
    else:
        config = {"overrides": []}

    # Add new result
    config["overrides"].append(result)

    # Save updated config
    with open(output_file, "w") as f:
        json.dump(config, f, indent=2)

    print(f"\nSaved configuration to: {output_file}")


def main():
    args = parse_args()

    try:
        result = tune_hyperparameters(args)

        if result:
            save_config_override(result, args.output)
            return 0
        else:
            return 1

    except Exception as e:
        print(f"Error during tuning: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
