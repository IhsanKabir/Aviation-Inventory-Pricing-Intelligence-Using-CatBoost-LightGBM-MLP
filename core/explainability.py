"""
Feature importance and explainability utilities using SHAP.

Provides SHAP-based feature importance extraction for CatBoost, LightGBM,
and sklearn models to explain which features drive each prediction.
"""

import numpy as np
import pandas as pd


def compute_shap_feature_importance(model, X: pd.DataFrame, model_type: str = "tree"):
    """
    Compute SHAP feature importance for a trained model.

    Args:
        model: Trained model (CatBoost, LightGBM, or sklearn)
        X: Feature dataframe used for prediction
        model_type: Type of model ("tree" for CatBoost/LightGBM, "linear" for sklearn)

    Returns:
        dict: Feature importance dictionary with feature names and SHAP values
    """
    try:
        import shap
    except ImportError:
        # SHAP not available, return empty result
        return {"available": False, "features": [], "importance": []}

    try:
        # Create appropriate explainer based on model type
        if model_type == "tree":
            explainer = shap.TreeExplainer(model)
        else:
            # For non-tree models (MLP, etc.), use KernelExplainer with sampled background
            # Use small sample for efficiency
            background = shap.sample(X, min(100, len(X)))
            explainer = shap.KernelExplainer(model.predict, background)

        # Compute SHAP values
        shap_values = explainer.shap_values(X)

        # Handle multi-output models (take mean across outputs)
        if isinstance(shap_values, list):
            shap_values = np.mean(shap_values, axis=0)

        # Ensure 2D array
        if len(shap_values.shape) == 1:
            shap_values = shap_values.reshape(1, -1)

        # Compute mean absolute SHAP values per feature
        mean_abs_shap = np.abs(shap_values).mean(axis=0)

        # Create feature importance dictionary
        feature_names = list(X.columns)
        importance_dict = {
            "available": True,
            "features": feature_names,
            "importance": mean_abs_shap.tolist(),
            "shap_values": shap_values.tolist() if shap_values.shape[0] <= 10 else None,  # Only store if small
        }

        return importance_dict

    except Exception as e:
        # If SHAP computation fails, return empty result
        return {
            "available": False,
            "error": str(e),
            "features": [],
            "importance": []
        }


def get_top_features(importance_dict: dict, top_n: int = 5):
    """
    Extract top N most important features from SHAP importance dictionary.

    Args:
        importance_dict: Dictionary returned by compute_shap_feature_importance
        top_n: Number of top features to return

    Returns:
        list: List of (feature_name, importance_score) tuples, sorted by importance
    """
    if not importance_dict.get("available", False):
        return []

    features = importance_dict["features"]
    importance = importance_dict["importance"]

    # Create list of (feature, importance) tuples
    feature_importance = list(zip(features, importance))

    # Sort by importance (descending) and take top N
    feature_importance.sort(key=lambda x: x[1], reverse=True)

    return feature_importance[:top_n]


def format_feature_importance_for_output(importance_dict: dict, top_n: int = 5):
    """
    Format feature importance for inclusion in prediction output CSV.

    Args:
        importance_dict: Dictionary returned by compute_shap_feature_importance
        top_n: Number of top features to include

    Returns:
        dict: Dictionary with keys like shap_feature_1, shap_value_1, etc.
    """
    top_features = get_top_features(importance_dict, top_n)

    output = {}
    for i, (feature, value) in enumerate(top_features, start=1):
        output[f"shap_feature_{i}"] = feature
        output[f"shap_value_{i}"] = round(float(value), 6)

    # Fill remaining slots with None if fewer than top_n features
    for i in range(len(top_features) + 1, top_n + 1):
        output[f"shap_feature_{i}"] = None
        output[f"shap_value_{i}"] = None

    return output


def explain_prediction_change(
    current_features: dict,
    previous_features: dict,
    importance_dict: dict,
    threshold_pct: float = 5.0
):
    """
    Explain why a prediction changed compared to previous prediction.

    Args:
        current_features: Current feature values
        previous_features: Previous feature values
        importance_dict: SHAP importance dictionary
        threshold_pct: Minimum percentage change to report

    Returns:
        list: List of explanations for features that changed significantly
    """
    if not importance_dict.get("available", False):
        return []

    explanations = []
    features = importance_dict["features"]
    importance = importance_dict["importance"]

    # Create importance lookup
    importance_map = dict(zip(features, importance))

    # Find features with significant changes
    for feature in features:
        if feature not in current_features or feature not in previous_features:
            continue

        curr_val = current_features.get(feature, 0)
        prev_val = previous_features.get(feature, 0)

        # Skip if no meaningful change
        if prev_val == 0 and curr_val == 0:
            continue

        # Calculate percentage change
        if prev_val != 0:
            pct_change = 100 * (curr_val - prev_val) / abs(prev_val)
        else:
            pct_change = 100.0 if curr_val > 0 else -100.0

        if abs(pct_change) >= threshold_pct:
            explanations.append({
                "feature": feature,
                "previous": prev_val,
                "current": curr_val,
                "pct_change": round(pct_change, 1),
                "importance": importance_map.get(feature, 0)
            })

    # Sort by importance
    explanations.sort(key=lambda x: x["importance"], reverse=True)

    return explanations[:5]  # Return top 5 changes
