"""
Transfer learning for sparse data routes in aviation inventory prediction.

Enables predictions for new or data-sparse routes by leveraging patterns
learned from similar routes with rich historical data.
"""

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from typing import Dict, List, Tuple


def find_similar_routes(
    target_route: Dict[str, str],
    all_routes_df: pd.DataFrame,
    similarity_features: List[str] = None,
    top_n: int = 5
) -> pd.DataFrame:
    """
    Find similar routes based on route characteristics.

    Args:
        target_route: Dict with 'origin', 'destination', 'airline', etc.
        all_routes_df: DataFrame with all routes and their characteristics
        similarity_features: Features to use for similarity (default: distance, hub indicators)
        top_n: Number of similar routes to return

    Returns:
        DataFrame with top N most similar routes
    """
    if similarity_features is None:
        similarity_features = [
            "route_distance_km",
            "route_type_code",
            "is_hub_spoke",
            "competition_level_code",
        ]

    # Ensure target route has required features
    target_features = {k: target_route.get(k, 0) for k in similarity_features}

    # Calculate similarity scores (Euclidean distance)
    route_features = all_routes_df[similarity_features].fillna(0)
    target_vector = np.array([target_features[k] for k in similarity_features])

    # Normalize features to 0-1 range for fair comparison
    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    route_features_scaled = scaler.fit_transform(route_features)
    target_vector_scaled = scaler.transform(target_vector.reshape(1, -1))[0]

    # Calculate distances
    distances = np.linalg.norm(route_features_scaled - target_vector_scaled, axis=1)

    # Get indices of top N similar routes (excluding exact match if present)
    similar_indices = np.argsort(distances)[:top_n + 5]  # Get more to filter out exact matches

    # Filter out the exact route if present
    similar_routes = all_routes_df.iloc[similar_indices].copy()
    similar_routes = similar_routes[
        ~((similar_routes["origin"] == target_route.get("origin")) &
          (similar_routes["destination"] == target_route.get("destination")) &
          (similar_routes["airline"] == target_route.get("airline")))
    ].head(top_n)

    similar_routes["similarity_distance"] = distances[similar_indices[:len(similar_routes)]]

    return similar_routes


def cluster_routes_by_characteristics(
    routes_df: pd.DataFrame,
    n_clusters: int = 5,
    features: List[str] = None
) -> Tuple[pd.DataFrame, object]:
    """
    Cluster routes into groups with similar characteristics.

    Args:
        routes_df: DataFrame with route characteristics
        n_clusters: Number of clusters
        features: Features to use for clustering

    Returns:
        Tuple of (DataFrame with cluster assignments, fitted KMeans model)
    """
    if features is None:
        features = [
            "route_distance_km",
            "route_type_code",
            "is_hub_spoke",
            "competition_level_code",
        ]

    # Prepare data for clustering
    route_features = routes_df[features].fillna(0)

    # Normalize features
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    route_features_scaled = scaler.fit_transform(route_features)

    # Fit K-means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    routes_df = routes_df.copy()
    routes_df["route_cluster"] = kmeans.fit_predict(route_features_scaled)

    return routes_df, kmeans


def get_cluster_priors(
    cluster_df: pd.DataFrame,
    target_column: str,
    cluster_id: int
) -> Dict[str, float]:
    """
    Calculate statistical priors for a route cluster.

    Args:
        cluster_df: DataFrame with clustered routes and target values
        target_column: Name of target column
        cluster_id: Cluster ID to get priors for

    Returns:
        Dict with statistical priors (mean, std, median, q10, q90)
    """
    cluster_data = cluster_df[cluster_df["route_cluster"] == cluster_id][target_column]

    if len(cluster_data) == 0:
        return {
            "mean": 0,
            "std": 0,
            "median": 0,
            "q10": 0,
            "q90": 0,
            "count": 0
        }

    return {
        "mean": float(cluster_data.mean()),
        "std": float(cluster_data.std()),
        "median": float(cluster_data.median()),
        "q10": float(cluster_data.quantile(0.1)),
        "q90": float(cluster_data.quantile(0.9)),
        "count": int(len(cluster_data))
    }


def transfer_learning_prediction(
    target_route_data: pd.DataFrame,
    similar_routes_data: pd.DataFrame,
    model,
    feature_columns: List[str],
    blend_weight: float = 0.7
) -> np.ndarray:
    """
    Make prediction for sparse-data route using transfer learning.

    Args:
        target_route_data: Limited data for target route
        similar_routes_data: Rich data from similar routes
        model: Trained model (from similar routes)
        feature_columns: List of feature column names
        blend_weight: Weight for target route data vs transfer (0-1)

    Returns:
        np.ndarray: Blended predictions
    """
    # Prepare features
    target_features = target_route_data[feature_columns].fillna(0)

    # Get base predictions from transfer model
    transfer_predictions = model.predict(target_features)

    # If we have some history for target route, blend with its own pattern
    if len(target_route_data) >= 3:
        # Simple moving average of recent values as target-specific prediction
        target_specific = target_route_data["target"].rolling(3, min_periods=1).mean().values

        # Blend transfer and target-specific predictions
        blended = blend_weight * target_specific + (1 - blend_weight) * transfer_predictions
        return blended
    else:
        # Not enough history, use pure transfer learning
        return transfer_predictions


def augment_sparse_data_with_transfer(
    sparse_route_df: pd.DataFrame,
    rich_routes_df: pd.DataFrame,
    similarity_features: List[str],
    augment_size: int = 20
) -> pd.DataFrame:
    """
    Augment sparse route data with synthetic samples from similar routes.

    Args:
        sparse_route_df: Limited data for target route
        rich_routes_df: Rich data from similar routes
        similarity_features: Features to match on
        augment_size: Number of synthetic samples to add

    Returns:
        DataFrame with augmented data
    """
    # Find most similar route
    if len(rich_routes_df) == 0:
        return sparse_route_df

    # Calculate similarity
    sparse_features = sparse_route_df[similarity_features].mean()
    rich_features = rich_routes_df.groupby(["airline", "origin", "destination"])[similarity_features].mean()

    # Find most similar
    distances = ((rich_features - sparse_features) ** 2).sum(axis=1)
    most_similar_route = distances.idxmin()

    # Get data from most similar route
    similar_data = rich_routes_df[
        (rich_routes_df["airline"] == most_similar_route[0]) &
        (rich_routes_df["origin"] == most_similar_route[1]) &
        (rich_routes_df["destination"] == most_similar_route[2])
    ].copy()

    # Sample and add noise to create synthetic data
    if len(similar_data) >= augment_size:
        synthetic_data = similar_data.sample(n=augment_size, replace=True).copy()

        # Add small random noise to make it synthetic
        numeric_cols = synthetic_data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            noise = np.random.normal(0, synthetic_data[col].std() * 0.1, size=len(synthetic_data))
            synthetic_data[col] += noise

        # Mark as synthetic
        synthetic_data["is_synthetic"] = 1

        # Combine with original sparse data
        sparse_route_df = sparse_route_df.copy()
        sparse_route_df["is_synthetic"] = 0

        augmented = pd.concat([sparse_route_df, synthetic_data], ignore_index=True)
        return augmented

    return sparse_route_df


def calculate_transfer_learning_confidence(
    sparse_route_history_length: int,
    similar_routes_count: int,
    similarity_distance: float
) -> float:
    """
    Calculate confidence score for transfer learning prediction.

    Args:
        sparse_route_history_length: Number of historical data points for target route
        similar_routes_count: Number of similar routes found
        similarity_distance: Average distance to similar routes (normalized 0-1)

    Returns:
        float: Confidence score (0-1)
    """
    # Base confidence from similar routes
    similarity_conf = max(0, 1 - similarity_distance)

    # Boost from having similar routes
    count_conf = min(1.0, similar_routes_count / 5.0)

    # Penalty for very sparse data
    history_conf = min(1.0, sparse_route_history_length / 14.0)

    # Weighted combination
    confidence = (
        0.4 * similarity_conf +
        0.3 * count_conf +
        0.3 * history_conf
    )

    return float(np.clip(confidence, 0, 1))
