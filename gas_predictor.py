"""Data processing pipeline and OLS regression for Brent Crude vs US Gas Price."""

import logging

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_absolute_error

from fred_client import fetch_series

logger = logging.getLogger(__name__)

# Train/test split date
SPLIT_DATE = "2019-04-01"

# Index base dates
BRENT_BASE_DATE = "1992-01-01"
GAS_BASE_DATE = "1993-01-01"  # earliest reliable monthly observation


def _parse_series(raw_observations):
    """Parse FRED observations into a DataFrame, filtering missing values."""
    rows = []
    for obs in raw_observations:
        val = obs.get("value", ".")
        if val == ".":
            continue
        try:
            rows.append({"date": obs["date"], "value": float(val)})
        except (ValueError, KeyError):
            continue
    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_gas_predictor_data():
    """Fetch FRED data, compute indexes, run regression, return everything for the template."""
    # Fetch raw data
    brent_raw = fetch_series("POILBREUSDM", start="1990-01-01")
    gas_raw = fetch_series("GASREGW", start="1990-01-01", frequency="m", aggregation="avg")

    brent_df = _parse_series(brent_raw)
    gas_df = _parse_series(gas_raw)

    if brent_df.empty or gas_df.empty:
        return _empty_result("No data available from FRED API")

    # ---- Chart data: indexed series ----
    # Brent index: base = Jan 1992
    brent_base_row = brent_df[brent_df["date"] == BRENT_BASE_DATE]
    if brent_base_row.empty:
        brent_base = brent_df["value"].iloc[0]
    else:
        brent_base = brent_base_row["value"].iloc[0]
    brent_df["index"] = (brent_df["value"] / brent_base) * 100

    # Gas index: base = earliest available
    gas_base_row = gas_df[gas_df["date"] == GAS_BASE_DATE]
    if gas_base_row.empty:
        gas_base = gas_df["value"].iloc[0]
    else:
        gas_base = gas_base_row["value"].iloc[0]
    gas_df["index"] = (gas_df["value"] / gas_base) * 100

    # Build chart data
    chart_data = {
        "brent_dates": brent_df["date"].dt.strftime("%Y-%m-%d").tolist(),
        "brent_index": brent_df["index"].round(2).tolist(),
        "brent_price": brent_df["value"].round(2).tolist(),
        "gas_dates": gas_df["date"].dt.strftime("%Y-%m-%d").tolist(),
        "gas_index": gas_df["index"].round(2).tolist(),
        "gas_price": gas_df["value"].round(3).tolist(),
    }

    # ---- Regression: merge on date ----
    merged = pd.merge(
        brent_df[["date", "value"]].rename(columns={"value": "brent"}),
        gas_df[["date", "value"]].rename(columns={"value": "gas"}),
        on="date",
        how="inner",
    ).sort_values("date").reset_index(drop=True)

    if len(merged) < 10:
        return {
            "chart_data": chart_data,
            "error": "Insufficient overlapping data for regression",
            **_empty_model(),
        }

    # Month-over-month changes
    merged["delta_brent"] = merged["brent"].diff()
    merged["delta_gas"] = merged["gas"].diff()
    merged["lag_delta_gas"] = merged["delta_gas"].shift(1)
    merged = merged.dropna().reset_index(drop=True)

    # Train/test split
    split = pd.Timestamp(SPLIT_DATE)
    train = merged[merged["date"] < split]
    test = merged[merged["date"] >= split]

    if len(train) < 5 or len(test) < 2:
        return {
            "chart_data": chart_data,
            "error": "Insufficient data for train/test split",
            **_empty_model(),
        }

    # Fit OLS
    X_train = train[["delta_brent", "lag_delta_gas"]].values
    y_train = train["delta_gas"].values
    X_test = test[["delta_brent", "lag_delta_gas"]].values
    y_test = test["delta_gas"].values

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    residuals = y_train - y_pred_train
    std_resid = float(np.std(residuals))

    coefficients = {
        "intercept": round(float(model.intercept_), 6),
        "brent": round(float(model.coef_[0]), 6),
        "lag_gas": round(float(model.coef_[1]), 6),
    }

    metrics = {
        "train": {
            "r2": round(float(r2_score(y_train, y_pred_train)), 4),
            "mae": round(float(mean_absolute_error(y_train, y_pred_train)), 4),
            "n": len(train),
            "period": f"{train['date'].iloc[0].strftime('%b %Y')} - {train['date'].iloc[-1].strftime('%b %Y')}",
        },
        "test": {
            "r2": round(float(r2_score(y_test, y_pred_test)), 4),
            "mae": round(float(mean_absolute_error(y_test, y_pred_test)), 4),
            "n": len(test),
            "period": f"{test['date'].iloc[0].strftime('%b %Y')} - {test['date'].iloc[-1].strftime('%b %Y')}",
        },
        "pred_interval_95": round(1.96 * std_resid, 4),
    }

    # Latest values for the calculator default
    latest = merged.iloc[-1]

    return {
        "chart_data": chart_data,
        "coefficients": coefficients,
        "metrics": metrics,
        "latest_brent": round(float(latest["brent"]), 2),
        "latest_gas": round(float(latest["gas"]), 3),
        "latest_delta_gas": round(float(latest["delta_gas"]), 4),
        "error": None,
    }


def _empty_result(error_msg):
    return {
        "chart_data": {
            "brent_dates": [], "brent_index": [], "brent_price": [],
            "gas_dates": [], "gas_index": [], "gas_price": [],
        },
        "error": error_msg,
        **_empty_model(),
    }


def _empty_model():
    return {
        "coefficients": {"intercept": 0, "brent": 0, "lag_gas": 0},
        "metrics": {
            "train": {"r2": 0, "mae": 0, "n": 0, "period": "N/A"},
            "test": {"r2": 0, "mae": 0, "n": 0, "period": "N/A"},
            "pred_interval_95": 0,
        },
        "latest_brent": 0,
        "latest_gas": 0,
        "latest_delta_gas": 0,
    }
