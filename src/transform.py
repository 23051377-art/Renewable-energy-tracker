import pandas as pd
import numpy as np

NUMERIC_COLUMNS = [
    "year", "month", "solar_energy", "wind_energy", "hydro_energy",
    "geothermal_energy", "biomass_energy", "total_energy", "capacity",
    "efficiency", "capacity_factor", "co2_reduction", "carbon_offset",
    "temperature", "wind_speed", "rainfall", "sunlight_hours",
    "cost", "revenue"
]

TEXT_COLUMNS = [
    "state", "region", "plant_id", "plant_name", "plant_type", "data_source"
]

def clean_columns(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def convert_types(df):
    df = df.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def fill_missing_values(df):
    df = df.copy()

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        median_value = df[col].median()
        if pd.isna(median_value):
            median_value = 0
        df[col] = df[col].fillna(median_value)

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", np.nan).fillna("Unknown")

    return df

def add_features(df):
    df = df.copy()

    source_cols = [
        c for c in ["solar_energy", "wind_energy", "hydro_energy", "geothermal_energy", "biomass_energy"]
        if c in df.columns
    ]

    if source_cols:
        df["source_energy_total"] = df[source_cols].sum(axis=1)

    if "total_energy" in df.columns and "capacity" in df.columns:
        df["energy_gap"] = df["total_energy"] - df.get("source_energy_total", df["total_energy"])
        df["capacity_utilization_pct"] = np.where(
            df["capacity"] != 0,
            (df["total_energy"] / df["capacity"]) * 100,
            0
        )

    if "cost" in df.columns and "total_energy" in df.columns:
        df["cost_per_unit_energy"] = np.where(
            df["total_energy"] != 0,
            df["cost"] / df["total_energy"],
            0
        )

    if "revenue" in df.columns and "total_energy" in df.columns:
        df["revenue_per_unit_energy"] = np.where(
            df["total_energy"] != 0,
            df["revenue"] / df["total_energy"],
            0
        )

    if "revenue" in df.columns and "cost" in df.columns:
        df["profit"] = df["revenue"] - df["cost"]
        df["profit_margin_pct"] = np.where(
            df["revenue"] != 0,
            (df["profit"] / df["revenue"]) * 100,
            0
        )

    if "co2_reduction" in df.columns and "total_energy" in df.columns:
        df["co2_reduction_per_unit"] = np.where(
            df["total_energy"] != 0,
            df["co2_reduction"] / df["total_energy"],
            0
        )

    if "efficiency" in df.columns and "capacity_factor" in df.columns:
        df["performance_score"] = (df["efficiency"] * df["capacity_factor"]) / 100

    if "timestamp" in df.columns and "total_energy" in df.columns:
        group_col = None
        if "plant_id" in df.columns:
            group_col = "plant_id"
        elif "state" in df.columns:
            group_col = "state"

        if group_col is not None:
            df = df.sort_values([group_col, "timestamp"])
            df["energy_growth"] = df.groupby(group_col)["total_energy"].diff()
            df["energy_growth_pct"] = df.groupby(group_col)["total_energy"].pct_change() * 100

    return df

def transform_data(df):
    df = clean_columns(df)
    df = convert_types(df)
    df = fill_missing_values(df)
    df = df.drop_duplicates()
    df = add_features(df)
    return df
