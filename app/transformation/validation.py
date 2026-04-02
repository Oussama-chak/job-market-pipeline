import pandas as pd


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def summarize_nulls(df: pd.DataFrame) -> pd.Series:
    return df.isnull().sum()


def summarize_shape(df: pd.DataFrame) -> tuple[int, int]:
    return df.shape