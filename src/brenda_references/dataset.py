"""Functions for loading the dataset splits."""

import pathlib

import pandas as pd

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"


def load_split(split: str) -> pd.DataFrame:
    """Load dataset split."""
    path = DATA_DIR / f"{split}_data.csv"
    return pd.read_csv(path)


def validation_data() -> pd.DataFrame:
    """Load validation data."""
    return load_split("validation")


def training_data() -> pd.DataFrame:
    """Load training data."""
    return load_split("training")


def test_data() -> pd.DataFrame:
    """Load test data."""
    return load_split("test")
