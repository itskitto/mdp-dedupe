import pandas as pd
import re
from data_extraction import extract_all_data


def normalize_text(value: str) -> str:
    """Convert text to lowercase and strip whitespace."""
    return value.strip().lower() if isinstance(value, str) else value


def normalize_phone(value: str) -> str:
    """Normalize phone numbers to a consistent format (digits only)."""
    return re.sub(r"\D", "", value) if isinstance(value, str) else value


def preprocess_clinic_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess clinic data."""
    df["first_name"] = df["first_name"].apply(normalize_text)
    df["last_name"] = df["last_name"].apply(normalize_text)
    df["phone_number"] = df["phone_number"].apply(normalize_phone)
    df["email"] = df["email"].apply(normalize_text)
    df["address"] = df["address"].apply(normalize_text)
    return df


def preprocess_urgent_care_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess urgent care data."""
    df["first_name"] = df["first_name"].apply(normalize_text)
    df["last_name"] = df["last_name"].apply(normalize_text)
    df["dob"] = pd.to_datetime(df["dob"]).dt.strftime("%Y-%m-%d")
    df["phone"] = df["phone"].apply(normalize_phone)
    df["email"] = df["email"].apply(normalize_text)
    df["address_line"] = df["address_line"].apply(normalize_text)
    return df


def preprocess_hospital_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess hospital data."""
    df["first_name"] = df["first_name"].apply(normalize_text)
    df["middle_name"] = df["middle_name"].apply(normalize_text)
    df["last_name"] = df["last_name"].apply(normalize_text)
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"]).dt.strftime("%Y-%m-%d")
    df["phone_number"] = df["phone_number"].apply(normalize_phone)
    df["email_address"] = df["email_address"].apply(normalize_text)
    df["address"] = df["address"].apply(lambda x: {k: normalize_text(v) for k, v in x.items()})
    return df


def preprocess_physical_therapy_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess physical therapy data."""
    df["full_name"] = df["full_name"].apply(normalize_text)
    df["first_name"] = df["full_name"].str.extract(r"^(\S+)")
    df["last_name"] = df["full_name"].str.extract(r"\s+(.+)$")
    df["dob"] = pd.to_datetime(df["dob"]).dt.strftime("%Y-%m-%d")
    df["contact_phone"] = df["contact_phone"].apply(normalize_phone)
    df["email"] = df["email"].apply(normalize_text)
    df["street_address"] = df["street_address"].apply(normalize_text)
    df["city"] = df["city"].apply(normalize_text)
    df["state"] = df["state"].apply(normalize_text)
    return df


def preprocess_all_data(data: dict) -> dict:
    """Preprocess all data sources."""
    preprocessed_data = {}
    preprocessed_data["clinic"] = preprocess_clinic_data(data["clinic"])
    preprocessed_data["urgent_care"] = preprocess_urgent_care_data(data["urgent_care"])
    preprocessed_data["hospital"] = preprocess_hospital_data(data["hospital"])
    preprocessed_data["physical_therapy"] = preprocess_physical_therapy_data(data["physical_therapy"])
    return preprocessed_data


if __name__ == "__main__":
    # Extract raw data
    raw_data = extract_all_data()

    # Preprocess data
    clean_data = preprocess_all_data(raw_data)

    # Print summaries of cleaned data
    for source, df in clean_data.items():
        print(f"Cleaned data from {source}:")
        print(df.info())
        print(df.head())
