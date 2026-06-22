import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)


def data_quality_check(data):
    try:
        df = pd.json_normalize(data)

        if df.empty:
            logging.error("DataFrame is empty")
            return False

        errors = []

        # ----------------------------
        # Required columns
        # ----------------------------
        required_columns = [
            "id",
            "symbol",
            "name",
            "current_price",
            "market_cap",
            "market_cap_rank",
            "fully_diluted_valuation",
            "total_volume",
            "high_24h",
            "low_24h",
            "price_change_24h",
            "price_change_percentage_24h",
            "market_cap_change_24h",
            "market_cap_change_percentage_24h",
            "circulating_supply",
            "total_supply",
            "max_supply",
            "ath",
            "ath_change_percentage",
            "ath_date",
            "atl",
            "atl_change_percentage",
            "atl_date",
            "roi",
            "roi_times",
            "roi_currency",
            "roi_percentage"
        ]

        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")

        # ----------------------------
        # Not-null checks
        # ----------------------------
        not_null_columns = ["id", "symbol", "name"]

        for col in not_null_columns:
            if col in df.columns and df[col].isnull().any():
                errors.append(f"Null values found in {col}")

        # ----------------------------
        # Uniqueness checks
        # ----------------------------
        if "id" in df.columns and df["id"].duplicated().any():
            errors.append("Duplicate values found in id")

        if "symbol" in df.columns and df["symbol"].duplicated().any():
            errors.append("Duplicate values found in symbol")

        # ----------------------------
        # Numeric validations
        # ----------------------------
        numeric_checks = {
            "current_price": (0, None),
            "market_cap_rank": (1, None),
            "circulating_supply": (0, None)
        }

        for col, (min_val, max_val) in numeric_checks.items():
            if col in df.columns:
                if min_val is not None:
                    if (df[col] < min_val).any():
                        errors.append(f"{col} has values below {min_val}")

                if max_val is not None:
                    if (df[col] > max_val).any():
                        errors.append(f"{col} has values above {max_val}")

        # ----------------------------
        # Final result
        # ----------------------------
        if errors:
            logging.error("DATA QUALITY FAILED ❌")
            for err in errors:
                logging.error(err)
            return False

        logging.info("DATA QUALITY PASSED ✅")
        return True

    except Exception as e:
        logging.exception(f"Unexpected error in quality check: {e}")
        return False