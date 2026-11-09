"""Pipeline reproduzível para o Tech Challenge Amazon (Power BI).

Uso:
    python fase4_amazon/src/dataprep.py --input amazon.csv \
        --output-dir fase4_amazon/data/processed

Mantém preços em rúpias indianas (INR), sem taxa de câmbio dinâmica.
"""

from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {
    "product_id", "product_name", "category", "discounted_price", "actual_price",
    "discount_percentage", "rating", "rating_count", "review_id",
    "review_title", "review_content",
}
OPTIONAL_COLUMNS = ("about_product", "img_link", "product_link")


def to_numeric(series: pd.Series, strip_pattern: str = r"[,\s]") -> pd.Series:
    """Converte valores com agrupamentos indianos, símbolo monetário e porcentagens."""
    cleaned = series.astype("string").str.replace(strip_pattern, "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def clean_review_text(value: object) -> str:
    """Limpa texto preservando vírgulas e sem tentar separar reviews compostos."""
    if pd.isna(value):
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"https?://[^\s,]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def prepare(input_path: Path, output_dir: Path) -> dict:
    """Gera products.csv (1 por produto), review_bundles.csv e quality_report.json."""
    input_path, output_dir = Path(input_path), Path(output_dir)
    raw = pd.read_csv(input_path, dtype="string", keep_default_na=False, encoding="utf-8-sig")
    missing = REQUIRED_COLUMNS - set(raw.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(sorted(missing))}")
    if raw.empty:
        raise ValueError("O arquivo CSV está vazio.")
    for col in OPTIONAL_COLUMNS:
        if col not in raw.columns:
            raw[col] = ""

    df = raw.copy()
    df["source_row"] = range(2, len(df) + 2)  # número da linha no CSV, com cabeçalho
    for col in REQUIRED_COLUMNS | set(OPTIONAL_COLUMNS):
        df[col] = df[col].astype("string").str.strip()
        df[col] = df[col].replace("", pd.NA)

    df["actual_price_inr"] = to_numeric(df["actual_price"], r"[₹,\s]")
    df["discounted_price_inr"] = to_numeric(df["discounted_price"], r"[₹,\s]")
    df["discount_pct"] = to_numeric(df["discount_percentage"], r"[%\s]") / 100
    df["rating_value"] = to_numeric(df["rating"], r"\s")
    df["rating_count_value"] = to_numeric(df["rating_count"], r"[,\s]")

    # Invalid values become missing, not zero (avoids distorting averages).
    for col in ("actual_price_inr", "discounted_price_inr"):
        df.loc[df[col].le(0), col] = float("nan")
    df.loc[~df["discount_pct"].between(0, 1), "discount_pct"] = float("nan")
    df.loc[~df["rating_value"].between(0, 5), "rating_value"] = float("nan")
    df.loc[df["rating_count_value"].lt(0), "rating_count_value"] = float("nan")

    levels = df["category"].fillna("").str.split("|", regex=False)
    for i in range(1, 5):
        df[f"category_level_{i}"] = levels.str.get(i - 1).replace("", pd.NA)
    df["category_leaf"] = levels.apply(lambda xs: xs[-1] if xs else "")

    df["discount_pct_calculated"] = (
        1 - df["discounted_price_inr"] / df["actual_price_inr"]
    ).where(df["actual_price_inr"].gt(0))
    df["discount_inconsistent"] = (
        (df["discount_pct"] - df["discount_pct_calculated"]).abs().gt(0.02)
    ).fillna(False)
    df["price_inverted"] = df["discounted_price_inr"].gt(df["actual_price_inr"]).fillna(False)

    # Same ID can occur more than once. Never sum rating_count across source rows.
    # Select the most informative observation (largest non-null rating_count),
    # then the most complete observation, then the first in the source CSV.
    completeness_fields = [
        "actual_price_inr", "discounted_price_inr", "discount_pct",
        "rating_value", "rating_count_value", "product_name", "category",
    ]
    df["completeness"] = df[completeness_fields].notna().sum(axis=1)
    selected = (
        df.sort_values(
            ["product_id", "rating_count_value", "completeness", "source_row"],
            ascending=[True, False, False, True], na_position="last", kind="stable",
        )
        .drop_duplicates("product_id", keep="first")
        .sort_values("product_id", kind="stable")
        .copy()
    )

    grp = df.groupby("product_id", dropna=False)
    observations = grp.size().rename("source_rows")
    conflict_cols = {
        "actual_price_inr": "price_conflict",
        "discounted_price_inr": "discounted_price_conflict",
        "rating_count_value": "rating_count_conflict",
        "discount_pct": "discount_conflict",
    }
    for raw_col, flag_col in conflict_cols.items():
        flags = grp[raw_col].nunique(dropna=True).gt(1).rename(flag_col)
        selected = selected.join(flags, on="product_id")
    selected = selected.join(observations, on="product_id")

    product_columns = [
        "product_id", "product_name", "category", "category_level_1",
        "category_level_2", "category_level_3", "category_level_4",
        "category_leaf", "actual_price_inr", "discounted_price_inr",
        "discount_pct", "discount_pct_calculated", "rating_value",
        "rating_count_value", "about_product", "img_link", "product_link",
        "source_rows", "price_conflict", "discounted_price_conflict",
        "rating_count_conflict", "discount_conflict", "discount_inconsistent",
        "price_inverted", "source_row",
    ]
    products = selected[product_columns].rename(
        columns={"rating_value": "rating", "rating_count_value": "rating_count"}
    )

    # Kaggle puts multiple comma-separated review IDs/titles/texts in one cell.
    # Commas can also occur *inside* a review, so blind splitting corrupts text.
    # Each row below is a review BUNDLE, not necessarily an individual review.
    reviews = df[["source_row", "product_id", "review_id", "review_title", "review_content"]].copy()
    reviews = reviews.drop_duplicates(
        ["product_id", "review_id", "review_title", "review_content"], keep="first"
    )
    reviews = reviews.loc[reviews["review_content"].notna()].copy()
    reviews["review_title_clean"] = reviews["review_title"].map(clean_review_text)
    reviews["review_content_clean"] = reviews["review_content"].map(clean_review_text)
    reviews.insert(0, "review_bundle_key", range(1, len(reviews) + 1))
    reviews = reviews.sort_values(["product_id", "review_bundle_key"], kind="stable")

    # Relationship contract for Power BI: products[product_id] 1 -> * review_bundles[product_id].
    assert products["product_id"].is_unique, "Produtos ainda duplicados após deduplicação"
    assert reviews["product_id"].isin(products["product_id"]).all(), "Review sem produto correspondente"

    report = {
        "source_file": input_path.name,
        "source_rows": int(len(df)),
        "unique_products": int(len(products)),
        "duplicate_product_rows": int(len(df) - len(products)),
        "products_with_multiple_rows": int(products["source_rows"].gt(1).sum()),
        "review_bundles": int(len(reviews)),
        "review_bundles_discarded_as_duplicates_or_empty": int(len(df) - len(reviews)),
        "source_invalid_rating_rows": int(df["rating_value"].isna().sum()),
        "source_missing_rating_count_rows": int(df["rating_count_value"].isna().sum()),
        "products_missing_rating": int(products["rating"].isna().sum()),
        "products_missing_rating_count": int(products["rating_count"].isna().sum()),
        "products_with_price_conflict": int(products["price_conflict"].sum()),
        "products_with_discounted_price_conflict": int(products["discounted_price_conflict"].sum()),
        "products_with_rating_count_conflict": int(products["rating_count_conflict"].sum()),
        "products_with_discount_inconsistency": int(products["discount_inconsistent"].sum()),
        "products_with_inverted_prices": int(products["price_inverted"].sum()),
        "currency": "INR",
        "assumptions": [
            "rating_count is a snapshot of total product ratings; duplicates are not additive",
            "one review_bundles row may contain several individual customer reviews",
            "no dates, sales, revenue or customer exposure are available",
            "no live INR/BRL conversion; prices remain in the original currency",
            "when repeated product records conflict, select the row with highest valid rating_count",
        ],
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    products.to_csv(output_dir / "products.csv", index=False, encoding="utf-8-sig")
    reviews.to_csv(output_dir / "review_bundles.csv", index=False, encoding="utf-8-sig")
    (output_dir / "quality_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepara Amazon CSV para análise no Power BI")
    parser.add_argument("--input", required=True, type=Path, help="CSV original amazon.csv")
    parser.add_argument("--output-dir", default=Path("data/processed"), type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.input, args.output_dir), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
