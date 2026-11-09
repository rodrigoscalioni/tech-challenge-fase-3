"""Testes mínimos de qualidade, com unittest da biblioteca padrão."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from dataprep import prepare, clean_review_text  # noqa: E402


class AmazonDataprepTests(unittest.TestCase):
    def test_dedupe_products_without_summing_rating_count(self):
        rows = [
            dict(product_id="A", product_name="Cabo", category="Eletronicos|Cabos",
                 actual_price="₹1,000", discounted_price="₹600",
                 discount_percentage="40%", rating="4.3", rating_count="1,000",
                 review_id="R1,R2", review_title="Bom,Ótimo", review_content="Bom, gostei"),
            dict(product_id="A", product_name="Cabo", category="Eletronicos|Cabos",
                 actual_price="₹1,000", discounted_price="₹500",
                 discount_percentage="50%", rating="4.3", rating_count="1,200",
                 review_id="R3", review_title="Barato", review_content="Bom, resistente"),
            dict(product_id="B", product_name="Mouse", category="Eletronicos|Mouse",
                 actual_price="₹300", discounted_price="₹200",
                 discount_percentage="33%", rating="|", rating_count="",
                 review_id="R4", review_title="Ruim", review_content="http://abc.com/x.jpg Frágil"),
        ]
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            pd.DataFrame(rows).to_csv(p / "raw.csv", index=False)
            report = prepare(p / "raw.csv", p / "processed")
            products = pd.read_csv(p / "processed/products.csv")
            reviews = pd.read_csv(p / "processed/review_bundles.csv")
            self.assertEqual(report["source_rows"], 3)
            self.assertEqual(report["unique_products"], 2)
            self.assertEqual(products.loc[products.product_id == "A", "rating_count"].iloc[0], 1200)
            self.assertEqual(report["products_with_discounted_price_conflict"], 1)
            self.assertEqual(report["source_invalid_rating_rows"], 1)
            self.assertTrue(pd.isna(products.loc[products.product_id == "B", "rating"].iloc[0]))
            self.assertEqual(len(reviews), 3)
            self.assertTrue((p / "processed/quality_report.json").exists())
            self.assertEqual(json.loads((p / "processed/quality_report.json").read_text())["currency"], "INR")

    def test_invalid_schema_fails_fast(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            pd.DataFrame({"product_id": ["A"]}).to_csv(p / "bad.csv", index=False)
            with self.assertRaisesRegex(ValueError, "Colunas obrigatórias ausentes"):
                prepare(p / "bad.csv", p / "out")

    def test_review_text_preserves_commas(self):
        self.assertEqual(clean_review_text("bom, ótimo https://site.com/img.jpg produto"), "bom, ótimo produto")


if __name__ == "__main__":
    unittest.main()
