import os
import unittest
from datetime import datetime
from filedownloadstat.log_parser import LogParser
import yaml

class TestLogParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Load YAML configuration before running tests."""
        yaml_path = os.path.join(os.path.dirname(__file__), "../params/pride-local-params.yml")
        with open(yaml_path, "r") as file:
            cls.config = yaml.safe_load(file)

        # Initialize LogParser with YAML config
        cls.parser = LogParser("",
            resource_list=cls.config["resource_identifiers"],
            completeness_list=cls.config["completeness"],
            accession_pattern= "^PXD\\d{6}$" #cls.config["accession_pattern"]
        )
    def test_valid_row(self):
        """Test parsing a correctly formatted row."""
        row = [ "2023-01-01T23:57:16.000Z",
                "caf4815df53c650523eb4943a05bc7b7906e5507",
                "669943668",
                "/pride/data/archive/2016/12/PXD004242/filename.raw",
                "OUT",
                "03dbae9a96db63fa62487cd3c134d05230858127",
                "Complete",
                "United Kingdom",
                "Cambridgeshire",
                "Sawston",
                "52.1202,0.1845",
                "http",
                "public"
               ]
        parsed = self.parser.parse_row(row, 1)

        self.assertIsInstance(parsed, dict)
        self.assertEqual(parsed["year"], 2023)
        self.assertEqual(parsed["month"], 1)
        self.assertEqual(parsed["date"], datetime(2023, 1, 1).date())
        self.assertEqual(parsed["accession"], "PXD004242")
        self.assertEqual(parsed["filename"], "filename.raw")
        self.assertEqual(parsed["completed"], "complete")
        self.assertEqual(parsed["country"], "United Kingdom")
        self.assertEqual(parsed["method"], "http")

    def test_invalid_column_count(self):
        """Test parsing a row with missing columns."""
        row = [
            "2023-01-01T23:57:16.000Z", "caf4815df53c650523eb4943a05bc7b7906e5507",
            "669943668", "/pride/data/archive/2016/12/PXD004242/filename.raw", "OUT"
        ]
        parsed = self.parser.parse_row(row, 2)
        self.assertIsNone(parsed)  # Should return None due to incorrect column count

    def test_non_matching_accession(self):
        """Test parsing a row that does not contain the accession pattern."""
        row = [
            "2023-01-01T23:57:16.000Z", "caf4815df53c650523eb4943a05bc7b7906e5507",
            "669943668", "/pride/data/archive/2016/12/PXD999999999/filename.raw",
            "OUT", "03dbae9a96db63fa62487cd3c134d05230858127", "Complete",
            "United Kingdom", "Cambridgeshire", "Sawston", "52.1202,0.1845", "http", "public"
        ]
        parsed = self.parser.parse_row(row,  3)
        self.assertIsNone(parsed)  # Should return None because the accession doesn't match

    def test_valid_row_globus(self):
        """Test parsing a correctly formatted row."""

        row = [
                "2023-11-16T14:17:18.963Z",
                "206692352bb826652461c90c0e056045282f508d",
                "619815434",
                "/xfer/public/pride/data/archive/2023/11/PXD012558/LF_Natowicz_10_091916.raw.mgf",
                "OUT",
                "f1e078517ffda4b7cc0389661184ecc05cf6682f",
                "Complete",
                "United Kingdom",
                "Cambridgeshire",
                "Sawston",
                "52.1202,0.1845",
                "gridftp-globus",
                "public"
                ]
        parsed = self.parser.parse_row(row, 1)
        self.assertIsNotNone(parsed, dict)

if __name__ == '__main__':
    unittest.main()
