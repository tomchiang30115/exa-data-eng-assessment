import os
import unittest
import pandas as pd
import json
from FHIR_data_loader import read_nested_json, normalize_nested_json, process_json_files, create_resource_dataframes

# Provide the path to a test JSON file
TEST_JSON_FILE = r"C:\Users\ihsiu\Desktop\exa-data-eng-assessment-main\data\sample\Deedra511_Wilkinson796_cced3031-d98c-d870-5dce-f0086d8c7a34.json"

class TestETLPipeline(unittest.TestCase):
    def setUp(self):
        # Load the test JSON file for use in the tests
        with open(TEST_JSON_FILE, 'r', encoding="utf8") as file:
            self.test_data = json.load(file)

    def test_read_nested_json(self):
        # Test if the read_nested_json function reads JSON data correctly
        data = read_nested_json(TEST_JSON_FILE)
        self.assertDictEqual(data, self.test_data, "Failed to read JSON data")

    def test_normalize_nested_json(self):
        # Test if the normalize_nested_json function correctly flattens nested JSON data
        df = normalize_nested_json(self.test_data)
        self.assertIsInstance(df, pd.DataFrame, "normalize_nested_json should return a DataFrame")

    def test_process_json_files(self):
        # Test if the process_json_files function correctly processes JSON files and returns a DataFrame
        test_directory = os.path.dirname(TEST_JSON_FILE)
        combined_df = process_json_files(test_directory)
        self.assertIsInstance(combined_df, pd.DataFrame, "process_json_files should return a DataFrame")

    def test_create_resource_dataframes(self):
        # Test if the create_resource_dataframes function correctly creates a dictionary of DataFrames
        test_directory = os.path.dirname(TEST_JSON_FILE)
        combined_df = process_json_files(test_directory)
        dataframes_dict = create_resource_dataframes(combined_df)
        self.assertIsInstance(dataframes_dict, dict, "create_resource_dataframes should return a dictionary")
        # Check if each value in the dictionary is a DataFrame
        for df in dataframes_dict.values():
            self.assertIsInstance(df, pd.DataFrame, "Values in the dictionary should be DataFrames")

if __name__ == "__main__":
    unittest.main()
