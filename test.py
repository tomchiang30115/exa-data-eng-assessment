import os
import unittest
import pandas as pd
import json
import psycopg2
from FHIR_data_loader import read_nested_json, normalize_nested_json, process_json_files, create_resource_dataframes, create_database, create_table_from_dataframe_bulk

# Provide the path to a test JSON file
TEST_JSON_FILE = "data\sample\Deedra511_Wilkinson796_cced3031-d98c-d870-5dce-f0086d8c7a34.json"

TEST_DB_NAME = "test_db_123456789"
TEST_DB_USER = "postgres"
TEST_DB_PASSWORD = input("Enter the database password: ")
TEST_DB_HOST = input("Enter the database host (localhost for local machine, host.docker.internal for docker): ")
TEST_DB_PORT = "5432"

# Define the expected data type mapping for the test DataFrame
data_type_mapping = {
    'fullurl': 'character varying',
    'request_method': 'character varying',
    'request_url': 'character varying',
    'resource_address': 'text',
    'resource_birthdate': 'character varying',
    'resource_communication': 'text',
    'resource_extension': 'text',
    'resource_gender': 'character varying',
    'resource_id': 'character varying',
    'resource_identifier': 'text',
    'resource_maritalstatus_coding': 'text',
    'resource_maritalstatus_text': 'character varying',
    'resource_meta_profile': 'text',
    'resource_multiplebirthboolean': 'boolean',
    'resource_name': 'text',
    'resource_resourcetype': 'character varying',
    'resource_telecom': 'text',
    'resource_text_div': 'character varying',
    'resource_text_status': 'character varying',
    # Add other mappings as needed
}

class TestETLPipeline(unittest.TestCase):
    def setUp(self):
        # Load the test JSON file for use in the tests
        with open(TEST_JSON_FILE, 'r', encoding="utf8") as file:
            self.test_data = json.load(file)
        
        self.test_db_name = TEST_DB_NAME
        self.test_db_user = TEST_DB_USER
        self.test_db_password = TEST_DB_PASSWORD
        self.test_db_host = TEST_DB_HOST
        self.test_db_port = TEST_DB_PORT
    
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

    def test_create_database(self):
        # Test creating a new database with unique name
        
        self.test_db_name = TEST_DB_NAME
        self.test_db_user = TEST_DB_USER
        self.test_db_password = TEST_DB_PASSWORD
        self.test_db_host = TEST_DB_HOST
        self.test_db_port = TEST_DB_PORT

    # Create the test database
        create_database(self.test_db_name, self.test_db_user, self.test_db_password, self.test_db_host, self.test_db_port)

        try:
            # Connect to the created database to check if it exists
            conn = psycopg2.connect(
                dbname=self.test_db_name,
                user=self.test_db_user,
                password=self.test_db_password,
                host=self.test_db_host,
                port=self.test_db_port,
            )

            # Set autocommit to True
            conn.autocommit = True

            # Create a new cursor to execute queries
            cur = conn.cursor()

            # Check if the database exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (self.test_db_name,))
            exists = cur.fetchone()

            # Close the cursor and connection
            cur.close()
            conn.close()

            self.assertIsNotNone(exists, "The database should exist after creation.")

        except psycopg2.Error as e:
            self.fail(f"Failed to connect to the database: {e}")

    def test_create_table_and_bulk_insert(self):
        self.maxDiff = None
        json_directory = "data/sample"
        combined_df = process_json_files(json_directory)
        dataframes_dict = create_resource_dataframes(combined_df)
        try:
            # Call the function to create the table and bulk insert the data
            create_table_from_dataframe_bulk(
                df=dataframes_dict['Patient'],
                table_name="test_table",
                db_name=TEST_DB_NAME,
                db_user=TEST_DB_USER,
                db_password=TEST_DB_PASSWORD,
                db_host=TEST_DB_HOST,
                db_port=TEST_DB_PORT,
            )

            # Connect to the database and check if the table exists
            conn = psycopg2.connect(
                dbname=TEST_DB_NAME,
                user=TEST_DB_USER,
                password=TEST_DB_PASSWORD,
                host=TEST_DB_HOST,
                port=TEST_DB_PORT,
            )

            # Set autocommit to True
            conn.autocommit = True

            # Create a new cursor to execute queries
            cur = conn.cursor()

            # Check if the table exists in the database
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table');")
            table_exists = cur.fetchone()[0]

            # Check if the data types of columns match the expected mapping
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_table';")
            columns_data_types = dict(cur.fetchall())

            # Close the cursor and connection
            cur.close()
            conn.close()

            # Assert that the table exists and data types match the expected mapping
            self.assertTrue(table_exists, "The table should exist after creation.")
            self.assertEqual(data_type_mapping, columns_data_types)

        except Exception as e:
            self.fail(f"Test failed with an exception: {e}")

        finally:
            # Clean up by dropping the test database
            try:
                conn = psycopg2.connect(
                    dbname="postgres",  # Connect to the default 'postgres' database
                    user=self.test_db_user,
                    password=self.test_db_password,
                    host=self.test_db_host,
                    port=self.test_db_port,
                )

                # Set autocommit to True
                conn.autocommit = True

                # Create a new cursor to execute queries
                cur = conn.cursor()

                # Terminate all connections to the test database (except the current one)
                cur.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid();", (self.test_db_name,))

                # Drop the test database
                cur.execute(f"DROP DATABASE IF EXISTS {self.test_db_name};")
                print(f"Database '{self.test_db_name}' dropped successfully.")

                # Close the cursor and connection
                cur.close()
                conn.close()

            except psycopg2.Error as e:
                self.fail(f"Failed to drop the test database: {e}")


if __name__ == "__main__":
    unittest.main()
