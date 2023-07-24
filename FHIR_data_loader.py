# Import necessary libraries
import os
import pandas as pd
import json
import psycopg2
import time
import psycopg2.extras
import numpy as np
import argparse

# Define a function to read nested JSON data from a file
def read_nested_json(file_path):
    # Open the file and load JSON data into a Python dictionary
    with open(file_path, "r", encoding="utf8") as file:
        data = json.load(file)
    return data

# Define a function to normalize nested JSON data using pandas json_normalize()
def normalize_nested_json(data):
    # Use pandas json_normalize() to flatten the nested JSON data
    # The 'record_path' parameter specifies the path to the nested records in the JSON
    # The 'max_level' parameter limits the depth of normalization to level 2
    # The 'sep' parameter is used to separate the column names when flattening nested dictionaries
    df = pd.json_normalize(data, record_path=["entry"], max_level=2, sep="_")
    return df

# Define a function to process all JSON files in a directory
def process_json_files(json_directory):

    # Create an empty list to store DataFrames from all JSON files
    all_dataframes = []

    # Iterate through all files in the directory
    for filename in os.listdir(json_directory):
        # Check if the file has a '.json' extension
        if filename.endswith(".json"):
            # Create the full file path
            file_path = os.path.join(json_directory, filename)
            # Read the nested JSON data from the file
            nested_json_data = read_nested_json(file_path)
            # Normalize the nested JSON data and get a DataFrame
            normalized_df = normalize_nested_json(nested_json_data)
            # Append the DataFrame to the list
            all_dataframes.append(normalized_df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    return combined_df

# Define a function to create individual DataFrames for each resource type
def create_resource_dataframes(combined_df):
    # Get a list of unique resource types from the 'resource_resourceType' column
    resourceType_lst = combined_df["resource_resourceType"].unique().tolist()

    # Create an empty dictionary to store the DataFrames
    dataframes_dict = {}

    # Loop through each resource type and create the respective DataFrame
    for rT in resourceType_lst:
        # Filter the rows for the current resource type and drop columns with NaN values
        df = (
            combined_df.loc[combined_df["resource_resourceType"] == rT]
            .dropna(axis=1)
            .convert_dtypes()
        )

        # Store the DataFrame in the dictionary with the resource type as the key
        dataframes_dict[rT] = df

    return dataframes_dict

# Define a function to create a new PostgreSQL database
def create_database(db_name, db_user, db_password, db_host, db_port):
    try:
        # Connect to the default "postgres" database first to create a new database
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )

        # Set autocommit to True
        conn.autocommit = True

        # Create a new cursor to execute queries
        cur = conn.cursor()

        # Check if the database already exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (db_name,))
        exists = cur.fetchone()

        if not exists:
            # Create the new database
            cur.execute(f"CREATE DATABASE {db_name};")
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")

        # Close the cursor and connection
        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print("Error:", e)

# Define a function to create a PostgreSQL table and insert data from a DataFrame in bulk
def create_table_from_dataframe_bulk(
    df, table_name, db_name, db_user, db_password, db_host, db_port
):
    try:
        # Connect to the specified database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )

        # Set autocommit to True
        conn.autocommit = True

        # Create a new cursor to execute queries
        cur = conn.cursor()

        # Create the table with columns corresponding to DataFrame data types
        cols = ", ".join(
            [f"{col} {data_type_mapping[str(df.dtypes[col])]}" for col in df.columns]
        )
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols});"
        cur.execute(create_table_query)

        # Use psycopg2.extras.execute_values to perform bulk inserts
        # Convert the DataFrame to a list of tuples for bulk insertion
        # Convert complex data types (dicts, lists) to JSON strings
        # Convert NumPy boolean values to Python boolean values
        values = []
        for row in df.itertuples(index=False):
            converted_row = []
            for val in row:
                if isinstance(val, (dict, list)):
                    converted_row.append(json.dumps(val))
                elif isinstance(val, np.bool_):
                    converted_row.append(bool(val))
                else:
                    converted_row.append(val)
            values.append(tuple(converted_row))

        # Use %s as the placeholder for each value in the query
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s;"

        # Execute the bulk insert query
        psycopg2.extras.execute_values(cur, insert_query, values)

        print(f"Table '{table_name}' created and data inserted successfully.")

        # Close the cursor and connection
        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print("Error:", e)

# Dictionary mapping of data types used for creating PostgreSQL table columns
data_type_mapping = {
    "object": "TEXT",
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "SMALLINT",
    "int8": "SMALLINT",
    "float64": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "datetime64[ns, tz]": "TIMESTAMP WITH TIME ZONE",
    "timedelta64[ns]": "INTERVAL",
    "string": "VARCHAR",  # Add this mapping for 'string' data type
    # Add other mappings as needed
}

# Define a function to parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="FHIR Data Loader Script")

    # Add required arguments for database credentials
    parser.add_argument(
        "--database_name", required=True, help="Name of the new database"
    )
    parser.add_argument("--database_user", required=True, help="Database user")
    parser.add_argument("--database_password", required=True, help="Database password")

    # Add optional arguments for database host and port
    parser.add_argument(
        "--database_host",
        default="localhost",
        help="Database host (default: localhost)",
    )
    parser.add_argument(
        "--database_port", default=5432, type=int, help="Database port (default: 5432)"
    )

    return parser.parse_args()

# Main code starts here

if __name__ == "__main__":

    # Add a timer to measure the script's execution time
    start_time = time.time()

    # Set the path to the directory containing JSON files
    json_directory = (
        "data"  # Replace this with the path to your directory containing JSON files
    )

    # Process all JSON files and combine them into a single DataFrame
    combined_df = process_json_files(json_directory)

    # Create individual DataFrames for each resource type and store them in a dictionary
    dataframes_dict = create_resource_dataframes(combined_df)

    # Parse command-line arguments to get database credentials
    args = parse_arguments()
    new_database_name = args.database_name
    database_user = args.database_user
    database_password = args.database_password
    database_host = args.database_host
    database_port = args.database_port

    # Check if the database already exists or create a new one
    create_database(
        new_database_name,
        database_user,
        database_password,
        database_host,
        database_port,
    )

    # List of resource types to process
    resourceType = [
        "Encounter",
        "Patient",
        "Condition",
        "DiagnosticReport",
        "ExplanationOfBenefit",
        "MedicationRequest",
        "CareTeam",
        "CarePlan",
        "Procedure",
        "Immunization",
        "Observation",
        "Provenance",
        "Device",
    ]

    # Filter the dictionary to include only the desired resource types
    dataframes_dict = {restype: dataframes_dict[restype] for restype in resourceType}

    # Database name where tables will be created
    database_name = "fhir_db"

    # Iterate through the DataFrames and insert them into corresponding tables
    for table_name, dataframe in dataframes_dict.items():
        create_table_from_dataframe_bulk(
            dataframe,
            table_name,
            database_name,
            database_user,
            database_password,
            database_host,
            database_port,
        )

    # Calculate and print the execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Script executed in {execution_time:.2f} seconds.")
