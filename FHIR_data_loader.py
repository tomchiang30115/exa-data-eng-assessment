import os
import pandas as pd
import json
import psycopg2
import time
import psycopg2.extras
import numpy as np
import argparse


def read_nested_json(file_path):
    # Extract json files from allocated file path and return json format data
    with open(file_path, "r", encoding="utf8") as file:
        data = json.load(file)
    return data


def normalize_nested_json(data):
    # Use pandas json_normalize() to flatten the nested JSON data
    df = pd.json_normalize(data, record_path=["entry"], max_level=2, sep="_")
    return df


def process_json_files(json_directory):

    all_dataframes = []

    # Iterate through all files in the directory
    for filename in os.listdir(json_directory):
        if filename.endswith(".json"):
            file_path = os.path.join(json_directory, filename)
            nested_json_data = read_nested_json(file_path)
            normalized_df = normalize_nested_json(nested_json_data)
            all_dataframes.append(normalized_df)

    # Concatenate all dataframes into a single dataframe
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    return combined_df


def create_resource_dataframes(combined_df):

    resourceType_lst = combined_df["resource_resourceType"].unique().tolist()

    # Create an empty dictionary to store the dataframes
    dataframes_dict = {}

    # Loop through each resource type and create the respective dataframe
    for rT in resourceType_lst:
        # Filter the rows for the current resource type and drop columns with NaN values
        df = (
            combined_df.loc[combined_df["resource_resourceType"] == rT]
            .dropna(axis=1)
            .convert_dtypes()
        )

        # Store the dataframe in the dictionary with the resource type as the key
        dataframes_dict[rT] = df

    return dataframes_dict


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


def create_table_from_dataframe_bulk(
    df, table_name, db_name, db_user, db_password, db_host, db_port
):
    try:
        # Connect to the 'fhir_db' database
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


# Replace the data_type_mapping function with a dictionary mapping of data types
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


def parse_arguments():
    parser = argparse.ArgumentParser(description="FHIR Data Loader Script")

    parser.add_argument(
        "--database_name", required=True, help="Name of the new database"
    )
    parser.add_argument("--database_user", required=True, help="Database user")
    parser.add_argument("--database_password", required=True, help="Database password")
    parser.add_argument(
        "--database_host",
        default="localhost",
        help="Database host (default: localhost)",
    )
    parser.add_argument(
        "--database_port", default=5432, type=int, help="Database port (default: 5432)"
    )

    return parser.parse_args()


if __name__ == "__main__":

    # Add a timer to measure the script's execution time
    start_time = time.time()

    json_directory = (
        "data"  # Replace this with the path to your directory containing JSON files
    )

    combined_df = process_json_files(json_directory)
    dataframes_dict = create_resource_dataframes(combined_df)

    # Replace these variables with your actual database credentials
    # new_database_name = "FHIR_DB"
    # database_user = "postgres"
    # database_password = "password0123"
    # database_host = "localhost"  # Typically "localhost" if running locally, change it to host.docker.internal if building docker image
    # database_port = 5432  # Replace this with the correct integer port number

    args = parse_arguments()

    # Access the credentials from the command-line arguments
    new_database_name = args.database_name
    database_user = args.database_user
    database_password = args.database_password
    database_host = args.database_host
    database_port = args.database_port

    # Check if the database already exists
    create_database(
        new_database_name,
        database_user,
        database_password,
        database_host,
        database_port,
    )

    # Assuming dataframes_dict contains the DataFrames
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
    dataframes_dict = {restype: dataframes_dict[restype] for restype in resourceType}

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
