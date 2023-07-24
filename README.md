# EMIS Patient Data Pipeline (exa-data-eng-assessment)


## Introduction

This project is a data pipeline that transforms patient FHIR data and load it into a relational database such as PostgreSQL. The pipeline is designed to receive incoming FHIR messages, process and transform the data, and write to a SQL database.

## Technology Stack

This data pipeline is built using the following technologies:

- Python 3.9 - the programming language used to develop the pipeline
- PostgreSQL - the relational databased used for the loading part of the pipeline
- Docker - used for containerization

## Setup Instructions

To get started with this data pipeline, clone this repository from Github to your local machine. Then, follow either of these steps:

## Local machine

In the root directory, run the following commands to install the required dependencies:
```
pip install --no-cache-dir --upgrade -r requirements.txt
```
Then, run the pipeline by putting the command below in the terminal:

```
python FHIR_data_loader.py --database_name <DATABASE_NAME> --database_user <DATABASE_USER> --database_password <DATABASE_PASSWORD> --database_host <DATABASE_HOST> --database_port <DATABASE_PORT>
```

## Docker

Install Docker on your machine. Navigate to the root directory and run the following commands in the terminal to build the Docker container:

```
docker build -t <your container name here> .
```

Then, run the following command to start the container (ensure you use the same container name as above):

```
docker run --name <CONTAINER_NAME> -it --rm --network="host" <CONTAINER_IMAGE_NAME> --database_name <DATABASE_NAME> --database_user <DATABASE_USER> --database_password <DATABASE_PASSWORD> --database_host <DATABASE_HOST> --database_port <DATABASE_PORT>
```

## Testing

To test the code FHIR_data_loader.py, run the following command to begin testing the code:

```
python test.py
```
Please ensure to input the password and the host to run the test script.

## Directory Structure

```
- exa-data-eng-assessment
    |- data
    |- Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json
    |- Aaron697_Jerde200_6fa23508-960e-ff22-c3d0-0519a036543b.json
    ...
    |- sample
        |- Deedra511_Wilkinson796_cced3031-d98c-d870-5dce-f0086d8c7a34.json
        |- James276_Champlin946_4cef483d-6b1b-a284-b8fd-1de7f5aba0a4.json
    |- jupyter_notebook
        |- Initial ET pipeline.ipynb
- Dokerfile
- FHIR_data_loader.py
- README.md
- requirements.txt
- test.py
```


`exa-data-eng-assessment` - This directory contains the main pipeline code.

`data` - Directory where all FHIR .json data files are, which are to be processed by the pipeline.

`Dockerfile` - This file is used to build the Docker image for the pipeline.

`Initial ET pipeline.ipynb` - This Jupyter notebook is used in the beginning to explore around the json files.

`FHIR_data_loader.py` - This is the main Python script that initiates the pipeline and load the data into PostgreSQL database.

`README.md` - This file contains the documentation for the project.

`requirements.txt` - This file contains the Python dependencies required by the pipeline.

`test.py` - This file contains the Python test script for the main pipeline.