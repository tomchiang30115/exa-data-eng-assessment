FROM python:3.9

WORKDIR /FHIR_loader-main

COPY requirements.txt .

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

ENTRYPOINT ["python", "./FHIR_data_loader.py"]
