# Dataflow Challenge (Python)
This repo contains step by step guide on how I executed this challenge.
I won't disclaim challenge information, as this repo was made specifically for the team that created the test.

Author: Helio Ribeiro<br>
email: helioribeiropro@gmail.com<br><br>

Built and tested on:
- python 3.9
- apache-beam[gcp] 2.50.0
- geopy 1.18.0<br><br>

## Directory Structure
```bash
$ root .
 ├── docs
 │  ├── coding-challenge-steps.txt   # easy step-by-step overview
 ├── config.py                       # Apache Beam Pipeline configuration
 ├── easy.py                         # code used to solve the easy challenge
 ├── hard.py                         # code used to solve the hard challenge
 ├── README.md                       # this file
 ├── MANIFEST.in                     # package data
 ├── requirements.txt                # libraries used to run this challenge
 ├── LICENSE                         
 └── setup.py                        # the setup.py, provided by the challenge.
```

## 1. Verifying challenge results over BigQuery using SQL
Before I started creating the Dataflow ETL, I wanted to guarantee that I knew what to expect as an output, since executing this challenge over SQL becomes a much simpler matter.

## 2. Preparing my environment
Once I knew what to expect in terms of SQL, I have prepared the environment in terms of APIs, Cloud Storage and Github Repo so it would reflect the challenge's needs.

## 3. Building folder structure (For Apache Beam / Dataflow)
Created the structure above. Some people organize the scripts inside another folder, but for this demo I decided to put it on the root directory.

## 4. Cloning Git Repo on Cloud Shell
Inside Google's Cloud Shell --> git clone https://github.com/helioribeiro/dataflow-coding-challenge-delivery.git<br>
cd dataflow-coding-challenge-delivery

## 5. Easy Test
This test consists in providing 3 columns with a query on a single table.

### 5.1. Connecting to BigQuery
query = 'SELECT start_station_id, end_station_id FROM `bigquery-public-data.london_bicycles.cycle_hire`'
        extracted_data = (
            pipeline
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

### 5.2. Transformation
- Count rides for each combination of start_station_id and end_station_id
- Sort the output by the amount of rides
- Limit to 100 rows
        
### 5.3. Output
- Saves output as text file inside of a bucket.

### 5.4. Running the ETL script via Dataflow
Once the environment is set, just make sure you are at the folder dataflow-coding-challenge with all the libraries installed, then run 'python easy.py'

---

## 6. Hard Test
This test consists in providing 4 columns with queries from two tables, as well as calculations using the library geopy.

### 6.1. Connecting to BigQuery
-- Step 1: Query the data using SQL SELECT
        query = """
    SELECT
        hire.start_station_id AS start_station_id,
        hire.end_station_id AS end_station_id,
        stations.latitude AS start_station_latitude,
        stations.longitude AS start_station_longitude,
        stations_end.latitude AS end_station_latitude,
        stations_end.longitude AS end_station_longitude
    FROM
        `bigquery-public-data.london_bicycles.cycle_hire` AS hire
    JOIN
        `bigquery-public-data.london_bicycles.cycle_stations` AS stations
    ON
        hire.start_station_id = stations.id
    JOIN
        `bigquery-public-data.london_bicycles.cycle_stations` AS stations_end
    ON
        hire.end_station_id = stations_end.id
"""
        extracted_data = (
            pipeline
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

### 6.2. Transformation
- Calculate distances,
- Filter null values,
- Count rides,
- Calculate full distances based on the amoun of rides
- Sort and Limit to 100

### 6.3. Output
- Saves output as text file inside of a bucket.

### 6.4. Running the ETL script via Dataflow
Once the environment is set, just make sure you are at the folder dataflow-coding-challenge with all the libraries installed, then run 'python hard.py'

## 7. Final remarks
Overall, it was a pleasure to develop the code for this challenge and I am looking forward for our next conversations.

Cheers,
Helio Ribeiro.
