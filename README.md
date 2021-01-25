# Data Engineer NanoDegree: Data Lake Project 


## Introduction

Sparkify is a fast scaling startup, since the amount of data is increasing each day it
was decided to build an pipeline to extract the raw data stored in a S3 bucket and 
process it into a new structure of dimensional tables stored in the form of a Data Lake. 

This will automate the entire data fusion process combining multiple data sources from AWS S3 into a new structure. The resulting data will be stored on AWS S3 again, to allow future access to this preprocessed data.

## Project Structure

```
.
├── dl.cfg (The main configuration file, stores the AWS credentials)
├── etl.py (The ETL pipeline in charge to transform and partition the data)
└── README.md (The project description file)
```

## Source data

The original data is stored in a single S3 bucket inside it there are a couple of folders:

* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`

The song data has the same structure of the diagram bellow and inside each subfolder the actual song data is stored inside a json file.

```
song_data
└── A
    ├── A
    │   ├── A
    │   │   ├── TRAAAAW128F429D538.json
```
The log data is also stored in a similar folder structure, data is classified by year and month and the day events are stored in a collection of json files.

```
log_data
└── 2018
    └── 11
        ├── 2018-11-01-events.json
        ├── 2018-11-02-events.json
```

## How to run the script

* Setup an AWS IAM role and user with S3 access.
* Enter the IAM's secret id and key inside the dl.cfg file.
* Create an S3 bucket on the us-west-2 region and use the name on the etl.py file 
as the value for output_data.
* Run the long process on the shell.
```
$ python3 etl.py
```

## Resulting Structure

The result should be collection of folders with varios files in parquet format
that could be used later as part of the analytics process. 

| name | type | columns |
| ---- | ---- | ------- |
| users | dimension table | user_id, first_name, last_name, gender, level |
| songs | dimension table |  song_id, title, artist_id, year, duration |
| artists | dimension table | artist_id, name, location, lattitude, longitude |
| time | dimension table | start_time, hour, day, week, month, year, weekday |
| song_plays | fact table | songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent |

The ETL pipeline loads the S3 data sources into Spark dataframes, the it proceeds to transform the data into the described structure and schema and writes the resulting dataframes as parquet files back into a different S3 bucket.
