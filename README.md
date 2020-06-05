# TR_CHALLENGE

## Requirements

- Python
- Docker
- Make

## How to run it?

- Git clone the repo.
- cd to the root directory and run : `make run_pipeline`

A docker container with a Postgres database will be created and the `main.py` script will run automatically.

The pipeline takes some time to run, since dowloading and inserting the yearly weather data is pretty heavy, but there is logging informing of every step.

Once the whole pipeline runs, you can connect the Database where the tableswith the reasults are ready:

Schema: `tr`

Tables:
```
max_temp
median_temperatures_city
median_temperatures_germany
percentile_city
stadt
stations
stations_5km
yearly_data
```

Credentials to connect to the database:
```
(user='user',
password='password',
host='localhost',
database='database')
```