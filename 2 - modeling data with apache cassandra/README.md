## ETL pipeline

ETL pipeline is presented in **etl.ipynb**. You can run the cells in order to extract the data from the **event_data** folder, perform necessary transformations and load it to the Apache Cassandra database.

## Installation of dependencies

It's better to have a separate environment
```
conda create --name myenv python=3.7
conda activate myenv
pip install -r requirements.txt
```

## Local start

If you want to start the Apache Cassandra locally, just run
```
docker-compose up
```