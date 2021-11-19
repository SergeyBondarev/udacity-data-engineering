# Project summary

This is a first demo project in the Data Engineer Nanodegree Udacity program. Description of the project is given below.   

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Pipeline implemented in this project allows to transofrm JSON logs with user activity on the app and JSON metadata on the songs and load the result to Postgres database.

# Database schema




# Prerequisites for ETL pipeline

In order to run the scripts you'll need python. **Python 3.7** and higher is supported. Also it's better to work in isolated environment. There are plenty of ways to isolate the
environment (virtualenv, pyenv, anaconda, .etc). One of the simplest is to create new Anaconda environment with the following commands
```
conda create --name my-env
conda activate my-env
pip install -r requirements.txt
```

It's better to have docker installed. It would simplify a local start. Otherwise you can try to run postgres locally.

# Instructions on how to run pipeline

Run **docker-compose**. Note that for the purposes of demo password and username are specified directly in the repository. You wouldn't want to do this in real production.

```
docker-compose up
```

Then create tables with python script
```
python create_tables.py
```

Finally, run the ETL pipeline.

If you run this stuff with docker-compose you can now go to **localhost:8080** and after login review all your tables via postgres-adminer. Also you can easily manage your data or perform SQL queries using this simple tool.


# Console application

This pipeline can be used a console application as well. Currently this functionality is still under development.


