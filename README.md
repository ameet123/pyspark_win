## PySpark on Windows for Troubleshooting

### Install Pyspark on windows
https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3

Do this in `conda` environment with python 3.6.8

#### Tweaks:
Set up following environment variables
1. PYTHONPATH: point to py4j zip file
2. SPARK_HOME:
3. SPARK_LOCAL_DIRS: create a temp directory in a user accessible location for this.

#### Logging:
copy/create log4j.properties file under `spark installation\conf` folder

#### Jupyter
`conda install miktex`