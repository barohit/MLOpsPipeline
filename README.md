This repo represents a standards Machine Learning Operations Pipeline. The Data ingestion folder currenetly only has sql seed scripts but will be expanded to include an API. 

The Feature Engineering folder represents the processes of extracting data from defined sources - right now Snowflake and PostgresSQL databases - and running jobs via Pyspark. (with some example jobs defined in the folder) and then publishing the data to a feature store. The feature store tables are defined in a script in the sql folder in the Data Ingestion folder. The feature process object combines data extraction and running jobs and will have the functionality to be run on external infrastructure such as AWS EMR or a local Hadoop instance. 

The model training process and monitoring will be added later. 