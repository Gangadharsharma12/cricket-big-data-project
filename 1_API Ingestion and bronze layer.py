# Databricks notebook source
# DBTITLE 1,Import the required libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

base_path = "/Volumes/dgs/default/cricket_api_project"


# COMMAND ----------

# DBTITLE 1,Calling cricket api
api_key = 'c232d58c-51de-4352-b22d-8697971217e1'
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={api_key}&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent = 2)[:2000])

# COMMAND ----------

# DBTITLE 1,Save Raw API to volume
raw_file_path = f"{base_path}/current_matches_raw.json"

with open(raw_file_path,'w') as file:
    json.dump(api_data, file)

print(f"RAW API data is saved at the: {raw_file_path}")

# COMMAND ----------

# DBTITLE 1,create bronze layer df or table
bronze_data = [{
   "source_api": api_url,
   "raw_json": json.dumps(api_data),
   "ingestion_time": None

}]


bronze_schema = "source_api string, raw_json string, ingestion_time timestamp"

bronze_df = (spark.createDataFrame(bronze_data, schema = bronze_schema) 
            .withColumn("ingestion_time", current_timestamp())
            )

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,save bronze data to table
bronze_df.write \
         .format("delta") \
         .mode("overwrite") \
         .saveAsTable("dgs.default.cricket_bronze_curreent_matches")

print("Bronze table created successfully")
