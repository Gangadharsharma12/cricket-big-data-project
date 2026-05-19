# Databricks notebook source
# DBTITLE 1,Importing libraries
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Reading bronze table
bronze_df = spark.table("dgs.default.cricket_bronze_curreent_matches")

raw_json = bronze_df.select("raw_json").collect()[0]['raw_json']
api_data = json.loads(raw_json)

matches = api_data.get("data", [])

print("Total matches found: ", len(matches))

print(matches[0] if len(matches) > 0 else 'no matches found')

# COMMAND ----------

# DBTITLE 1,extract only useful fields
silver_rows = []

for match in matches:
    teams = match.get("teams", [])
    score = match.get("score", [])
    team_1 = teams[0] if len(teams) > 0 else None
    team_2 = teams[1] if len(teams) > 1 else None

    score_1 = None
    score_2 = None

if len(score) > 0:
    score_1 = score[0].get("runs", None)
    score_2 = score[1].get("runs", None)
print(score)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dgs.default.cricket_bronze_curreent_matches
