# Databricks notebook source
# MAGIC %run /adf/utils/merger

# COMMAND ----------

# DBTITLE 1,Initial Setup
# Imports
from pyspark.sql.functions import when, col, lit, to_json
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from datetime import datetime
import json
import requests
from urllib.parse import urlencode
import ast
import os

# Parameters
storage_account = dbutils.widgets.get("storage_account")
folder_path = dbutils.widgets.get("folder_path")
file_name = dbutils.widgets.get("file_name")
end_point = dbutils.widgets.get("end_point")
fields = dbutils.widgets.get("fields")
account_id = dbutils.widgets.get("account_id")
other_params = dbutils.widgets.get("other_params")

#Convert strings to lists/dicts
fields = ast.literal_eval(fields)
other_params = ast.literal_eval(other_params)

#Retrieve token from key vault
token = dbutils.secrets.get(scope=os.environ["SECRETSCOPENAME"],key="fb-ads-token")

# Run Spark configuration for storage account from Storage Utils
auth_with_storage(storage_account)

# COMMAND ----------

def get(url):

    results = []   
    next_exists = True
    next = None
    while (next_exists):
        if next:
            r = requests.get(next).json()
        else:
            r = requests.get(url).json()

        results += r['data']

        try:
            next = r['paging']['next']
            next_exists = True
        except KeyError:
            next_exists = False

    return results

# COMMAND ----------

def create_df(dictionary, field_list):
    schema = StructType([StructField(field, StringType()) for field in field_list])
    return spark.createDataFrame(dictionary, schema)

# COMMAND ----------

# DBTITLE 1,Execute
#This will be passed in with all API calls
data = {
  'access_token': token,
  'fields': fields,
  **other_params
}

#The Facebook API endpoint to hit, since it has to be a GET request we embed data as query params
url = f'https://graph.facebook.com/v9.0/act_{account_id}/{end_point}?{urlencode(data)}'

r = get(url)

#The actions field is nested JSON, so flatten the field if it is present
if 'actions' in fields:
    flattened = []
    for row in r:
        for action in row.get('actions', []):
            obj = {**row, **action}
            del obj['actions']
            flattened.append(obj)

    r = flattened
    fields = flattened[0].keys()

#Transform the JSON into a DF
df = create_df(r, fields)

# COMMAND ----------

# DBTITLE 1,Write to Container
#Container storage location
output_path = "abfss://ingest@" + storage_account + ".dfs.core.windows.net/" + folder_path + file_name

df.write.format("parquet").mode("overwrite").option("header", "true").save(output_path)