# Databricks notebook source
import pandas as pd

files = [
    {"file":"map_cities"},
    {"file":"map_cancellation_reasons"},
    {"file":"map_payment_methods"},
    {"file":"map_ride_statuses"},
    {"file":"map_vehicle_makes"},
    {"file":"map_vehicle_types"}
]

for file in files:
    url = f"https://sk2026dluberprojectdev.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-04-22T16:34:45Z&se=2026-04-23T00:49:45Z&spr=https&sv=2025-11-05&sr=c&sig=fkTPkeZp1%2BfuGdPqyDNyLYK8C9IbrCz32n5Qj%2FNpC5M%3D"

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)

    # Writing Data to the Bronze layer
    df_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"uber.bronze.{file['file']}")

# COMMAND ----------

import pandas as pd

url = "https://sk2026dluberprojectdev.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=r&st=2026-04-22T16:34:45Z&se=2026-04-23T00:49:45Z&spr=https&sv=2025-11-05&sr=c&sig=fkTPkeZp1%2BfuGdPqyDNyLYK8C9IbrCz32n5Qj%2FNpC5M%3D"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta").mode("overwrite").saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")
     