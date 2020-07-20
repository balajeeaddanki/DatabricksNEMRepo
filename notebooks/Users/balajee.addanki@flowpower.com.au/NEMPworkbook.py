# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/customer/Ausgrid/Jun_20

# COMMAND ----------

data = spark.read.csv("dbfs:/mnt/customer/Ausgrid/Jun_20/Ausgrid Recieved on 01_Jun_2020 12_50_29 PM 446.csv", header="false", inferSchema="true")
data.cache()

# COMMAND ----------

data.show()

# COMMAND ----------

# Mount ADLS Gen2 to DBFS
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "3797a11e-184a-47f8-9c18-5d6684051d22",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="analyticsKeyVault",key="Databrickssecret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b9b745a1-8100-4607-9c62-6d19002c1963/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://analytics@flowpowerstorage.dfs.core.windows.net/raw/NEM",
  mount_point = "/mnt/customer",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/customer")

# COMMAND ----------

arrayStructureData = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
        ]

# COMMAND ----------

df = spark.createDataFrame(data = arrayStructureData)

# COMMAND ----------

df.show()