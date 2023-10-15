# Databricks notebook source
service_credential = dbutils.secrets.get(scope="zorescope",key="zoresecret")

spark.conf.set("fs.azure.account.auth.type.rupeshstrg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rupeshstrg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rupeshstrg.dfs.core.windows.net", "1f1173a7-86b9-4def-b59b-dd07de6a81b9")
spark.conf.set("fs.azure.account.oauth2.client.secret.rupeshstrg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rupeshstrg.dfs.core.windows.net", "https://login.microsoftonline.com/69a259e4-c0ea-4d7c-a4a5-e32f2ee786b5/oauth2/token")

# COMMAND ----------

# MAGIC %fs ls abfss://rupeshcont@rupeshstrg.dfs.core.windows.net

# COMMAND ----------

# MAGIC %fs ls abfss://rupeshcont@rupeshstrg.dfs.core.windows.net/inbound/

# COMMAND ----------

df = spark.read.csv("abfss://rupeshcont@rupeshstrg.dfs.core.windows.net/inbound/YellowTaxis_202225.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("VendorID").display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------



# COMMAND ----------

df.groupBy("VendorID").count().display()

# COMMAND ----------

df.groupBy("VendorID").sum("total_amount").display()

# COMMAND ----------


