# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "xxx",
           "fs.azure.account.oauth2.client.secret": "xxx",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/xxxx/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://staging@zzz.dfs.core.windows.net/",
  mount_point = "/mnt/stg",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/stg/files/'))
