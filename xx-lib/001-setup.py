# Databricks notebook source
client_id = dbutils.secrets.get(scope = 'scope-dev', key = 'demolk-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'scope-dev', key = 'demolk-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'scope-dev', key = 'demolk-app-client-secret')

# COMMAND ----------

client_secret

# COMMAND ----------

def is_mounted(mount_point):
    try:
        dbutils.fs.ls(mount_point)
        return True
    except Exception as e:
        return False

# COMMAND ----------

container='consumption'
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

if is_mounted(f'/mnt/{container}'):
    dbutils.fs.unmount(f'/mnt/{container}')

dbutils.fs.mount(
  source = f"abfss://{container}@demolk.dfs.core.windows.net/",
  mount_point = f"/mnt/{container}",
  extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

if is_mounted('/mnt/raw'):
    dbutils.fs.unmount('/mnt/raw')

