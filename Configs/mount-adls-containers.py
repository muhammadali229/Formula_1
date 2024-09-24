# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
  # service principal
  # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1scope', key = 'formula-1-client-id') 
    client_secret = dbutils.secrets.get(scope = 'formula1scope', key = 'formula-1-client-secret')
    tenant_id = dbutils.secrets.get(scope = 'formula1scope', key = 'formula-1-tenantid')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Get account access key from Key Vault
    # storage_account_access_key = dbutils.secrets.get('storageaccountaccesstoken', 'storageaccountaccesskey')
    # Set spark configurations
    # configs = {
    #   f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key
    # }
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      # use abfss protocol when use service principal
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      # source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

storage_account_name = "learningdbsa448"
mount_adls(storage_account_name, 'demo')
# mount_adls(storage_account_name, 'raw')
# mount_adls(storage_account_name, 'process')
# mount_adls(storage_account_name, 'presentation')

# COMMAND ----------

