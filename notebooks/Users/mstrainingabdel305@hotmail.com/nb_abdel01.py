# Databricks notebook source
# MAGIC %fs ls file:/dbfs/mnt/data

# COMMAND ----------

data_storage_account_name = 'saabdel01'
data_storage_account_key = 'ipW+ACo6f9eLy+Z7XUd95yjmJK6/SgigwhIuqGya8ocqmh1khe1o9Nxfr9ovjzIwkqxJE9xD0Yb6+ASt/wPswQ=='

data_mount_point = '/mnt/data'

data_file_path = '/green_tripdata_2021-01.csv'

dbutils.fs.mount(
  source = f"wasbs://data@{data_storage_account_name}.blob.core.windows.net",
  mount_point = data_mount_point,
  extra_configs = {f"fs.azure.account.key.{data_storage_account_name}.blob.core.windows.net": data_storage_account_key})

display(dbutils.fs.ls("/mnt/data"))
#this path is available as dbfs:/mnt/data for spark APIs, e.g. spark.read
#this path is available as file:/dbfs/mnt/data for regular APIs, e.g. os.listdir

# COMMAND ----------

dbutils.fs.unmount("/mnt/data")

# COMMAND ----------

val accbbstorekey = dbutils.secrets.get(scope = "ScopeSecretAbdel01", key ="ipW+ACo6f9eLy+Z7XUd95yjmJK6/SgigwhIuqGya8ocqmh1khe1o9Nxfr9ovjzIwkqxJE9xD0Yb6+ASt/wPswQ==")
print(accbbstorekey)


# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.saabdel01.dfs.core.windows.net",
"ipW+ACo6f9eLy+Z7XUd95yjmJK6/SgigwhIuqGya8ocqmh1khe1o9Nxfr9ovjzIwkqxJE9xD0Yb6+ASt/wPswQ=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://data@saabdel01.dfs.core.windows.net/")
