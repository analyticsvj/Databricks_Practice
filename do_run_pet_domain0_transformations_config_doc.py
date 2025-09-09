# Databricks notebook source
# MAGIC %run ./pet_config_validators

# COMMAND ----------

dbutils.widgets.text("dataset_id", "")
dbutils.widgets.text("source_file_path", "")
dbutils.widgets.dropdown("stage", "Data-Provisioning", ["Data-Provisioning", "Domain-0-Transformation"])
dbutils.widgets.dropdown("overwrite", "False", ["True", "False"])

dataset_id = dbutils.widgets.get("dataset_id")
source_file_path = dbutils.widgets.get("source_file_path")
stage = dbutils.widgets.get("stage")
overwrite = dbutils.widgets.get("overwrite")

log_info(f"Running Loader for dataset: {dataset_id}, stage: {stage}, overwrite: {overwrite}")

result = dbutils.notebook.run(
    "pet_domain_0_transformations_config",
    timeout_seconds=0,
    arguments={
        "dataset_id": dataset_id,
        "source_file_path": source_file_path,
        "stage": stage,
        "overwrite": overwrite
    }
)

log_info(f"Loader Result: {result}")

