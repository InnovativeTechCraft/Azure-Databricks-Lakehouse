import yaml
from pyspark.sql import SparkSession, functions as F

def load_settings(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_uc_objects(spark, catalog, schema, volume):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

def bronze_base_path(cfg: dict):
    c = cfg["storage"]["catalog"]
    s = cfg["storage"]["schema"]
    v = cfg["storage"]["volume"]
    return f"/Volumes/{c}/{s}/{v}"

def ingest_union(settings_file: str = "config/settings.dev.yaml"):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    cfg = load_settings(settings_file)

    catalog = cfg["storage"]["catalog"]
    schema  = cfg["storage"]["schema"]
    volume  = cfg["storage"]["volume"]
    union_target = cfg["storage"]["union_target"]

    ensure_uc_objects(spark, catalog, schema, volume)

    bronze_root = bronze_base_path(cfg)
    target = f"{bronze_root}/{union_target}"

    all_counts = {}

    for ds in cfg["datasets"]:
        fmt = ds["format"]
        src = f'{cfg["repo_workspace_path"]}/{ds["source"]}'
        opts = ds.get("options", {})

        reader = spark.read.format(fmt)
        for k,v in opts.items():
            reader = reader.option(k, v)

        df = reader.load(src) \
            .withColumn("_dataset", F.lit(ds["name"])) \
            .withColumn("_source_file", F.input_file_name()) \
            .withColumn("_ingest_ts", F.current_timestamp())

        df.write.mode("append").format("delta").save(target)
        all_counts[ds["name"]] = df.count()

    return all_counts
