"""
Generate sample migration "source" and "target" output tables as Parquet, with bugs planted on the target side.

    spark-submit generate_sample_data.py data        ->  data/source/<TABLE>, data/target/<TABLE>   (Parquet folders)
    generate_sample_data.py --tables main.parity_demo  ->  tables main.parity_demo_source.<TABLE>,
                                                          main.parity_demo_target.<TABLE>        (Databricks / Hive)

Tables
  FACT_SALES (200,000 rows)  big enough to trigger sampling in the demo config; incremental by batch id
  DIM_CUSTOMER (SCD2)        history bugs; incremental by update timestamp
  DIM_PRODUCT                schema differences + duplicate keys
  FACT_INVENTORY_SNAPSHOT    identical on both sides (named FACT_INV_SNAPSHOT on the target side) -> must PASS
"""
import datetime as dt
import sys
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = sys.argv[1:]
as_tables = bool(args) and args[0] == "--tables"
out = (args[1] if as_tables else args[0]) if args else "data"
spark = SparkSession.builder.appName("generate_sample_data").getOrCreate()
if as_tables:
    for side in ("source", "target"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {out}_{side}")


def save(df, side, table):
    if as_tables:     # catalog table: <out>_source.<TABLE> / <out>_target.<TABLE>
        name = f"{out}_{side}.{table}"
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    else:             # Parquet folder: <out>/<side>/<TABLE>
        name = f"{out}/{side}/{table}"
        df.coalesce(2).write.mode("overwrite").parquet(name)
    print(f"  wrote {name}: {spark.read.table(name).count() if as_tables else df.count():,} rows")


OLD_TS = F.lit("2026-09-01 00:00:00").cast("timestamp")
SRC_RUN = F.lit("2026-10-13 02:30:00").cast("timestamp")
TGT_RUN = F.lit("2026-10-13 04:30:00").cast("timestamp")

# ------------------------------------------------------------------ FACT_SALES
lines = spark.createDataFrame([(1,), (2,)], "ORDER_LINE_NBR INT")
base = (spark.range(1, 100_001).select(F.col("id").cast("int").alias("ORDER_ID"))
        .crossJoin(lines)
        .withColumn("NET_AMT", ((F.col("ORDER_ID") * 7 + F.col("ORDER_LINE_NBR") * 13) % 50000 / 100 + 1)
                    .cast("decimal(12,2)"))
        .withColumn("STATUS", F.element_at(F.array(F.lit("OPEN"), F.lit("CLOSED"), F.lit("SHIPPED")),
                                           (F.col("ORDER_ID") % 3 + 1).cast("int")))
        .withColumn("SALE_DT", F.date_add(F.lit("2026-01-01").cast("date"), (F.col("ORDER_ID") % 285).cast("int"))))
in_src_run = F.col("ORDER_ID") % 50 == 0
src_sales = (base
              .withColumn("SALES_SK", (F.col("ORDER_ID") * 10 + F.col("ORDER_LINE_NBR")).cast("long"))
              .withColumn("ETL_BATCH_ID", F.when(in_src_run, 101).otherwise(100))
              .withColumn("LOAD_TS", F.when(in_src_run, SRC_RUN).otherwise(OLD_TS)))

oid, line = F.col("ORDER_ID"), F.col("ORDER_LINE_NBR")
in_tgt_run = (in_src_run & (oid % 1000 != 0)) | (oid % 2500 == 25)     # BUG: missed 200 updates, rewrote 80 rows
tgt_sales = (base
                .where(~((oid % 9973 == 0) & (line == 1)))                       # BUG: 10 rows missing
                .withColumn("NET_AMT",
                            F.when((oid % 997 == 0) & (line == 2), F.col("NET_AMT") + 5)         # BUG: 100 wrong amounts
                             .when((oid % 101 == 0) & (line == 1), F.col("NET_AMT") + Decimal("0.01"))  # within tolerance
                             .otherwise(F.col("NET_AMT")).cast("decimal(12,2)"))
                .withColumn("STATUS", F.when(oid % 503 == 0, F.concat("STATUS", F.lit(" ")))  # trailing space: normalized
                                       .otherwise(F.col("STATUS")))
                .withColumn("SALES_SK", (oid * 10 + line + 5_000_000).cast("long"))          # different SKs (excluded)
                .withColumn("ETL_BATCH_ID", F.when(in_tgt_run, 9001).otherwise(9000))
                .withColumn("LOAD_TS", F.when(in_tgt_run, TGT_RUN).otherwise(OLD_TS)))
extra = spark.createDataFrame(
    [(100_000 + i, 1, Decimal("9.99"), "OPEN", dt.date(2026, 10, 1), 9_000_000 + i, 9000,
      dt.datetime(2026, 9, 1)) for i in range(1, 6)], tgt_sales.schema)          # BUG: 5 extra rows
tgt_sales = tgt_sales.unionByName(extra)

print("FACT_SALES")
save(src_sales, "source", "FACT_SALES")
save(tgt_sales, "target", "FACT_SALES")

# ------------------------------------------------------------------ DIM_CUSTOMER (SCD2)
HIGH = dt.date(9999, 12, 31)
CUST_SCHEMA = ("CUSTOMER_NBR INT, EFF_FROM_DT DATE, EFF_TO_DT DATE, CURRENT_FLG STRING, CUSTOMER_NAME STRING, "
               "SEGMENT STRING, CUSTOMER_SK LONG, UPDATE_TS TIMESTAMP")


def history(c, run_ts):
    """Correct SCD2 history for customer c: 1-3 versions, next EFF_FROM_DT = previous EFF_TO_DT + 1 day."""
    n = 1 + c % 3
    rows, start = [], dt.date(2025, 1, 1) + dt.timedelta(days=c % 30)
    for v in range(n):
        last = v == n - 1
        end = HIGH if last else start + dt.timedelta(days=89)
        touched = c % 10 == 0 and v >= n - 2              # this run expired the old version and added the new one
        rows.append([c, start, end, "Y" if last else "N", f"Customer {c} v{v + 1}", ["RETAIL", "SMB", "ENT"][v % 3],
                     c * 10 + v, run_ts if touched else dt.datetime(2026, 1, 1)])
        if not last:
            start = end + dt.timedelta(days=1)
    return rows


src_c, tgt_c = [], []
for c in range(1, 1001):
    src_c += [tuple(r) for r in history(c, dt.datetime(2026, 10, 13, 2, 30))]
    t = history(c, dt.datetime(2026, 10, 13, 4, 30))
    if c == 7:                       # BUG: old version never expired -> 2 current rows, overlap, current not latest
        t[0][2], t[0][3] = HIGH, "Y"
    if c == 13:                      # BUG: new version starts 5 days late -> gap (and a different key)
        t[1][1] = t[1][1] + dt.timedelta(days=5)
    if c == 22:                      # BUG: old version ends 10 days after the next starts -> overlap
        t[0][2] = t[1][1] + dt.timedelta(days=10)
    if c == 40:                      # BUG: this run's change never applied -> missing version, old one still current
        t = [t[0]]
        t[0][2], t[0][3], t[0][7] = HIGH, "Y", dt.datetime(2026, 1, 1)
    for r in t:
        r[6] += 5_000_000            # different SKs (excluded)
    tgt_c += [tuple(r) for r in t]

print("DIM_CUSTOMER")
save(spark.createDataFrame(src_c, CUST_SCHEMA), "source", "DIM_CUSTOMER")
save(spark.createDataFrame(tgt_c, CUST_SCHEMA), "target", "DIM_CUSTOMER")

# ------------------------------------------------------------------ DIM_PRODUCT (schema + duplicates)
prod = [(f"P{p:04d}", f"Product {p}", Decimal(f"{(p % 90) + 10}.99"), ["TOYS", "FOOD", "HOME"][p % 3])
        for p in range(1, 501)]
src_prod = spark.createDataFrame(prod, "PRODUCT_CODE STRING, PRODUCT_NAME STRING, LIST_PRICE DECIMAL(10,2), CATEGORY STRING")
tgt_prod = (src_prod.drop("CATEGORY")                                            # BUG: column missing
               .withColumn("LIST_PRICE", F.col("LIST_PRICE").cast("decimal(10,4)"))  # BUG: different scale
               .withColumn("BRAND", F.lit("ACME")))                                  # BUG: unexpected column
tgt_prod = tgt_prod.unionByName(tgt_prod.where(F.col("PRODUCT_CODE").isin("P0010", "P0020", "P0030")))  # BUG: 3 duplicated rows

print("DIM_PRODUCT")
save(src_prod, "source", "DIM_PRODUCT")
save(tgt_prod, "target", "DIM_PRODUCT")

# ------------------------------------------------------------------ FACT_INVENTORY_SNAPSHOT (clean)
inv = (spark.range(0, 5000)
       .select(F.date_add(F.lit("2026-10-01").cast("date"), (F.col("id") % 5).cast("int")).alias("SNAPSHOT_DT"),
               (F.col("id") % 50 + 1).cast("int").alias("STORE_ID"),
               (F.col("id") / 250 + 1).cast("int").alias("SKU_ID"),
               (F.col("id") % 37).cast("int").alias("ON_HAND_QTY"))
       .dropDuplicates(["SNAPSHOT_DT", "STORE_ID", "SKU_ID"]))
print("FACT_INVENTORY_SNAPSHOT")
save(inv.withColumn("ETL_BATCH_ID", F.lit(100)), "source", "FACT_INVENTORY_SNAPSHOT")
save(inv.withColumn("ETL_BATCH_ID", F.lit(9000)), "target", "FACT_INV_SNAPSHOT")
if not as_tables:      # never stop the shared session on Databricks
    spark.stop()
