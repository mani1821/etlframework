"""
etl_parity — parity validation for an ETL migration: source (existing ETL) vs target (new ETL) output tables.

Both ETLs land tables in Oracle (different envs/schemas). This module reads both
sides into Spark and runs all comparisons in Spark. Oracle is only used to:
  * serve metadata (ALL_TAB_COLUMNS) and a COUNT(*)
  * apply WHERE conditions (table filter, delta window, sampling) so only needed rows cross the wire

Connections are normally Oracle; type: spark reads Parquet tables instead (local testing).

Sampling is key-based (ORA_HASH / xxhash64 of the business key), never df.sample():
both sides must select the SAME keys or every row looks like a mismatch.

Checks
  full table : schema, row count, null/duplicate keys, full-row duplicates, missing/extra rows,
               value comparison, column profiles
  incremental: rows touched by one load run (delta) — same count, same keys, same values
  SCD2       : one current row per key, valid date ranges, no overlaps/gaps, current = latest,
               same number of versions per business key on both sides

Run:  spark-submit --jars ojdbc8.jar validator.py tables.yaml [--param NAME=VALUE ...] [--tables T1,T2]
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import string
import uuid
from dataclasses import dataclass, field
from functools import reduce
from typing import Optional

import yaml
from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

HASH_BUCKETS = 10_000
NULL_TOKEN = "<NULL>"
SEP = "\u0001"
DAY_SECONDS = 86_400


# --------------------------------------------------------------------------- config
@dataclass
class Connection:
    type: str = "oracle"                  # "oracle"; "spark" = Parquet folders; "table" = catalog tables (Databricks/Hive)
    schema: str = ""                      # oracle: schema (owner); spark: optional sub-folder under path;
                                          # table: "catalog.schema" or "schema" the tables live in
    url: Optional[str] = None             # oracle: JDBC URL
    user: Optional[str] = None            # oracle: database user
    password: Optional[str] = None
    password_env: Optional[str] = None
    fetchsize: int = 10_000
    path: Optional[str] = None            # spark: folder holding one Parquet folder per table

    def __post_init__(self):
        self.type = self.type.lower()
        if self.type == "oracle":
            missing = [f for f in ("url", "user", "schema") if not getattr(self, f)]
            if missing:
                raise ValueError(f"oracle connection needs {missing}")
        elif self.type == "spark":
            if not self.path:
                raise ValueError("spark connection needs path")
        elif self.type == "table":
            if not self.schema:
                raise ValueError("table connection needs schema (e.g. main.parity_demo_source)")
        else:
            raise ValueError(f"connection type must be 'oracle', 'spark' or 'table', got {self.type!r}")

    def secret(self) -> str:
        return self.password or os.environ[self.password_env]


OracleConn = Connection   # old name, kept for compatibility


@dataclass
class Incremental:
    source_delta: str             # condition: rows the source ETL run inserted/updated
    target_delta: str             # condition: rows the target ETL run inserted/updated
    delta_only: bool = False      # True -> skip the full-table checks, validate only the delta


@dataclass
class SCD2:
    business_key: list[str]                 # identifies the entity, e.g. [CUSTOMER_NBR]
    effective_from: str                     # version start column
    effective_to: str                       # version end column
    current_flag: Optional[str] = None      # e.g. CURRENT_FLG; if absent, "current" = open-ended row
    current_value: str = "Y"                # value of current_flag that means current
    open_end: Optional[str] = None          # e.g. "9999-12-31" if open rows use a high date instead of NULL
    gap_days: float = 0                     # expected next.effective_from - prev.effective_to, in days
    require_current: bool = True            # every business key must have exactly one current row

    def __post_init__(self):
        self.business_key = [c.upper() for c in self.business_key]
        self.effective_from = self.effective_from.upper()
        self.effective_to = self.effective_to.upper()
        self.current_flag = self.current_flag.upper() if self.current_flag else None
        self.current_value = str(self.current_value)


@dataclass
class TableSpec:
    name: str
    keys: list[str]                                    # business keys, never sequence-generated SKs
    exclude_cols: list[str] = field(default_factory=list)  # audit cols, batch ids, load timestamps, SKs
    filter: Optional[str] = None                       # e.g. "LOAD_DT = DATE '2026-10-07'" (both sides)
    num_tolerance: float = 0.0                         # absolute tolerance for numeric columns
    ts_truncate: Optional[str] = None                  # "second" -> ignore sub-second drift
    force_full: bool = False                           # never sample this table
    target_name: Optional[str] = None                  # if the table is named differently on the target side
    row_mismatch_tolerance_pct: Optional[float] = None  # overrides settings.row_mismatch_tolerance_pct
    incremental: Optional[Incremental] = None          # delta checks for one load run
    scd2: Optional[SCD2] = None                        # history checks for SCD Type 2 tables

    def __post_init__(self):
        self.name = self.name.upper()
        self.target_name = (self.target_name or self.name).upper()
        self.keys = [k.upper() for k in self.keys]
        self.exclude_cols = [c.upper() for c in self.exclude_cols]
        if isinstance(self.incremental, dict):
            self.incremental = Incremental(**self.incremental)
        if isinstance(self.scd2, dict):
            self.scd2 = SCD2(**self.scd2)
        if self.scd2:
            sc = self.scd2
            need = sc.business_key + [sc.effective_from]
            if [c for c in need if c not in self.keys]:
                raise ValueError(f"{self.name}: for an SCD2 table, keys must be the business key plus "
                                 f"effective_from, e.g. keys: {need}")
            used = need + [sc.effective_to] + ([sc.current_flag] if sc.current_flag else [])
            excluded = [c for c in used if c in self.exclude_cols]
            if excluded:
                raise ValueError(f"{self.name}: SCD2 column(s) cannot be in exclude_cols: {excluded}")


@dataclass
class Settings:
    sample_threshold_rows: int = 5_000_000             # above this -> sample
    target_sample_rows: int = 1_000_000                # approx rows per side in the sample
    read_partitions: int = 16
    full_key_check: bool = True                        # when sampling, still reconcile ALL keys (key cols only)
    count_tolerance_pct: float = 0.0
    row_mismatch_tolerance_pct: float = 0.0            # % of matched rows allowed to differ (value comparison)
    mismatch_sample_limit: int = 50
    results_path: str = "/data/etl_parity"


def fill_params(text: Optional[str], params: dict, where: str) -> Optional[str]:
    """Replace ${NAME} placeholders (run windows, batch ids) with values from --param or env vars."""
    if text is None:
        return None
    try:
        return string.Template(text).substitute(params)
    except KeyError as e:
        name = e.args[0]
        raise ValueError(f"{where} uses ${{{name}}} but no value was given: "
                         f"pass --param {name}=... or set environment variable {name}") from None


LEGACY_DELTA_KEYS = {"informatica_delta": "source_delta", "talend_delta": "target_delta"}


def build_spec(tcfg: dict, params: dict) -> TableSpec:
    tcfg = dict(tcfg)
    label = tcfg.get("name", "?")
    tcfg["filter"] = fill_params(tcfg.get("filter"), params, f"{label}.filter")
    if tcfg.get("incremental"):
        inc = dict(tcfg["incremental"])
        for old, new in LEGACY_DELTA_KEYS.items():          # accept the older key names
            if old in inc and new not in inc:
                inc[new] = inc.pop(old)
        for k in ("source_delta", "target_delta"):
            if not inc.get(k):
                raise ValueError(f"{label}.incremental needs {k}")
            inc[k] = fill_params(inc[k], params, f"{label}.incremental.{k}")
        tcfg["incremental"] = inc
    return TableSpec(**tcfg)


# --------------------------------------------------------------------------- oracle access
class OracleReader:
    def __init__(self, spark: SparkSession, conn: OracleConn):
        self.spark, self.conn = spark, conn

    def _props(self) -> dict:
        return {
            "user": self.conn.user,
            "password": self.conn.secret(),
            "driver": "oracle.jdbc.OracleDriver",
            "fetchsize": str(self.conn.fetchsize),
        }

    def fq(self, table: str) -> str:
        return f"{self.conn.schema}.{table}".upper()

    def query(self, sql: str) -> DataFrame:
        return (self.spark.read.format("jdbc").option("url", self.conn.url)
                .options(**self._props()).option("query", sql).load())

    def count(self, table: str, where: Optional[str]) -> int:
        sql = f"SELECT COUNT(*) AS CNT FROM {self.fq(table)}" + (f" WHERE {where}" if where else "")
        return int(self.query(sql).first()["CNT"])

    def columns_meta(self, table: str) -> list[Row]:
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{self.conn.schema.upper()}' AND TABLE_NAME = '{table.upper()}'
            ORDER BY COLUMN_ID"""
        rows = self.query(sql).collect()
        if not rows:
            raise ValueError(f"{self.fq(table)} not found or no access")
        return rows

    def load(self, table: str, cols: list[str], where: Optional[str], key_expr: str, n_parts: int) -> DataFrame:
        """
        Parallel read: each Spark task pulls one ORA_HASH slice of the key.
        `where` goes INSIDE the subquery, so it can use any column of the table, including
        ones not selected (e.g. a LOAD_TS delta filter on an excluded audit column).
        """
        inner = f" WHERE {where}" if where else ""
        src = f"(SELECT {', '.join(cols)} FROM {self.fq(table)}{inner}) t"
        preds = [f"MOD(ORA_HASH({key_expr}), {n_parts}) = {i}" for i in range(n_parts)]
        return self.spark.read.jdbc(self.conn.url, src, predicates=preds, properties=self._props())

    # SQL dialect helpers (the validator asks the reader, so Oracle and Spark can differ)
    def key_expr(self, keys: list[str], types: dict[str, str]) -> str:
        return key_expr(keys, types)

    def sample_condition(self, kexpr: str, buckets: int, total: int) -> str:
        return f"ORA_HASH({kexpr}, {total - 1}) < {buckets}"


class SparkTableReader:
    """
    Same interface as OracleReader, but reads Parquet tables with Spark: <path>/[<schema>/]<TABLE>.
    Used for local testing (and works for tables already in a data lake).
    Filters and delta conditions are Spark SQL expressions here, not Oracle SQL.
    """

    def __init__(self, spark: SparkSession, conn: Connection):
        self.spark, self.conn = spark, conn

    def _path(self, table: str) -> str:
        base = self.conn.path.rstrip("/")
        return f"{base}/{self.conn.schema}/{table}" if self.conn.schema else f"{base}/{table}"

    def _df(self, table: str) -> DataFrame:
        if self.conn.type == "table":                       # catalog table, e.g. main.parity_demo_source.FACT_SALES
            df = self.spark.table(f"{self.conn.schema}.{table}")
        else:
            df = self.spark.read.parquet(self._path(table))
        return df.toDF(*[c.upper() for c in df.columns])

    def count(self, table: str, where: Optional[str]) -> int:
        df = self._df(table)
        return (df.where(where) if where else df).count()

    def columns_meta(self, table: str) -> list[Row]:
        rows = []
        for i, f in enumerate(self._df(table).schema.fields, 1):
            dt = f.dataType
            prec, scale = (dt.precision, dt.scale) if isinstance(dt, T.DecimalType) else (None, None)
            rows.append(Row(COLUMN_NAME=f.name, DATA_TYPE=dt.simpleString().upper(), DATA_LENGTH=None,
                            DATA_PRECISION=prec, DATA_SCALE=scale, NULLABLE="Y" if f.nullable else "N",
                            COLUMN_ID=i))
        return rows

    def load(self, table: str, cols: list[str], where: Optional[str], key_expr: str, n_parts: int) -> DataFrame:
        df = self._df(table)
        if where:
            df = df.where(where)          # filter first, so it can use columns that are not selected
        return df.select(*cols)

    def key_expr(self, keys: list[str], types: dict[str, str]) -> str:
        return "concat_ws('|', " + ", ".join(f"coalesce(cast(`{k}` as string), '~')" for k in keys) + ")"

    def sample_condition(self, kexpr: str, buckets: int, total: int) -> str:
        return f"pmod(xxhash64({kexpr}), {total}) < {buckets}"


def make_reader(spark: SparkSession, conn: Connection):
    return OracleReader(spark, conn) if conn.type == "oracle" else SparkTableReader(spark, conn)


def key_expr(keys: list[str], types: dict[str, str]) -> str:
    """NLS-independent string form of the composite business key, identical on both sides."""
    parts = []
    for k in keys:
        t = types[k]
        if t == "DATE":
            e = f"TO_CHAR({k}, 'YYYYMMDDHH24MISS')"
        elif t.startswith("TIMESTAMP"):
            e = f"TO_CHAR({k}, 'YYYYMMDDHH24MISSFF6')"
        elif t in ("NUMBER", "FLOAT", "BINARY_DOUBLE", "BINARY_FLOAT"):
            e = f"TO_CHAR({k}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')"
        else:
            e = k
        parts.append(f"NVL({e}, '~')")
    return " || '|' || ".join(parts)


# --------------------------------------------------------------------------- spark-side helpers
def normalize(df: DataFrame, spec: TableSpec) -> DataFrame:
    """Neutralise representation differences that are not real data differences."""
    out = []
    for f in df.schema.fields:
        c = F.col(f.name)
        if isinstance(f.dataType, T.StringType):
            c = F.when(F.trim(c) == "", F.lit(None)).otherwise(F.trim(c))
        elif isinstance(f.dataType, T.DecimalType):
            c = c.cast(T.DecimalType(38, 10))           # NUMBER(10,2) vs NUMBER -> same scale
        elif isinstance(f.dataType, (T.DoubleType, T.FloatType)):
            c = F.round(c.cast("double"), 8)
        elif isinstance(f.dataType, T.TimestampType) and spec.ts_truncate:
            c = F.date_trunc(spec.ts_truncate, c)
        out.append(c.alias(f.name))
    return df.select(*out)


def row_hash(cols: list[str]):
    return F.sha2(F.concat_ws(SEP, *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in cols]), 256)


def is_numeric(dtype) -> bool:
    return isinstance(dtype, (T.DecimalType, T.DoubleType, T.FloatType, T.IntegerType, T.LongType))


RESULT_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("run_ts", T.TimestampType()),
    T.StructField("table_name", T.StringType()), T.StructField("check_name", T.StringType()),
    T.StructField("status", T.StringType()), T.StructField("src_value", T.StringType()),
    T.StructField("tgt_value", T.StringType()), T.StructField("detail", T.StringType()),
])


# --------------------------------------------------------------------------- validator
class ParityValidator:
    def __init__(self, spark: SparkSession, src: OracleReader, tgt: OracleReader, settings: Settings):
        self.spark, self.src, self.tgt, self.s = spark, src, tgt, settings
        self.run_id = uuid.uuid4().hex[:12]
        self.run_ts = dt.datetime.now()
        self.results: list[tuple] = []

    # ---- result bookkeeping
    def rec(self, table: str, check: str, ok: bool, src_val=None, tgt_val=None, detail: str = "", status=None):
        self.results.append((
            self.run_id, self.run_ts, table, check,
            status or ("PASS" if ok else "FAIL"),
            None if src_val is None else str(src_val),
            None if tgt_val is None else str(tgt_val),
            (detail or "")[:4000]))

    def dump(self, df: DataFrame, table: str, kind: str):
        (df.limit(self.s.mismatch_sample_limit)
           .withColumn("run_id", F.lit(self.run_id))
           .write.mode("overwrite")
           .parquet(f"{self.s.results_path}/mismatches/run_id={self.run_id}/table={table}/kind={kind}"))

    def _sample(self, spec: TableSpec, where: Optional[str], kexpr: str, n: int) -> tuple[Optional[str], bool]:
        """Add a key-hash sampling condition when n is above the threshold (same keys on both sides)."""
        if spec.force_full or n <= self.s.sample_threshold_rows:
            return where, False
        buckets = max(1, math.ceil(HASH_BUCKETS * self.s.target_sample_rows / n))
        samp = self.src.sample_condition(kexpr, buckets, HASH_BUCKETS)
        return (f"({where}) AND {samp}" if where else samp), True

    def _load_pair(self, spec: TableSpec, cols: list[str], src_where, tgt_where, kexpr: str):
        parts = self.s.read_partitions
        src_df = normalize(self.src.load(spec.name, cols, src_where, kexpr, parts), spec).cache()
        tgt_df = normalize(self.tgt.load(spec.target_name, cols, tgt_where, kexpr, parts), spec).cache()
        return src_df, tgt_df

    # ---- entry point
    def validate(self, spec: TableSpec):
        t = spec.name
        excl = set(spec.exclude_cols)

        # schema (from Oracle metadata — Spark type mapping hides length/precision)
        src_meta, tgt_meta = self.src.columns_meta(t), self.tgt.columns_meta(spec.target_name)
        self.check_schema(t, src_meta, tgt_meta, excl)
        types = {r.COLUMN_NAME: r.DATA_TYPE for r in src_meta}
        tgt_cols = {r.COLUMN_NAME for r in tgt_meta}
        cols = [r.COLUMN_NAME for r in src_meta if r.COLUMN_NAME not in excl and r.COLUMN_NAME in tgt_cols]
        missing_keys = [k for k in spec.keys if k not in cols]
        if missing_keys:
            raise ValueError(f"key columns missing/excluded: {missing_keys}")
        if spec.scd2:
            sc = spec.scd2
            absent = [c for c in [sc.effective_to] + ([sc.current_flag] if sc.current_flag else []) if c not in cols]
            if absent:
                raise ValueError(f"SCD2 column(s) not found on both sides: {absent}")
        non_keys = [c for c in cols if c not in spec.keys]
        kexpr = self.src.key_expr(spec.keys, types)
        # SCD2: sample whole histories (every version of a business key), never single versions
        samp_kexpr = self.src.key_expr(spec.scd2.business_key, types) if spec.scd2 else kexpr

        if not (spec.incremental and spec.incremental.delta_only):
            self.validate_full(spec, cols, non_keys, kexpr, samp_kexpr)
        if spec.incremental:
            self.validate_delta(spec, cols, non_keys, kexpr, samp_kexpr)

    # ---- full table
    def validate_full(self, spec: TableSpec, cols, non_keys, kexpr, samp_kexpr):
        t, keys, parts = spec.name, spec.keys, self.s.read_partitions

        n_src, n_tgt = self.src.count(t, spec.filter), self.tgt.count(spec.target_name, spec.filter)
        diff_pct = abs(n_src - n_tgt) / max(n_src, 1) * 100
        self.rec(t, "row_count", diff_pct <= self.s.count_tolerance_pct, n_src, n_tgt, f"diff_pct={diff_pct:.4f}")

        where, sampled = self._sample(spec, spec.filter, samp_kexpr, max(n_src, n_tgt))
        scope = "sample" if sampled else "full"
        if sampled:
            self.rec(t, "sampling", True, detail=f"rows={max(n_src, n_tgt)}, sampled by key hash", status="INFO")
            if self.s.full_key_check:          # keys only, full population: cheap and catches dropped rows
                self.reconcile_keys(t, keys,
                                    self.src.load(t, keys, spec.filter, kexpr, parts),
                                    self.tgt.load(spec.target_name, keys, spec.filter, kexpr, parts),
                                    "full_key_reconciliation")

        src_df, tgt_df = self._load_pair(spec, cols, where, where, kexpr)
        try:
            dups = self.check_keys(t, cols, keys, src_df, tgt_df, scope)
            if not (sampled and self.s.full_key_check):
                self.reconcile_keys(t, keys, src_df.select(*keys), tgt_df.select(*keys), f"key_reconciliation[{scope}]")
            self.compare_rows(t, spec, keys, non_keys, src_df, tgt_df, scope, dups)
            self.compare_profiles(t, spec, cols, src_df, tgt_df, scope)
            if spec.scd2:
                self.check_scd2(t, spec.scd2, src_df, tgt_df, scope)
        finally:
            src_df.unpersist(); tgt_df.unpersist()

    # ---- incremental: rows touched by one load run
    def validate_delta(self, spec: TableSpec, cols, non_keys, kexpr, samp_kexpr):
        t, keys, parts, inc = spec.name, spec.keys, self.s.read_partitions, spec.incremental
        sw, tw = inc.source_delta, inc.target_delta

        # 1. same number of rows touched
        n_s, n_t = self.src.count(t, sw), self.tgt.count(spec.target_name, tw)
        self.rec(t, "delta_row_count", n_s == n_t, n_s, n_t,
                 f"rows touched by this run | source: {sw} | target: {tw}")
        if n_s == 0 and n_t == 0:
            self.rec(t, "delta_not_empty", False, 0, 0,
                     "both deltas are empty: check the delta filters / run window", status="WARN")
            return

        # 2. same keys touched (all delta keys, key columns only)
        self.reconcile_keys(t, keys,
                            self.src.load(t, keys, sw, kexpr, parts),
                            self.tgt.load(spec.target_name, keys, tw, kexpr, parts),
                            "delta_keys_match", kind_prefix="delta_",
                            detail="src_value=touched by source only, tgt_value=touched by target only")

        # 3. same values in the touched rows (sampled only if the delta itself is huge)
        s_where, sampled = self._sample(spec, sw, samp_kexpr, max(n_s, n_t))
        t_where, _ = self._sample(spec, tw, samp_kexpr, max(n_s, n_t))
        scope = "delta_sample" if sampled else "delta"
        src_df, tgt_df = self._load_pair(spec, cols, s_where, t_where, kexpr)
        try:
            dups = {side: df.groupBy(*keys).count().where("count > 1").count()
                    for side, df in (("source", src_df), ("target", tgt_df))}
            self.rec(t, f"delta_duplicate_keys[{scope}]", not any(dups.values()),
                     dups["source"], dups["target"], "keys appearing more than once in one run's delta")
            self.compare_rows(t, spec, keys, non_keys, src_df, tgt_df, scope, dups)
        finally:
            src_df.unpersist(); tgt_df.unpersist()

    # ---- checks
    def check_schema(self, t, src_meta, tgt_meta, excl):
        s = {r.COLUMN_NAME: r for r in src_meta}
        g = {r.COLUMN_NAME: r for r in tgt_meta}
        only_src = sorted(set(s) - set(g) - excl)
        only_tgt = sorted(set(g) - set(s) - excl)
        self.rec(t, "schema_columns", not only_src and not only_tgt,
                 detail=f"only_in_src={only_src} only_in_tgt={only_tgt}")
        diffs = []
        for c in sorted((set(s) & set(g)) - excl):
            a, b = s[c], g[c]
            for attr in ("DATA_TYPE", "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE", "NULLABLE"):
                if getattr(a, attr) != getattr(b, attr):
                    diffs.append(f"{c}.{attr}: {getattr(a, attr)} vs {getattr(b, attr)}")
        self.rec(t, "schema_types", not diffs, detail="; ".join(diffs))

    def check_keys(self, t, cols, keys, src_df, tgt_df, scope) -> dict:
        dups = {}
        for side, df in (("src", src_df), ("tgt", tgt_df)):
            null_keys = df.where(reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])).count()
            dups[side] = df.groupBy(*keys).count().where("count > 1").count()
            self.rec(t, f"{side}_null_keys[{scope}]", null_keys == 0, detail=f"{null_keys} rows")
            self.rec(t, f"{side}_duplicate_keys[{scope}]", dups[side] == 0, detail=f"{dups[side]} keys")
            full_dups = df.groupBy(*cols).count().where("count > 1").count()
            self.rec(t, f"{side}_full_row_duplicates[{scope}]", full_dups == 0, detail=f"{full_dups}")
        return dups

    def reconcile_keys(self, t, keys, src_keys: DataFrame, tgt_keys: DataFrame, check: str,
                       kind_prefix: str = "", detail: str = "src_value=missing_in_target, tgt_value=extra_in_target"):
        src_keys, tgt_keys = src_keys.distinct(), tgt_keys.distinct()
        missing = src_keys.join(tgt_keys, keys, "left_anti")
        extra = tgt_keys.join(src_keys, keys, "left_anti")
        n_missing, n_extra = missing.count(), extra.count()
        self.rec(t, check, n_missing == 0 and n_extra == 0, n_missing, n_extra, detail)
        if n_missing:
            self.dump(missing, t, f"{kind_prefix}missing_in_target")
        if n_extra:
            self.dump(extra, t, f"{kind_prefix}extra_in_target")

    def compare_rows(self, t, spec, keys, non_keys, src_df, tgt_df, scope, dups: dict):
        if any(dups.values()):
            self.rec(t, f"row_hash_match[{scope}]", False, detail=f"skipped: duplicate keys {dups} would "
                     f"inflate mismatch counts; fix duplicates first", status="SKIP")
            return
        s = src_df.select(*keys, row_hash(non_keys).alias("_h_s"))
        g = tgt_df.select(*keys, row_hash(non_keys).alias("_h_t"))
        matched = s.join(g, keys, "inner")
        n_matched = matched.count()
        hash_diff = matched.where(F.col("_h_s") != F.col("_h_t")).select(*keys)
        n_hash_diff = hash_diff.count()
        if n_hash_diff == 0:
            self.rec(t, f"row_hash_match[{scope}]", True, n_matched, n_matched, "all matched rows identical")
            return

        # drill into differing rows column by column (applies numeric tolerance)
        sd = src_df.join(hash_diff, keys).select(*keys, *[F.col(c).alias(f"{c}__src") for c in non_keys])
        td = tgt_df.join(hash_diff, keys).select(*keys, *[F.col(c).alias(f"{c}__tgt") for c in non_keys])
        cmp = sd.join(td, keys)
        schema = {f.name: f.dataType for f in src_df.schema.fields}
        conds = {}
        for c in non_keys:
            a, b = F.col(f"{c}__src"), F.col(f"{c}__tgt")
            if spec.num_tolerance and is_numeric(schema[c]):
                conds[c] = (a.isNull() != b.isNull()) | (F.abs(a - b) > F.lit(spec.num_tolerance))
            else:
                conds[c] = ~a.eqNullSafe(b)
        conds = {c: F.coalesce(e, F.lit(False)) for c, e in conds.items()}
        cmp = cmp.withColumn("_any_diff", reduce(lambda x, y: x | y, conds.values()))
        agg = cmp.agg(F.sum(F.col("_any_diff").cast("int")).alias("_rows"),
                      *[F.sum(e.cast("int")).alias(c) for c, e in conds.items()]).first().asDict()
        n_real = agg.pop("_rows") or 0
        per_col = {c: v for c, v in agg.items() if v}

        # row-level tolerance: pass if the share of differing rows is small enough
        row_tol = self.row_tolerance(spec)
        pct = n_real / max(n_matched, 1) * 100
        ok = pct <= row_tol
        within = " (within row tolerance)" if ok and n_real else ""
        self.rec(t, f"row_hash_match[{scope}]", ok, n_matched - n_real, n_matched,
                 f"{n_real} rows differ beyond value tolerance = {pct:.4f}% of matched rows; "
                 f"row tolerance {row_tol}%{within} ({n_hash_diff} raw hash diffs)")
        for c, v in sorted(per_col.items(), key=lambda kv: -kv[1]):
            self.rec(t, f"column_diff:{c}[{scope}]", False, detail=f"{v} rows ({v / max(n_matched, 1):.4%})",
                     status="WARN" if ok else "FAIL")
        if n_real:
            self.dump(cmp.where("_any_diff").drop("_any_diff"), t, f"column_mismatch_{scope}")

    def row_tolerance(self, spec: TableSpec) -> float:
        """% of matched rows allowed to differ: the table's own setting, else the global default."""
        if spec.row_mismatch_tolerance_pct is not None:
            return spec.row_mismatch_tolerance_pct
        return self.s.row_mismatch_tolerance_pct

    def compare_profiles(self, t, spec, cols, src_df, tgt_df, scope):
        schema = {f.name: f.dataType for f in src_df.schema.fields}

        def exprs(c):
            col, dtype = F.col(c), schema[c]
            e = [F.sum(col.isNull().cast("int")).alias(f"{c}|nulls"),
                 F.approx_count_distinct(col).alias(f"{c}|approx_distinct")]
            if is_numeric(dtype):
                e += [F.min(col).alias(f"{c}|min"), F.max(col).alias(f"{c}|max"), F.sum(col).alias(f"{c}|sum")]
            elif isinstance(dtype, T.StringType):
                e += [F.max(F.length(col)).alias(f"{c}|max_len"), F.min(F.length(col)).alias(f"{c}|min_len")]
            elif isinstance(dtype, (T.TimestampType, T.DateType)):
                e += [F.min(col).alias(f"{c}|min"), F.max(col).alias(f"{c}|max")]
            return e

        all_exprs = [e for c in cols for e in exprs(c)] + [F.count(F.lit(1)).alias("_rows")]
        ps, pt = src_df.agg(*all_exprs).first().asDict(), tgt_df.agg(*all_exprs).first().asDict()
        n_rows = max(ps.pop("_rows"), pt.pop("_rows"), 1)
        tol = spec.num_tolerance
        bad = []
        for k in ps:
            a, b = ps[k], pt[k]
            if a == b:
                continue
            # numeric min/max may differ by num_tolerance; a sum by num_tolerance per row
            if tol and k.endswith(("|min", "|max", "|sum")) and a is not None and b is not None:
                allowed = tol * n_rows if k.endswith("|sum") else tol
                try:
                    if abs(float(a) - float(b)) <= allowed:
                        continue
                except (TypeError, ValueError):      # dates: compared exactly
                    pass
            bad.append(f"{k}: {a} vs {b}")
        # profiles are diagnostic: if the table allows some differing rows, report as WARN, not FAIL
        status = None if not bad or self.row_tolerance(spec) == 0 else "WARN"
        self.rec(t, f"column_profiles[{scope}]", not bad, detail="; ".join(bad), status=status)

    def check_scd2(self, t, sc: SCD2, src_df: DataFrame, tgt_df: DataFrame, scope: str):
        """History integrity on each side, then version counts compared across sides."""
        bk, frm, to = sc.business_key, sc.effective_from, sc.effective_to
        secs = lambda c: F.col(c).cast("timestamp").cast("long")
        is_open = F.col(to).isNull()
        if sc.open_end:
            is_open = is_open | (F.to_date(F.col(to)) == F.lit(sc.open_end).cast("date"))
        is_current = (F.coalesce(F.col(sc.current_flag).cast("string") == F.lit(sc.current_value), F.lit(False))
                      if sc.current_flag else is_open)
        expected = sc.gap_days * DAY_SECONDS
        w = Window.partitionBy(*bk).orderBy(frm)

        for side, df in (("source", src_df), ("target", tgt_df)):
            x = df.select(*bk, frm, to, is_current.alias("_cur"), is_open.alias("_open"),
                          F.lead(frm).over(w).alias("_next_from"))
            has_next = F.col("_next_from").isNotNull()
            step = secs("_next_from") - secs(to)              # seconds from this version's end to next start
            flags = x.select(
                (F.col(to).isNotNull() & ~F.col("_open") & (secs(frm) > secs(to))).alias("bad_range"),
                (has_next & (F.col(to).isNull() | (step < expected))).alias("overlap"),
                (has_next & F.col(to).isNotNull() & (step > expected)).alias("gap"),
                (F.col("_cur") & has_next).alias("cur_not_latest"))
            c = flags.agg(*[F.sum(F.col(n).cast("int")).alias(n) for n in flags.columns]).first().asDict()
            c = {k: v or 0 for k, v in c.items()}
            cur = (df.groupBy(*bk).agg(F.sum(is_current.cast("int")).alias("n"))
                     .agg(F.sum((F.col("n") > 1).cast("int")).alias("multi"),
                          F.sum((F.col("n") == 0).cast("int")).alias("none")).first())
            multi, none = cur["multi"] or 0, cur["none"] or 0

            self.rec(t, f"scd2_one_current_row[{side}][{scope}]", multi == 0 and (none == 0 or not sc.require_current),
                     multi, none, "src_value=keys with >1 current row, tgt_value=keys with no current row")
            self.rec(t, f"scd2_valid_date_ranges[{side}][{scope}]", c["bad_range"] == 0,
                     detail=f"{c['bad_range']} versions with {frm} after {to}")
            self.rec(t, f"scd2_no_overlaps[{side}][{scope}]", c["overlap"] == 0,
                     detail=f"{c['overlap']} versions overlapping the next version (expected gap {sc.gap_days} day(s))")
            self.rec(t, f"scd2_no_gaps[{side}][{scope}]", c["gap"] == 0,
                     detail=f"{c['gap']} versions with a gap before the next version (expected gap {sc.gap_days} day(s))")
            self.rec(t, f"scd2_current_is_latest[{side}][{scope}]", c["cur_not_latest"] == 0,
                     detail=f"{c['cur_not_latest']} current rows that have a later version")

        # same number of versions per business key on both sides
        vs = src_df.groupBy(*bk).agg(F.count(F.lit(1)).alias("versions_source"))
        vt = tgt_df.groupBy(*bk).agg(F.count(F.lit(1)).alias("versions_target"))
        d = vs.join(vt, bk, "full_outer").where(
            F.coalesce("versions_source", F.lit(0)) != F.coalesce("versions_target", F.lit(0)))
        n = d.count()
        self.rec(t, f"scd2_version_counts_match[{scope}]", n == 0, detail=f"{n} business keys with a different number of versions")
        if n:
            self.dump(d, t, "scd2_version_count")

    # ---- persistence
    def save(self) -> DataFrame:
        df = self.spark.createDataFrame(self.results, RESULT_SCHEMA)
        df.write.mode("append").partitionBy("run_id").parquet(f"{self.s.results_path}/results")
        return df


# --------------------------------------------------------------------------- runner
def main(argv=None):
    ap = argparse.ArgumentParser(description="Source vs target parity validation for an ETL migration")
    ap.add_argument("config", help="path to tables.yaml")
    ap.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                    help="value for ${NAME} placeholders in filters, e.g. the run window (repeatable)")
    ap.add_argument("--tables", help="comma-separated subset of tables to validate")
    a = ap.parse_args(argv)

    params = dict(os.environ)
    for p in a.param:
        k, sep, v = p.partition("=")
        if not sep:
            ap.error(f"--param must be NAME=VALUE, got {p!r}")
        params[k] = v
    only = {x.strip().upper() for x in a.tables.split(",")} if a.tables else None

    with open(a.config) as fh:
        cfg = yaml.safe_load(fh)
    spark = SparkSession.builder.appName("etl_parity").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:          # not available on Databricks shared / serverless compute
        pass
    settings = Settings(**(cfg.get("settings") or {}))
    src_cfg, tgt_cfg = cfg.get("source") or cfg.get("informatica"), cfg.get("target") or cfg.get("talend")
    if not (src_cfg and tgt_cfg):
        raise SystemExit("config needs a 'source:' and a 'target:' connection")
    src_conn, tgt_conn = Connection(**src_cfg), Connection(**tgt_cfg)
    if src_conn.type != tgt_conn.type:   # both sides must hash keys the same way for sampling
        raise SystemExit("source and target connections must have the same type")
    v = ParityValidator(spark, make_reader(spark, src_conn), make_reader(spark, tgt_conn), settings)
    for tcfg in cfg["tables"]:
        name = str(tcfg.get("name", "?")).upper()
        if only and name not in only:
            continue
        try:
            v.validate(build_spec(tcfg, params))
        except Exception as e:  # one bad table must not kill the run
            v.rec(name, "run", False, detail=f"{type(e).__name__}: {e}", status="ERROR")
    res = v.save()
    res.groupBy("table_name").agg(
        *[F.sum((F.col("status") == s).cast("int")).alias(s.lower()) for s in ("PASS", "FAIL", "ERROR", "SKIP", "WARN")]
    ).orderBy("table_name").show(truncate=False)
    failed = res.where(F.col("status").isin("FAIL", "ERROR")).count()
    raise SystemExit(1 if failed else 0)   # non-zero exit fails the CI / scheduler step


if __name__ == "__main__":
    main()
