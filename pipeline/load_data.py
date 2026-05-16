from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from utils import canonicalize_columns, find_label_column, normalize_attack_label


CSV_GLOBS = ("*.csv")

def log(msg: str) -> None:
    print(f"[load_data] {msg}", flush=True)

@dataclass
class DetectionSplits:
    train_all: pd.DataFrame
    val_all: pd.DataFrame
    test_all: pd.DataFrame
    train_benign: pd.DataFrame
    val_benign: pd.DataFrame


def _iter_csv_paths(data_dir):
    data_dir = Path(data_dir)

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
    if not data_dir.is_dir():
        raise NotADirectoryError(f"Expected a directory, got: {data_dir}")

    seen = set()

    for path in sorted(data_dir.rglob("*")):
        # เอาเฉพาะไฟล์จริง
        if not path.is_file():
            continue

        # เอาเฉพาะ .csv
        if path.suffix.lower() != ".csv":
            continue

        key = str(path.resolve()).lower()
        if key in seen:
            continue

        seen.add(key)
        yield path


def read_cic_ids2017_folder(data_dir):
    data_dir = Path(data_dir)
    paths = list(_iter_csv_paths(data_dir))
    if not paths:
        raise FileNotFoundError(f"No CSV files found under: {data_dir}")
    for i, p in enumerate(paths, start=1):
        log(f"[{i}] csv file = {p}")
    log(f"found {len(paths)} CSV files under: {data_dir}")
    frames: list[pd.DataFrame] = []
    for path in paths:
        log(f"reading: {path}")
        try:
            df = pd.read_csv(path, low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, low_memory=False, encoding="latin-1")
        df["source_file"] = path.name
        frames.append(df)

    merged = pd.concat(frames, axis=0, ignore_index=True)
    merged = canonicalize_columns(merged)
    merged["row_id"] = np.arange(len(merged), dtype=np.int64)
    log(f"merged rows={len(merged):,}, cols={merged.shape[1]:,}")
    return merged


def read_lycos_csv(csv_path) -> pd.DataFrame:
    """Read the single-file Lycos2017 corpus and return a DataFrame in the
    same shape downstream cascade scripts expect.

    Lycos2017 (Rosay et al., IEEE WI-IAT 2021) is the corrected variant of
    CIC-IDS2017: same source pcaps but the LycoSTand feature extractor
    fixes the CICFlowMeter bugs and the labels are re-aligned. The corpus
    ships as one big CSV with 83 columns:
        flow_id, src_addr, src_port, dst_addr, dst_port, ip_prot, timestamp,
        75 numeric flow features (flow_duration, pkt_per_s, fwd_*, bwd_*,
        iat_*, active/idle_*, flag_*, fwd_bulk_*, bwd_bulk_*, fwd_subflow_*,
        bwd_subflow_*, fwd_tcp_init_win_bytes, bwd_tcp_init_win_bytes),
        label.

    The timestamp column is microseconds since Unix epoch. We convert it to
    a pandas Timestamp so that ``_time_series_for_df`` downstream works,
    and we synthesise a ``source_file`` column from the day-of-week so that
    ``split_strategy='temporal_by_file'`` keeps the same per-day chronology
    used for the CIC-IDS2017 evaluation.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Lycos2017 CSV not found at: {csv_path}")

    log(f"reading Lycos2017 single-CSV: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    log(f"raw rows={len(df):,}, cols={df.shape[1]}")

    if "timestamp" not in df.columns:
        raise ValueError("Lycos2017 CSV must contain a 'timestamp' column.")

    ts_us = pd.to_numeric(df["timestamp"], errors="coerce")
    ts_dt = pd.to_datetime(ts_us, unit="us", errors="coerce")
    df["timestamp"] = ts_dt

    day_name = ts_dt.dt.day_name().fillna("Unknown")
    df["source_file"] = day_name + "-WorkingHours.lycos.csv"
    day_counts = df["source_file"].value_counts().to_dict()
    log(f"derived source_file from timestamp day-of-week: {day_counts}")

    df = canonicalize_columns(df)
    df["row_id"] = np.arange(len(df), dtype=np.int64)
    log(f"loaded rows={len(df):,}, cols={df.shape[1]}")
    return df




# UNSW-NB15 column ordering (Moustafa & Slay 2015) — 49 columns, no header in CSVs.
UNSW_NB15_COLUMNS = [
    "srcip", "sport", "dstip", "dsport", "proto", "state", "dur",
    "sbytes", "dbytes", "sttl", "dttl", "sloss", "dloss", "service",
    "Sload", "Dload", "Spkts", "Dpkts", "swin", "dwin", "stcpb",
    "dtcpb", "smeansz", "dmeansz", "trans_depth", "res_bdy_len",
    "Sjit", "Djit", "Stime", "Ltime", "Sintpkt", "Dintpkt", "tcprtt",
    "synack", "ackdat", "is_sm_ips_ports", "ct_state_ttl",
    "ct_flw_http_mthd", "is_ftp_login", "ct_ftp_cmd", "ct_srv_src",
    "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm", "ct_src_dport_ltm",
    "ct_dst_sport_ltm", "ct_dst_src_ltm",
    "attack_cat", "Label",
]


def read_unsw_nb15_folder(data_dir) -> pd.DataFrame:
    """Read the four UNSW-NB15 CSV files (no header in the official release)
    and return a DataFrame in the shape downstream cascade scripts expect.

    UNSW-NB15 (Moustafa & Slay, 2015) is an independent NIDS benchmark
    generated on the UNSW Cyber Range using the IXIA PerfectStorm tool. It
    contains 9 attack families (Generic, Exploits, Fuzzers, DoS,
    Reconnaissance, Analysis, Backdoor, Shellcode, Worms) plus benign
    traffic, across ~2.5M flows. The CSV release has no header; this
    function assigns the canonical 49-column schema.

    The ``attack_cat`` column carries the multi-class attack name (NaN
    for benign rows). We promote it to a ``label`` column so that
    ``clean_detection_dataframe`` produces ``multiclass_label`` and
    ``binary_label`` the same way it does for CIC-IDS2017 and Lycos2017.

    Each input file's basename is used as the ``source_file`` so that
    ``split_strategy='temporal_by_file'`` treats each release chunk as a
    bucket (matching the multi-day CIC-IDS2017 evaluation pattern).
    """
    data_dir = Path(data_dir)
    files = sorted(data_dir.glob("UNSW-NB15_*.csv"))
    if not files:
        raise FileNotFoundError(f"No UNSW-NB15_*.csv files in {data_dir}")
    log(f"reading {len(files)} UNSW-NB15 CSVs from {data_dir}")

    frames: list[pd.DataFrame] = []
    for f in files:
        try:
            df = pd.read_csv(f, header=None, low_memory=False, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(f, header=None, low_memory=False, encoding="latin-1")
        if df.shape[1] != len(UNSW_NB15_COLUMNS):
            raise ValueError(
                f"{f.name}: expected {len(UNSW_NB15_COLUMNS)} columns, got "
                f"{df.shape[1]}"
            )
        df.columns = UNSW_NB15_COLUMNS
        df["source_file"] = f.name
        frames.append(df)
        log(f"  {f.name}: {len(df):,} rows")

    merged = pd.concat(frames, axis=0, ignore_index=True)

    # UNSW-NB15 has well-documented mixed-type quirks: some columns
    # (notably ``sport``, ``dsport``, ``ct_ftp_cmd``) store integers as
    # plain ints in some chunks and as strings in others, which crashes
    # sklearn's OneHotEncoder downstream with
    # "TypeError: Encoders require input ... uniformly strings or numbers".
    # We force every column that is *supposed* to be numeric per the
    # official UNSW spec to ``pd.to_numeric(errors='coerce')`` so the
    # cascade preprocessor sees clean numeric dtypes.
    UNSW_NUMERIC_COLUMNS = [
        "sport", "dsport", "dur", "sbytes", "dbytes", "sttl", "dttl",
        "sloss", "dloss", "Sload", "Dload", "Spkts", "Dpkts", "swin",
        "dwin", "stcpb", "dtcpb", "smeansz", "dmeansz", "trans_depth",
        "res_bdy_len", "Sjit", "Djit", "Stime", "Ltime", "Sintpkt",
        "Dintpkt", "tcprtt", "synack", "ackdat", "is_sm_ips_ports",
        "ct_state_ttl", "ct_flw_http_mthd", "is_ftp_login", "ct_ftp_cmd",
        "ct_srv_src", "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm",
        "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm",
    ]
    for c in UNSW_NUMERIC_COLUMNS:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    # Promote attack_cat to the standard `label` column (NaN -> "benign").
    # clean_detection_dataframe will rename it to multiclass_label and
    # derive binary_label from "BENIGN" vs other.
    merged["label"] = (
        merged["attack_cat"]
        .fillna("benign")
        .astype(str)
        .str.strip()
    )
    merged = merged.drop(columns=["attack_cat", "Label"])

    merged = canonicalize_columns(merged)
    merged["row_id"] = np.arange(len(merged), dtype=np.int64)
    log(f"merged UNSW-NB15: {len(merged):,} rows, {merged.shape[1]} cols")
    return merged


def clean_detection_dataframe(
    df: pd.DataFrame,
    max_missing_fraction: float = 0.30,
    drop_unknown_labels: bool = True,
) -> pd.DataFrame:
    label_col = find_label_column(df)
    df = df.copy()
    log(f"raw rows before clean={len(df):,}")
    df[label_col] = df[label_col].map(normalize_attack_label)
    df = df.rename(columns={label_col: "multiclass_label"})
    if drop_unknown_labels:
        df = df.loc[df["multiclass_label"] != "UNKNOWN"].copy()
    df["binary_label"] = (df["multiclass_label"] != "BENIGN").astype(int)

    df = df.replace([np.inf, -np.inf], np.nan)

    missing_fraction = df.isna().mean(axis=1)
    df = df.loc[missing_fraction <= max_missing_fraction].copy()
    log(f"rows after missing filter={len(df):,}")

    all_missing = [col for col in df.columns if df[col].isna().all()]
    if all_missing:
        df = df.drop(columns=all_missing)

    if "row_id" in df.columns:
        dedup_subset = [c for c in df.columns if c != "row_id"]
    else:
        dedup_subset = None
    df = df.drop_duplicates(subset=dedup_subset).reset_index(drop=True)
    log(
        f"rows after dedup={len(df):,} | "
        f"benign={(df['binary_label'] == 0).sum():,} | "
        f"attack={(df['binary_label'] == 1).sum():,}"
    )
    return df


def _sort_for_sequences(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce")
        return df.assign(_ts_sort=ts).sort_values(["_ts_sort", "row_id"], na_position="last").drop(columns=["_ts_sort"])
    return df.sort_values("row_id")


def _time_series_for_df(df: pd.DataFrame) -> pd.Series:
    """Return a sortable per-row time key for temporal splits.

    Falls back to row_id (or enumerate) when no timestamp column is available,
    so splits remain deterministic but still honor ingestion order.
    """
    for col in ("timestamp", "Timestamp"):
        if col in df.columns:
            ts = pd.to_datetime(df[col], errors="coerce")
            if ts.notna().any():
                return ts
    if "row_id" in df.columns:
        return pd.Series(df["row_id"].to_numpy(), index=df.index)
    return pd.Series(np.arange(len(df)), index=df.index)


def _time_split_positions(
    n: int,
    test_size: float,
    val_size_from_train: float,
    time_rank: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Split n rows ordered by time_rank into (train, val, test) by chronology.

    Proportions mirror the existing random splitter:
        test  = last test_size fraction
        val   = last val_size_from_train fraction of the remaining head
        train = everything before val
    """
    n_test = int(round(n * test_size))
    n_remain = n - n_test
    n_val = int(round(n_remain * val_size_from_train))
    n_train = n_remain - n_val
    order = np.argsort(time_rank, kind="stable")
    train_pos = order[:n_train]
    val_pos = order[n_train : n_train + n_val]
    test_pos = order[n_train + n_val :]
    return train_pos, val_pos, test_pos


def split_detection_data(
    df: pd.DataFrame,
    test_size: float = 0.20,
    val_size_from_train: float = 0.20,
    random_state: int = 42,
    split_strategy: str = "random",
) -> DetectionSplits:
    """Split into train/val/test.

    split_strategy:
      * "random"           - stratified random split (original behaviour).
      * "temporal"         - single global chronological split; train has the
                             earliest rows, test has the latest.
      * "temporal_by_file" - within each source_file / day, take first 64%
                             rows as train, next 16% as val, last 20% as test.
                             Preserves within-day order while keeping each
                             attack class present in every split. Recommended
                             for CIC-IDS2017 evaluations.
    """
    strategy = split_strategy.lower()

    if strategy == "random":
        train_df, test_df = train_test_split(
            df,
            test_size=test_size,
            stratify=df["binary_label"],
            random_state=random_state,
        )
        train_df, val_df = train_test_split(
            train_df,
            test_size=val_size_from_train,
            stratify=train_df["binary_label"],
            random_state=random_state,
        )

    elif strategy == "temporal":
        ts = _time_series_for_df(df)
        time_rank = ts.rank(method="first", na_option="bottom").to_numpy()
        tr_pos, va_pos, te_pos = _time_split_positions(
            len(df), test_size, val_size_from_train, time_rank
        )
        train_df = df.iloc[tr_pos]
        val_df = df.iloc[va_pos]
        test_df = df.iloc[te_pos]

    elif strategy == "temporal_by_file":
        if "source_file" not in df.columns:
            raise ValueError(
                "split_strategy='temporal_by_file' requires a 'source_file' column."
            )
        ts_full = _time_series_for_df(df)
        tr_parts: list[pd.DataFrame] = []
        va_parts: list[pd.DataFrame] = []
        te_parts: list[pd.DataFrame] = []
        for fname, sub in df.groupby("source_file", sort=False):
            sub_ts = ts_full.loc[sub.index]
            time_rank = sub_ts.rank(method="first", na_option="bottom").to_numpy()
            tr_pos, va_pos, te_pos = _time_split_positions(
                len(sub), test_size, val_size_from_train, time_rank
            )
            tr_parts.append(sub.iloc[tr_pos])
            va_parts.append(sub.iloc[va_pos])
            te_parts.append(sub.iloc[te_pos])
        train_df = pd.concat(tr_parts, axis=0) if tr_parts else df.iloc[0:0]
        val_df = pd.concat(va_parts, axis=0) if va_parts else df.iloc[0:0]
        test_df = pd.concat(te_parts, axis=0) if te_parts else df.iloc[0:0]

    else:
        raise ValueError(
            f"unknown split_strategy={split_strategy!r}; "
            "expected 'random', 'temporal', or 'temporal_by_file'."
        )

    train_df = train_df.reset_index(drop=True)
    val_df = val_df.reset_index(drop=True)
    test_df = test_df.reset_index(drop=True)
    log(
        f"split sizes [{strategy}] | train={len(train_df):,}, "
        f"val={len(val_df):,}, test={len(test_df):,}"
    )

    train_benign = train_df.loc[train_df["binary_label"] == 0].copy()
    val_benign = val_df.loc[val_df["binary_label"] == 0].copy()

    train_benign = _sort_for_sequences(train_benign).reset_index(drop=True)
    val_benign = _sort_for_sequences(val_benign).reset_index(drop=True)

    return DetectionSplits(
        train_all=train_df,
        val_all=val_df,
        test_all=test_df,
        train_benign=train_benign,
        val_benign=val_benign,
    )


def load_and_prepare_detection_data(
    data_dir: str | Path,
    max_missing_fraction: float = 0.30,
    test_size: float = 0.20,
    val_size_from_train: float = 0.20,
    random_state: int = 42,
    drop_unknown_labels: bool = True,
    split_strategy: str = "random",
) -> tuple[pd.DataFrame, DetectionSplits]:
    # Auto-detect dataset format:
    #   - lycos.csv in data_dir  -> Lycos2017 loader (single-file corrected CIC)
    #   - UNSW-NB15_*.csv files  -> UNSW-NB15 loader (4 chunks, no header)
    #   - otherwise              -> CIC-IDS2017 multi-file loader
    data_dir_path = Path(data_dir)
    lycos_csv = data_dir_path / "lycos.csv"
    unsw_files = sorted(data_dir_path.glob("UNSW-NB15_*.csv"))
    if lycos_csv.exists():
        log(f"detected Lycos2017 corpus at: {lycos_csv}")
        raw = read_lycos_csv(lycos_csv)
    elif unsw_files:
        log(f"detected UNSW-NB15 corpus ({len(unsw_files)} files) under {data_dir_path}")
        raw = read_unsw_nb15_folder(data_dir_path)
    else:
        raw = read_cic_ids2017_folder(data_dir)
    cleaned = clean_detection_dataframe(
        raw,
        max_missing_fraction=max_missing_fraction,
        drop_unknown_labels=drop_unknown_labels,
    )
    splits = split_detection_data(
        cleaned,
        test_size=test_size,
        val_size_from_train=val_size_from_train,
        random_state=random_state,
        split_strategy=split_strategy,
    )
    return cleaned, splits
