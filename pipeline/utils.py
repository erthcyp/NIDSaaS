from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


CANONICAL_ALIASES = {
    "label": ["label", " Label", "class", "Class"],
    "timestamp": ["timestamp", " Timestamp", "time", "Time"],
    "source_ip": ["source ip", "src ip", "source_ip", "Src IP"],
    "destination_ip": ["destination ip", "dst ip", "destination_ip", "Dst IP"],
    "source_port": ["source port", "src port", "source_port", "Src Port"],
    "destination_port": ["destination port", "dst port", "destination_port", "Dst Port"],
    "flow_duration": ["flow duration", "Flow Duration"],
    "flow_packets_s": ["flow packets/s", "Flow Packets/s"],
    "flow_bytes_s": ["flow bytes/s", "Flow Bytes/s"],
    "total_fwd_packets": ["total fwd packets", "Tot Fwd Pkts", "Total Fwd Packets"],
    "total_backward_packets": ["total backward packets", "Tot Bwd Pkts", "Total Backward Packets", "Total Bwd packets"],
    "syn_flag_count": ["syn flag count", "SYN Flag Count"],
    "rst_flag_count": ["rst flag count", "RST Flag Count"],
    "flow_id": ["flow id", "Flow ID"],
    "simillarhttp": ["simillarhttp", "SimillarHTTP"],
    "source_file": ["source_file"],
}


ATTACK_NORMALIZATION = {
    "benign": "BENIGN",
    "normal": "BENIGN",
    "web attack – xss": "Web Attack - XSS",
    "web attack - xss": "Web Attack - XSS",
    "web attack – sql injection": "Web Attack - Sql Injection",
    "web attack - sql injection": "Web Attack - Sql Injection",
    "web attack – brute force": "Web Attack - Brute Force",
    "web attack - brute force": "Web Attack - Brute Force",
    "ftp-patator": "FTP-Patator",
    "ssh-patator": "SSH-Patator",
    "heartbleed": "Heartbleed",
    "infiltration": "Infiltration",
    "portscan": "PortScan",
    "bot": "Bot",
    "ddos": "DDoS",
    "dos hulk": "DoS Hulk",
    "dos goldeneye": "DoS GoldenEye",
    "dos slowhttptest": "DoS Slowhttptest",
    "dos slowloris": "DoS slowloris",
    # Lycos2017 (Rosay et al. 2021 corrected CIC-IDS2017) — snake_case labels
    "ftp_patator": "FTP-Patator",
    "ssh_patator": "SSH-Patator",
    "dos_hulk": "DoS Hulk",
    "dos_goldeneye": "DoS GoldenEye",
    "dos_slowhttptest": "DoS Slowhttptest",
    "dos_slowloris": "DoS slowloris",
    "botnet": "Bot",
    "web_attack_brute_force": "Web Attack - Brute Force",
    "web_attack_xss": "Web Attack - XSS",
    "web_attack_sql_injection": "Web Attack - Sql Injection",
    "web_attack___brute_force": "Web Attack - Brute Force",
    "web_attack___xss": "Web Attack - XSS",
    "web_attack___sql_injection": "Web Attack - Sql Injection",
    "webattack_bruteforce": "Web Attack - Brute Force",
    "webattack_xss": "Web Attack - XSS",
    "webattack_sql_injection": "Web Attack - Sql Injection",
    # UNSW-NB15 (Moustafa & Slay 2015) — IXIA PerfectStorm attack families
    "generic": "Generic",
    "exploits": "Exploits",
    "fuzzers": "Fuzzers",
    "dos": "DoS",
    "reconnaissance": "Reconnaissance",
    "analysis": "Analysis",
    "backdoor": "Backdoor",
    "backdoors": "Backdoor",
    "shellcode": "Shellcode",
    "worms": "Worms",
}


PAPER_CLASS_ORDER = [
    "Web Attack - XSS",
    "Web Attack - Brute Force",
    "Bot",
    "DoS Slowhttptest",
    "DoS slowloris",
    "SSH-Patator",
    "FTP-Patator",
    "DoS GoldenEye",
    "DDoS",
    "PortScan",
    "DoS Hulk",
    "BENIGN",
]


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def canonicalize_column_name(name: str) -> str:
    clean = name.strip()
    lowered = clean.lower()
    for canonical, aliases in CANONICAL_ALIASES.items():
        if lowered in {a.strip().lower() for a in aliases}:
            return canonical
    return clean


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {col: canonicalize_column_name(col) for col in df.columns}
    return df.rename(columns=renamed)


def canonicalize_column_list(columns: Iterable[str]) -> list[str]:
    return [canonicalize_column_name(c) for c in columns]


def find_label_column(df: pd.DataFrame) -> str:
    for candidate in ("label", "Label", " Label", "class", "Class"):
        if candidate in df.columns:
            return candidate
    raise KeyError("Could not find a label column. Expected one of: label, Label, ' Label', class.")


def normalize_attack_label(value: object) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    text = str(value).strip()
    key = text.lower()
    return ATTACK_NORMALIZATION.get(key, text)


def ensure_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def make_dir(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(data: dict, path: str | Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def infer_numeric_and_categorical(df: pd.DataFrame, feature_columns: Iterable[str]) -> tuple[list[str], list[str]]:
    numeric_cols: list[str] = []
    categorical_cols: list[str] = []
    for col in feature_columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric_cols.append(col)
        else:
            categorical_cols.append(col)
    return numeric_cols, categorical_cols

def align_prediction_to_rows(n_rows: int, seq_len: int, seq_scores: np.ndarray) -> np.ndarray:
    if n_rows == 0:
        return np.zeros(0, dtype=float)
    if seq_scores.size == 0:
        return np.zeros(n_rows, dtype=float)
    out = np.zeros(n_rows, dtype=float)
    counts = np.zeros(n_rows, dtype=np.int64)
    for i, s in enumerate(seq_scores):
        end = min(i + seq_len, n_rows)
        out[i:end] += float(s)
        counts[i:end] += 1
    counts = np.where(counts == 0, 1, counts)
    return out / counts

def set_random_seed(seed: int) -> None:
    """Set numpy + python random seeds. PyTorch seed is set if torch is
    available (no hard dependency)."""
    import random as _py_random
    _py_random.seed(int(seed))
    np.random.seed(int(seed))
    try:
        import torch as _torch  # type: ignore
        _torch.manual_seed(int(seed))
        if _torch.cuda.is_available():
            _torch.cuda.manual_seed_all(int(seed))
    except ImportError:
        pass

