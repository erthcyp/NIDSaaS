"""CLOSR/CLAD baseline runner aligned with our locked CIC-IDS2017 split.

Trains the CLAD encoder (Wilkie et al., IEEE TNSM 2026) on our temporal_by_file
64/16/20 split, scores val + test rows by cosine similarity to a benign
centroid, and exports prediction CSVs in the same column shape as
hybrid_cascade so `proposed_method_valcal.py` can apply the identical
val-accuracy-calibrated tau* protocol.

Usage:
    python3 closr_baseline_valcal.py \
        --data-dir ../csv_CIC_IDS2017 \
        --closr-repo ../CLOSR \
        --out-dir outputs_closr_baseline \
        --split-strategy temporal_by_file \
        --seed 42
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import torch as T
import torch.nn.functional as F
from sklearn.preprocessing import StandardScaler

from load_data import load_and_prepare_detection_data
from utils import set_random_seed, write_json


def log(msg: str) -> None:
    print(f"[closr_baseline_valcal] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Feature handling
# ---------------------------------------------------------------------------
NON_FEATURE_COLS = {
    "binary_label",
    "multiclass_label",
    "row_id",
    "source_file",
    "_ts_sort",
    # canonicalized identity / metadata columns produced by load_data
    "flow_id",
    "src_ip",
    "src_port",
    "dst_ip",
    "dst_port",
    "protocol",
    "ip_prot",
    "timestamp",
    "label",
}


def select_feature_columns(df: pd.DataFrame) -> List[str]:
    """Pick numeric columns, drop label / metadata."""
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    keep = [c for c in numeric if c.lower() not in NON_FEATURE_COLS]
    if not keep:
        raise RuntimeError("No numeric feature columns left after filtering metadata.")
    return keep


def encode_multiclass_labels(series: pd.Series) -> tuple[np.ndarray, dict]:
    """BENIGN -> 0, other classes -> 1..K-1, deterministic ordering."""
    unique = sorted(series.unique().tolist())
    mapping: dict[str, int] = {}
    if "BENIGN" in unique:
        mapping["BENIGN"] = 0
        unique = [c for c in unique if c != "BENIGN"]
    for c in unique:
        mapping[c] = len(mapping)
    return series.map(mapping).astype(np.int64).to_numpy(), mapping


def prepare_split(
    df: pd.DataFrame,
    feature_cols: List[str],
    scaler: Optional[StandardScaler],
    label_mapping: Optional[dict],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, StandardScaler, dict]:
    X = df[feature_cols].astype(np.float32)
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0).to_numpy()

    if scaler is None:
        scaler = StandardScaler().fit(X)
    X = scaler.transform(X).astype(np.float32)

    if label_mapping is None:
        y_multi, label_mapping = encode_multiclass_labels(df["multiclass_label"])
    else:
        y_multi = df["multiclass_label"].map(label_mapping).fillna(-1).astype(np.int64).to_numpy()
        unmapped = (y_multi == -1).sum()
        if unmapped:
            log(f"WARN: {unmapped:,} rows had unseen attack class — relabeled as last class")
            y_multi = np.where(y_multi == -1, max(label_mapping.values()), y_multi)

    y_binary = df["binary_label"].astype(np.int64).to_numpy()
    return X, y_multi, y_binary, scaler, label_mapping


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
def train_clad(
    model,
    criterion,
    train_dl,
    device,
    epochs: int,
    lr: float,
    weight_decay: float,
    print_every: int,
):
    from util.schedules import WarmupCosineSchedule, LRSchedule

    optim = T.optim.AdamW(
        model.parameters(),
        lr=1e-6,
        betas=(0.9, 0.999),
        weight_decay=weight_decay,
    )
    base_schedule = WarmupCosineSchedule(
        start_val=1e-6,
        end_val=1e-6,
        ref_val=lr,
        T_max=epochs * len(train_dl),
        warmup_steps=int((epochs // 10) * len(train_dl)),
    )
    lr_schedule = LRSchedule(optim, schedule=base_schedule)

    for epoch in range(1, epochs + 1):
        model.train()
        running = 0.0
        nb = 0
        t0 = time.time()
        for x, y in train_dl:
            x = x.to(device, non_blocking=True)
            y = y.to(device, non_blocking=True)
            z = model(x)
            loss = criterion(x=z, y=y)
            optim.zero_grad()
            loss.backward()
            optim.step()
            running += float(loss.item())
            nb += 1
        lr_schedule.step()
        if epoch == 1 or epoch % print_every == 0 or epoch == epochs:
            log(
                f"epoch {epoch:>3d}/{epochs} | loss={running/max(1,nb):.4f} | "
                f"time={time.time()-t0:.1f}s"
            )


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------
@T.no_grad()
def get_embeddings(model, X: np.ndarray, device, chunk: int = 4096) -> np.ndarray:
    model.eval()
    out = []
    for i in range(0, len(X), chunk):
        x = T.from_numpy(X[i : i + chunk]).to(device)
        z = model(x).cpu().numpy()
        out.append(z)
    return np.concatenate(out, axis=0).astype(np.float32)


def benign_centroid(embeds: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    c = embeds.mean(axis=0)
    n = float(np.linalg.norm(c)) + eps
    return c / n


def anomaly_score(embeds: np.ndarray, centroid: np.ndarray) -> np.ndarray:
    """attack_score in [0, 1]. Larger = more attack-like.

    Mapped from cosine similarity via gate_prob = (1 - cosine_sim) / 2:
        cossim = +1 (most benign-like, aligned with centroid) -> 0.0
        cossim =  0 (orthogonal)                              -> 0.5
        cossim = -1 (most anomalous, opposite of centroid)    -> 1.0
    The transform is monotone, so the val_accuracy_calibrated tau* found
    downstream by proposed_method_valcal.py is exactly the same operating
    point as the raw -cosine_sim ranking. The [0,1] range is required
    because proposed_method_valcal.py validates that gate_prob in [0,1].
    """
    sim = embeds @ centroid
    score = ((1.0 - sim) * 0.5).clip(0.0, 1.0)
    return score.astype(np.float32)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def export_predictions(
    out_dir: Path,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    val_score: np.ndarray,
    test_score: np.ndarray,
):
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, base, scores in [
        ("val", val_df, val_score),
        ("test", test_df, test_score),
    ]:
        out = base.copy()
        # columns proposed_method_valcal.py expects:
        # binary_label (label), snort_pred, gate_prob (score)
        out["snort_pred"] = 0
        out["snort_score"] = 0.0
        out["gate_prob"] = scores
        out["escalated"] = 1
        out["cascade_pred"] = 0  # placeholder; real prediction comes from val-cal tau*
        out["cascade_score"] = scores
        out["split"] = name
        path = out_dir / f"{name}_closr_predictions.csv"
        out.to_csv(path, index=False)
        log(f"wrote {path}  rows={len(out):,}")
        paths[name] = path
    return paths


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(args):
    set_random_seed(args.seed)
    device = T.device("cuda" if T.cuda.is_available() else "cpu")
    log(f"device={device} | torch={T.__version__}")

    # Inject CLOSR repo onto sys.path so its model/loss/loader imports resolve
    closr_root = Path(args.closr_repo).resolve()
    if not closr_root.exists():
        raise FileNotFoundError(f"CLOSR repo not found at: {closr_root}")
    sys.path.insert(0, str(closr_root))
    from model.model import ContrastiveMLP
    from losses.clad_loss import CLADLoss
    from data.loaders import tabular_dl

    log(f"loading data from {args.data_dir} | strategy={args.split_strategy}")
    cleaned, splits = load_and_prepare_detection_data(
        args.data_dir,
        random_state=args.seed,
        split_strategy=args.split_strategy,
    )

    feature_cols = select_feature_columns(splits.train_all)
    log(f"feature columns selected: {len(feature_cols)} (first 5: {feature_cols[:5]})")

    X_train, y_train_multi, y_train_bin, scaler, label_map = prepare_split(
        splits.train_all, feature_cols, None, None
    )
    X_val, _, y_val_bin, _, _ = prepare_split(splits.val_all, feature_cols, scaler, label_map)
    X_test, _, y_test_bin, _, _ = prepare_split(splits.test_all, feature_cols, scaler, label_map)

    n_classes = max(2, len(label_map))
    log(
        f"X_train={X_train.shape}  X_val={X_val.shape}  X_test={X_test.shape}  "
        f"n_classes={n_classes} | label_map={label_map}"
    )

    model = ContrastiveMLP(
        d_in=X_train.shape[1],
        n_classes=n_classes,
        d_out=args.d_out,
        neurons=args.neurons,
        dropout=args.dropout,
        residual=False,
    ).to(device)
    criterion = CLADLoss(m=args.margin, squared=True).to(device)

    train_dl = tabular_dl(
        x=X_train,
        y=y_train_multi,
        batch_size=args.batch_size,
        balanced=True,
        drop_last=True,
        num_workers=0,
    )
    log(
        f"training CLAD | batches/epoch={len(train_dl):,} | epochs={args.epochs} | "
        f"batch_size={args.batch_size} | lr={args.lr}"
    )
    train_clad(
        model=model,
        criterion=criterion,
        train_dl=train_dl,
        device=device,
        epochs=args.epochs,
        lr=args.lr,
        weight_decay=args.weight_decay,
        print_every=max(1, args.epochs // 20),
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    weights_path = out_dir / "clad_baseline.pt"
    T.save(
        {
            "model_state": model.state_dict(),
            "feature_columns": feature_cols,
            "label_mapping": label_map,
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
            "n_classes": n_classes,
            "d_out": args.d_out,
            "neurons": args.neurons,
        },
        weights_path,
    )
    log(f"saved weights -> {weights_path}")

    # benign centroid from training benign rows (matches CLOSR original methodology)
    benign_idx = y_train_multi == 0
    log(f"computing benign centroid from train_benign | n={int(benign_idx.sum()):,}")
    benign_emb = get_embeddings(model, X_train[benign_idx], device, chunk=args.chunk_size)
    centroid = benign_centroid(benign_emb)

    val_emb = get_embeddings(model, X_val, device, chunk=args.chunk_size)
    test_emb = get_embeddings(model, X_test, device, chunk=args.chunk_size)
    val_score = anomaly_score(val_emb, centroid)
    test_score = anomaly_score(test_emb, centroid)

    log(
        f"score stats | val: mean={val_score.mean():.4f} std={val_score.std():.4f} | "
        f"test: mean={test_score.mean():.4f} std={test_score.std():.4f}"
    )

    paths = export_predictions(out_dir, splits.val_all, splits.test_all, val_score, test_score)

    write_json(
        {
            "model": "CLAD (Wilkie et al., IEEE TNSM 2026)",
            "split_strategy": args.split_strategy,
            "seed": args.seed,
            "epochs": args.epochs,
            "batch_size": args.batch_size,
            "lr": args.lr,
            "margin": args.margin,
            "d_out": args.d_out,
            "neurons": args.neurons,
            "n_features": int(X_train.shape[1]),
            "n_classes": n_classes,
            "label_mapping": label_map,
            "n_train": int(X_train.shape[0]),
            "n_val": int(X_val.shape[0]),
            "n_test": int(X_test.shape[0]),
            "n_train_benign": int(benign_idx.sum()),
            "val_csv": str(paths["val"]),
            "test_csv": str(paths["test"]),
            "centroid_source": "train_benign",
            "score_definition": "-cosine_similarity(embedding, normalized_mean_benign_embedding)",
        },
        out_dir / "closr_baseline_summary.json",
    )
    log(
        f"done. To compute val-calibrated headline metrics, run:\n"
        f"  python3 proposed_method_valcal.py \\\n"
        f"    --val-csv {paths['val']} \\\n"
        f"    --test-csv {paths['test']} \\\n"
        f"    --out-dir {out_dir}_valcal \\\n"
        f"    --calibrate-isotonic"
    )


def parse_args():
    p = argparse.ArgumentParser(description="CLAD/CLOSR baseline runner on our locked CIC-IDS2017 split.")
    p.add_argument("--data-dir", required=True, help="Path to csv_CIC_IDS2017/")
    p.add_argument("--closr-repo", default="../CLOSR", help="Path to cloned jackwilkie/CLOSR repo")
    p.add_argument("--out-dir", default="outputs_closr_baseline")
    p.add_argument("--split-strategy", default="temporal_by_file",
                   choices=["random", "temporal", "temporal_by_file"])
    p.add_argument("--seed", type=int, default=42)
    # training hyperparams (mirror CLOSR defaults)
    p.add_argument("--epochs", type=int, default=200)
    p.add_argument("--batch-size", type=int, default=4096)
    p.add_argument("--lr", type=float, default=4.4e-6)
    p.add_argument("--weight-decay", type=float, default=3e-7)
    p.add_argument("--margin", type=float, default=1.0)
    p.add_argument("--d-out", type=int, default=8)
    p.add_argument("--neurons", type=int, nargs="+", default=[1024, 1024, 1024, 1024])
    p.add_argument("--dropout", type=float, default=0.0)
    p.add_argument("--chunk-size", type=int, default=4096, help="Inference chunk size")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())
