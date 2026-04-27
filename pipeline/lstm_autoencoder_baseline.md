# lstm_autoencoder_baseline

## What it does

Standalone LSTM-autoencoder anomaly detector. Trains on benign data only using sliding-window reconstruction, scores test data by per-sequence reconstruction error, and returns a vector of anomaly scores. Designed to be called by `compare_anomaly_baselines_valcal.py` but can also be used independently.

Not runnable as a standalone script; this module exports the `lstm_autoencoder_scores()` function for use by the baseline-comparison framework.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` (includes PyTorch)

## Function signature

```python
def lstm_autoencoder_scores(
    X_train_benign: np.ndarray,
    X_test: np.ndarray,
    seq_len: int = 10,
    hidden_size: int = 64,
    latent_dim: int = 32,
    epochs: int = 8,
    batch_size: int = 256,
    lr: float = 1e-3,
    train_size: int = 200_000,
    device: str = "cpu",
    seed: int = 42,
) -> np.ndarray
```

## Inputs (when called from compare_anomaly_baselines_valcal.py)

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `X_train_benign` | — | (n_benign, n_features) array of benign training flows |
| `X_test` | — | (n_test, n_features) array of test flows (benign + attack) |
| `seq_len` | `10` | Sliding-window length; trains on sequences of `seq_len` consecutive flows |
| `hidden_size` | `64` | Hidden dimension of LSTM encoder and decoder |
| `latent_dim` | `32` | Bottleneck latent dimension |
| `epochs` | `8` | Number of training epochs |
| `batch_size` | `256` | Batch size for Adam optimization |
| `lr` | `1e-3` | Adam learning rate |
| `train_size` | `200_000` | Max benign rows to use for training (subsampling for speed) |
| `device` | `cpu` | PyTorch device: `cpu` or `cuda` |
| `seed` | `42` | Random seed |

## Architecture

**Encoder**: LSTM (input_dim → hidden_size) → fully-connected (hidden_size → latent_dim)

**Decoder**: latent_dim tiled seq_len times → LSTM (latent_dim → hidden_size) → fully-connected per-timestep (hidden_size → input_dim)

**Loss**: MSE on full-sequence reconstruction of benign training data

**Scoring**: Per-sequence reconstruction MSE on the last timestep only. Rows [0, seq_len-2] reuse the first valid score to match input row count.

## Outputs

**Returns**: 1D numpy array of shape (n_test,) with reconstruction-error anomaly scores. Higher scores indicate greater anomaly. Scores are in the range of per-feature MSE (order of magnitude depends on feature scaling).

## How to interpret the scores

The returned array is directly usable by `compare_anomaly_baselines_valcal.py`, which applies validation-calibrated thresholding. Raw scores are not interpretable in isolation; apply a threshold to binary predictions.

## Common problems

1. **PyTorch not installed**: Install via `pip install torch` or ensure `pip install -r requirements.txt` from the repo root succeeded.

2. **CUDA device not available**: If you request `device=cuda` but no GPU is detected, PyTorch will raise a RuntimeError. Fall back to `device=cpu` (slower but works).

3. **Out of memory on GPU**: Reduce `latent_dim`, `hidden_size`, or `batch_size`. Or use CPU.

4. **Slow training on CPU**: LSTM-autoencoder training is I/O-bound on CPU. Expect ~30–60 seconds per epoch for 200k flows. Use GPU if available.

5. **NaN anomaly scores**: If training diverges (loss becomes NaN), reduce `lr` (e.g., to 1e-4) or increase `epochs` for more stable convergence. The baseline-comparison script catches this and falls back gracefully.

## Relationship to the comparison framework

When called by `compare_anomaly_baselines_valcal.py`, the scores are:
1. Computed on the full test set (benign + attack)
2. Combined with validation scores for threshold selection
3. Thresholded at the validation-optimal point
4. Evaluated on the test set to produce a row in the baseline metrics table
