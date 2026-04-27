"""Streaming adapter for the Hybrid-Cascade detector.

The research pipeline (pipeline/rf_anomaly.py, pipeline/conformal_wrapper.py,
pipeline/escalation_gate_fastsnort.py, pipeline/signature_rate_rules.py) is
built for offline batch use: it trains on a CIC-IDS2017 fold and saves nothing
we can reload cheaply. For a *prototype* we need a per-flow scoring path that
runs in O(ms), so this module:

  1) Inlines the six rate-rule thresholds from signature_rate_rules.py so the
     Tier-1 fast path is exact.
  2) Provides a pluggable Tier-2 backend:
        - if /models/gate.joblib exists, load it (joblib .pkl containing dict
          with keys: rf, conformal, gate, feature_order, tau_star)
        - otherwise fall back to a conservative statistical scorer that still
          honours the val-calibrated tau* contract.

The returned Verdict is what gets emitted on tenant.{u}.alerts (or .clean).
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import numpy as np

log = logging.getLogger("cascade")

# ---------------------------------------------------------------------------
# Rate-rule thresholds. Values mirror signature_rate_rules.py defaults so the
# prototype Tier-1 matches the offline ablation numbers.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RateRuleConfig:
    # V: volumetric — total fwd+bwd packets per second
    v_pps: float = 500.0
    # L: slow-HTTP — low fwd pkt rate with long idle
    l_pps_max: float = 2.0
    l_idle_min: float = 60.0
    # S: SYN flood — SYN count >> established connections
    s_syn_ratio: float = 3.0
    s_syn_min: int = 30
    # R: RST anomaly — RST count per flow
    r_rst_min: int = 10
    # P: port scan — short flow + low byte count + distinct dst port hint
    p_duration_max: float = 1.0
    p_bytes_max: int = 128
    # B: brute force — many init fwd packets on auth port (21/22/3389) in short time
    b_ports: tuple = (21, 22, 23, 3389)
    b_init_fwd_min: int = 10

    # Tier-1 OR set: the paper promotes {V, S, P} only (high-precision).
    # L, R, B stay as Tier-2 meta-features (too noisy for auto-alert).
    tier1_keys: tuple = ("V", "S", "P")


# ---------------------------------------------------------------------------
# Feature extraction — tolerant of Wireshark / CICFlowMeter column naming.
# ---------------------------------------------------------------------------
def _f(row: Mapping[str, Any], *names: str, default: float = 0.0) -> float:
    for n in names:
        if n in row and row[n] is not None:
            try:
                v = float(row[n])
                if not math.isnan(v):
                    return v
            except (TypeError, ValueError):
                pass
        lo = n.lower().replace(" ", "_")
        if lo in row and row[lo] is not None:
            try:
                v = float(row[lo])
                if not math.isnan(v):
                    return v
            except (TypeError, ValueError):
                pass
    return default


def rate_signals(features: Mapping[str, Any], cfg: RateRuleConfig) -> dict[str, int]:
    """Return {rule: 0/1} for the six rate rules."""
    duration_s = max(_f(features, "Flow Duration", "flow_duration") / 1e6, 1e-6)
    fwd_pkts = _f(features, "Total Fwd Packets", "total_fwd_packets")
    bwd_pkts = _f(features, "Total Backward Packets", "total_backward_packets")
    pkts = fwd_pkts + bwd_pkts
    pps = pkts / duration_s

    idle_mean = _f(features, "Idle Mean", "idle_mean") / 1e6
    syn = _f(features, "SYN Flag Count", "syn_flag_count")
    ack = max(_f(features, "ACK Flag Count", "ack_flag_count"), 1.0)
    rst = _f(features, "RST Flag Count", "rst_flag_count")
    total_bytes = _f(features, "Total Length of Fwd Packets", "total_length_of_fwd_packets") + \
                  _f(features, "Total Length of Bwd Packets", "total_length_of_bwd_packets")
    dst_port = int(_f(features, "Destination Port", "destination_port"))
    init_fwd = _f(features, "Init_Win_bytes_forward", "init_win_bytes_forward",
                  "Fwd Init Win Bytes", default=fwd_pkts)

    v = int(pps >= cfg.v_pps)
    l = int(pps <= cfg.l_pps_max and idle_mean >= cfg.l_idle_min)
    s = int(syn >= cfg.s_syn_min and (syn / ack) >= cfg.s_syn_ratio)
    r = int(rst >= cfg.r_rst_min)
    p = int(duration_s <= cfg.p_duration_max and total_bytes <= cfg.p_bytes_max)
    b = int(dst_port in cfg.b_ports and init_fwd >= cfg.b_init_fwd_min)

    return {"V": v, "L": l, "S": s, "R": r, "P": p, "B": b}


# ---------------------------------------------------------------------------
# Tier-2 scorer backends
# ---------------------------------------------------------------------------
class _FallbackScorer:
    """Conservative statistical scorer used when no trained model is mounted.

    This is *intentionally* simple: it converts the six rate signals and a
    couple of raw flow features into a logistic blend. Numbers are calibrated
    to roughly match the locked Tier-2 operating point on CIC-IDS2017, but the
    reported results in the paper still come from the offline pipeline, not
    this fallback. The prototype only needs a plausible per-flow score.
    """

    def score(self, features: Mapping[str, Any], sig: dict[str, int],
              snort_hit: int) -> tuple[float, float]:
        pps = _f(features, "Flow Packets/s", "flow_packets_s", "flow_pkts_s")
        bps = _f(features, "Flow Bytes/s", "flow_bytes_s", "flow_byts_s")
        pkt_len_std = _f(features, "Packet Length Std", "packet_length_std")
        # heuristic "anomaly score" s(x) in [0, 1]
        z = (
            0.30 * sig["V"] + 0.35 * sig["S"] + 0.30 * sig["P"]
            + 0.15 * sig["L"] + 0.10 * sig["R"] + 0.25 * sig["B"]
            + 0.10 * math.tanh(pps / 5000.0)
            + 0.10 * math.tanh(bps / 1e7)
            + 0.10 * math.tanh(pkt_len_std / 500.0)
            + 0.60 * snort_hit
        )
        s = 1.0 / (1.0 + math.exp(-(z - 0.6) * 3.0))
        # mock conformal p-value: smaller s -> larger p
        p = max(1e-4, 1.0 - s)
        return s, p


class _JoblibScorer:
    """Loads an exported bundle produced by pipeline/cascade_export_patch.py."""

    def __init__(self, path: Path) -> None:
        import joblib  # local import — heavy

        bundle = joblib.load(path)
        self.rf = bundle["rf"]
        self.conformal = bundle["conformal"]
        self.gate = bundle["gate"]
        self.feature_order: list[str] = list(bundle["feature_order"])
        log.info("loaded joblib bundle: %d features, tau*=%s",
                 len(self.feature_order), bundle.get("tau_star"))

    def _vector(self, features: Mapping[str, Any]) -> np.ndarray:
        return np.asarray([[_f(features, c) for c in self.feature_order]], dtype=float)

    def score(self, features: Mapping[str, Any], sig: dict[str, int],
              snort_hit: int) -> tuple[float, float]:
        X = self._vector(features)
        s = float(self.rf.score_samples(X)[0])
        p = float(self.conformal.pvalue(X)[0])
        meta = np.asarray([[
            s, p, snort_hit,
            sig["V"], sig["L"], sig["S"], sig["R"], sig["P"], sig["B"],
        ]])
        z = np.hstack([X, meta])
        prob = float(self.gate.predict_proba(z)[0, 1])
        return prob, p


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
@dataclass
class Verdict:
    decision: int               # 0 = benign, 1 = attack
    score: float                # final score in [0, 1]
    tier: str                   # "tier1_signature", "tier1_rate", "tier2_gate"
    tau_star: float
    rate_signals: dict[str, int] = field(default_factory=dict)
    snort_hit: int = 0
    p_value: float | None = None
    reason: str = ""


class HybridCascade:
    """Per-flow streaming scorer. Thread-unsafe; one per worker task."""

    def __init__(self, tau_star: float, model_dir: str | None = None) -> None:
        self.tau_star = tau_star
        self.cfg = RateRuleConfig()

        backend: Any
        if model_dir:
            bundle_path = Path(model_dir) / "gate.joblib"
            if bundle_path.exists():
                try:
                    backend = _JoblibScorer(bundle_path)
                    log.info("cascade: using trained bundle at %s", bundle_path)
                except Exception as e:  # noqa: BLE001
                    log.warning("cascade: bundle load failed (%s); using fallback", e)
                    backend = _FallbackScorer()
            else:
                log.info("cascade: no bundle at %s; using fallback scorer", bundle_path)
                backend = _FallbackScorer()
        else:
            backend = _FallbackScorer()
        self.backend = backend

    def decide(self, features: Mapping[str, Any], snort_hit: int = 0) -> Verdict:
        sig = rate_signals(features, self.cfg)
        tier1_rate = any(sig[k] for k in self.cfg.tier1_keys)

        if snort_hit:
            return Verdict(1, 1.0, "tier1_signature", self.tau_star,
                           sig, snort_hit, None, "snort signature hit")
        if tier1_rate:
            hit_keys = [k for k in self.cfg.tier1_keys if sig[k]]
            return Verdict(1, 1.0, "tier1_rate", self.tau_star,
                           sig, snort_hit, None,
                           f"rate rule(s) fired: {','.join(hit_keys)}")

        score, pval = self.backend.score(features, sig, snort_hit)
        decision = int(score >= self.tau_star)
        reason = f"gate score {score:.4f} {'>=' if decision else '<'} tau* {self.tau_star:.4f}"
        return Verdict(decision, float(score), "tier2_gate", self.tau_star,
                       sig, snort_hit, float(pval), reason)
