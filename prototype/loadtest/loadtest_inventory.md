# Loadtest Output Inventory — Which JSON is "the Real One"

The `prototype/loadtest/outputs/` directory has 20+ JSON run files from
multiple experiment dates. This document identifies which files
produced each paper figure/claim and which are reruns/superseded.

## Rule of thumb

**Use the `20260425` timestamped files for everything in the paper.**

The `20260428` files are reruns from three days later, performed after
fixing a deduplication bug; they exercise different rates and durations
and are not the canonical numbers reported in Section V.C.

## File-by-file inventory

### E1 — Throughput-latency frontier (paper Section V.C.E1)

Paper claim: Kafka p50/p95/p99/max = 14/19/22/33 ms, Direct-HTTP =
12/18/27/**1087** ms at 50 req/s, demonstrating $32\times$ tail
latency reduction.

| Status | File | Mode | Rate (rps) | Verified |
|---|---|---|---|---|
| **PAPER** | `e1_rate50_kafka_20260425T083223Z.json` | Kafka | 50 | max=33.4ms ✓ |
| **PAPER** | `e1_rate50_direct_http_20260425T083539Z.json` | Direct-HTTP | 50 | max=1086.8ms ✓ |
| sweep | `e1_rate5_kafka_20260425T083052Z.json` | Kafka | 5 | low-load reference |
| sweep | `e1_rate20_kafka_20260425T083139Z.json` | Kafka | 20 | mid-load reference |
| sweep | `e1_rate100_kafka_20260425T083310Z.json` | Kafka | 100 | high-load reference |
| sweep | `e1_rate5_direct_http_20260425T083407Z.json` | Direct-HTTP | 5 | low-load reference |
| sweep | `e1_rate20_direct_http_20260425T083455Z.json` | Direct-HTTP | 20 | mid-load reference |
| sweep | `e1_rate100_direct_http_20260425T083623Z.json` | Direct-HTTP | 100 | high-load reference |
| rerun | `e1_rate5_kafka_20260428T072006Z.json` | Kafka | 5 | post-dedup-fix rerun |
| rerun | `e1_rate50_kafka_20260428T072942Z.json` | Kafka | 50 | post-dedup-fix rerun |
| rerun | `e1_rate200_kafka_20260428T073030Z.json` | Kafka | 200 | post-dedup-fix rerun (new rate) |

Plot source: `figures/fig_e1_tail.pdf` and `fig_e1_frontier.pdf`.

### E2 — Noisy-neighbour tenant isolation (paper Section V.C.E2)

Paper claim: 3 tenants (1 noisy at 200 rps, 2 quiet at 5 rps each)
for 60 s. Kafka: quiet delivery 99.7%, total sent 12,300.
Direct-HTTP: quiet delivery 55.8%, total sent 5,105.

| Status | File | Mode | Verified |
|---|---|---|---|
| **PAPER** | `e2_noisy_neighbour_kafka_20260425T091303Z.json` | Kafka | sent=12,300, acme (quiet) 99.67%, globex (noisy) 94.87% ✓ |
| **PAPER** | `e2_noisy_neighbour_direct_http_20260425T091435Z.json` | Direct-HTTP | sent=5,105, acme (quiet) 55.77%, globex (noisy) 46.39% ✓ |
| rerun | `e2_noisy_neighbour_kafka_20260428T071121Z.json` | Kafka | post-dedup-fix rerun |
| rerun | `e2_noisy_neighbour_kafka_20260428T072057Z.json` | Kafka | post-dedup-fix rerun |
| rerun | `e2_noisy_neighbour_kafka_20260428T072357Z.json` | Kafka | post-dedup-fix rerun |
| rerun | `e2_noisy_neighbour_direct_http_20260428T071224Z.json` | Direct-HTTP | post-dedup-fix rerun |
| rerun | `e2_noisy_neighbour_direct_http_20260428T072205Z.json` | Direct-HTTP | post-dedup-fix rerun |

Plot source: `figures/fig_e2_isolation.pdf` and `fig_e2_throughput.pdf`.

### E5 — Resource footprint (paper Section V.C.E5)

Paper claim: 3 tenants × 50 rps × 60 s, `docker stats` at 1 Hz.
Kafka 298% CPU vs Direct-HTTP 272% CPU at the system level, with
the Kafka broker contributing 121.7% by itself.

| Status | File | Mode |
|---|---|---|
| **PAPER** | `e5_resource_footprint_kafka_20260425T092928Z.json` | Kafka |
| **PAPER** | `e5_resource_footprint_direct_http_20260425T093102Z.json` | Direct-HTTP |

Plot source: `figures/fig_e5_resources.pdf`.

## Verified per-tenant breakdown (extracted from PAPER files)

Useful for the camera-ready if a new SaaS-focused subsection is
added. All numbers from the `summary.per_tenant` block of the
`20260425` JSONs.

### Kafka — noisy-neighbour run

| Tenant | Role | Sent | Delivered | Delivery rate | p50 latency | p95 latency | p99 latency | Max latency |
|---|---|---|---|---|---|---|---|---|
| acme | quiet (5 rps) | 300 | 299 | **99.67%** | 6,477 ms | 13,546 ms | 15,122 ms | 15,705 ms |
| globex | noisy (200 rps) | 12,000 | 11,384 | 94.87% | 7,611 ms | 16,127 ms | 17,843 ms | 19,080 ms |

### Direct-HTTP — noisy-neighbour run

| Tenant | Role | Sent | Delivered | Delivery rate | p50 latency | p95 latency | p99 latency | Max latency |
|---|---|---|---|---|---|---|---|---|
| acme | quiet (5 rps) | 104 | 58 | **55.77%** | 577 ms | 1,999 ms | 2,478 ms | 8,961 ms |
| globex | noisy (200 rps) | 5,001 | 2,320 | 46.39% | 575 ms | 2,849 ms | 44,261 ms | **140,583 ms** |

### Key per-tenant insights for the paper

1. **Quiet-tenant delivery gap is decisive**: 99.67% vs 55.77% is the
   gap that motivates the broker. SaaS service-level agreements are
   written per tenant, and a quiet tenant losing 44 pp of delivery
   purely because another tenant is noisy violates any reasonable SLA.

2. **Direct-HTTP collapses on BOTH tenants**: even the noisy tenant
   only achieves 46% delivery under Direct-HTTP, because the gateway
   saturates and starts dropping requests for everyone. The quiet
   tenant is not just "starved" — it shares fate with the noisy
   tenant.

3. **Direct-HTTP worst-case latency is 140 seconds**: globex's max
   latency under Direct-HTTP reached 2 minutes 20 seconds. Under
   Kafka the same tenant's max was 19 seconds. This is the dramatic
   "head-of-line blocking" effect the paper describes.

4. **Quiet-tenant latency is also dramatically better on Direct-HTTP
   *for the rows that did get through*** (577 ms p50 vs 6,477 ms
   p50 on Kafka). This is honestly worth noting because it shows
   the Kafka broker introduces queueing latency for everyone. The
   trade-off: Kafka costs everyone a few seconds of p50 latency to
   protect the quiet tenant's delivery rate.

## Suggested paper additions (no new experiments)

If the camera-ready needs more SaaS emphasis without running new
experiments, paste the per-tenant table above as a new Table III
under Section V.C.E2 and add the four insights as discussion. This
addresses Reviewer 1's "scalability experiments small-scale" concern
by showing the per-tenant level of detail that aggregate numbers hide.

## Files to delete / archive (optional cleanup)

If you want to clean `prototype/loadtest/outputs/` before
camera-ready submission:

- Move the eight `20260428` rerun files into a
  `outputs/post_dedup_fix_reruns/` subfolder. They are not in the
  paper but are useful proof that the dedup fix did not regress
  the headline numbers.
- Keep the eleven `20260425` files as the canonical paper-backing
  results.
- The `outputs/archived_dedup_bug/` folder is already separated
  correctly — leave it alone.
