# Strata Performance Characterization

This document summarizes load testing results and provides guidance for capacity planning.

## Executive Summary

| Metric | Value |
|--------|-------|
| **Safe operating envelope** | ~40 concurrent users / ~32 req/s |
| **Peak throughput** | 32.3 req/s (at 55 users) |
| **Saturation point** | 55 users (429 rate > 5%) |
| **Latency knee** | 71 users (p95 explodes to 350ms+) |
| **Warm-cache p95 latency** | ~26ms |
| **Cold-cache p95 latency** | ~56ms |

## Test Configuration

### Server Settings
- **Interactive slots**: 8 (default)
- **Bulk slots**: 4 (default)
- **Total concurrent scans**: 12
- **Cache size**: 150MB

### Workload Mix
- **Dashboard queries**: 80% (small, filtered, 3 columns)
- **Analyst queries**: 15% (medium, unfiltered, 5 columns)
- **Bulk queries**: 5% (large, all columns)

### Test Data
- 6 tables, 30,000 rows each
- ~1MB estimated response per scan
- Local filesystem storage (not S3)

## Capacity Curve

Results from capacity sweep (40-150 concurrent users, 2 min per level):

| Users | Offered RPS | Goodput RPS | p95 (ms) | 429 Rate | Success Rate |
|-------|-------------|-------------|----------|----------|--------------|
| 40 | 33.4 | **32.0** | 56 | 4% | 96% |
| 55 | 46.5 | **32.3** | 26 | 31% | 69% |
| 71 | 56.5 | 30.3 | 350 | 46% | 54% |
| 87 | 70.3 | 28.9 | 377 | 59% | 41% |
| 102 | 74.8 | 25.8 | 2851 | 65% | 35% |
| 118 | 95.4 | 24.7 | 27 | 74% | 26% |
| 134 | 109.4 | 22.2 | 79 | 80% | 20% |
| 150 | 122.5 | 20.8 | 151 | 83% | 17% |

### Key Observations

1. **Capacity ceiling at ~32 req/s**: Peak goodput reached 32.3 req/s at 55 users. This is the maximum sustainable throughput with default settings (8 interactive + 4 bulk slots).

2. **Goodput degrades under heavy overload**: Beyond saturation, goodput actually *drops* (from 32 to 21 req/s at 150 users). The server spends more time handling rejects than serving requests.

3. **Latency spike at 71-102 users**: p95 latency exploded from 26ms to 350ms-2800ms at the "latency knee" where queuing delays cascade.

4. **Latency recovery at extreme overload**: p95 drops back down at 118+ users because most requests are fast-failed with 429 before they can queue.

5. **Zero errors**: Clean degradation with zero 5xx errors, timeouts, or connection failures. The rate limiter protects the server effectively.

## Operating Zones

```
         Goodput (req/s)
    35 ─┬────────────────────────────────────────
       │                    ╭─ Peak (32.3 @ 55 users)
    30 ─┤    ╭──────────────╯
       │   ╱                  ╲
    25 ─┤  ╱    Safe Zone      ╲   Overload Zone
       │ ╱                      ╲
    20 ─┼─────────────────────────╲──────────────
       │                           ╲
       └─┬────┬────┬────┬────┬────┬────┬────┬───
         40   55   71   87  102  118  134  150
                    Users
              ↑         ↑
          Saturation  Latency
            Point      Knee
```

### Zone Definitions

| Zone | Users | Characteristics |
|------|-------|-----------------|
| **Safe** | < 40 | Low 429 rate (< 5%), stable latency, full goodput |
| **Marginal** | 40-55 | Some 429s but goodput maintained, cache warming |
| **Overload** | 55+ | High 429 rate, goodput degrades, latency spikes |

## QoS Tier Behavior

The two-tier QoS system (interactive + bulk) provides workload isolation:

| Tier | Slots | Max Bytes | Max Columns | Behavior |
|------|-------|-----------|-------------|----------|
| Interactive | 8 | 10MB | 50 | Priority, fast-fail on overload |
| Bulk | 4 | unlimited | unlimited | Background, queue behind interactive |

### Per-Tier p95 Latency (at 55 users)

| Query Type | p95 Latency | POST p95 | GET p95 |
|------------|-------------|----------|---------|
| Dashboard | 24ms | 10ms | 11ms |
| Analyst | 24ms | 10ms | 13ms |
| Bulk | 37ms | 16ms | 21ms |

Bulk queries see ~50% higher latency due to larger payloads and lower priority.

## Recommendations

### For Production Deployment

1. **Set user limits based on expected load**: Target < 40 concurrent users per server instance for safe operation.

2. **Monitor 429 rate**: If 429 rate exceeds 5%, add capacity or reduce load.

3. **Watch p95 latency**: Latency spikes above 100ms indicate approaching saturation.

4. **Prefer warm caches**: Cold-cache latency (56ms) is ~2x warm-cache (26ms). Consider cache warming strategies for predictable workloads.

### Scaling Options

If more throughput is needed:

1. **Horizontal scaling**: Add more Strata instances behind a load balancer.

2. **Increase slot counts**: Experiment with higher `interactive_slots` and `bulk_slots` values. Current tests used defaults (8+4=12 total). Higher values may improve throughput if the bottleneck is slot availability rather than I/O or CPU.

3. **Enable adaptive concurrency**: The `AdaptiveConcurrencyController` can dynamically adjust slot counts based on latency. Currently disabled by default.

## Benchmark Commands

### Standard Capacity Sweep (5-60 users)
```bash
uv run python benchmarks/capacity_sweep.py
```

### High-Load Sweep (40-150 users)
```bash
uv run python benchmarks/capacity_sweep.py --levels 8 --min-users 40 --max-users 150 --duration 120
```

### Quick Smoke Test
```bash
uv run python benchmarks/capacity_sweep.py --quick
```

### Custom Configuration
```bash
uv run python benchmarks/capacity_sweep.py \
    --levels 6 \
    --min-users 10 \
    --max-users 100 \
    --duration 90 \
    --interactive-slots 12 \
    --bulk-slots 6
```

## Test Environment

- **Machine**: Apple M1, 16GB RAM
- **OS**: macOS 26.2 (Tahoe)
- **Python**: 3.13.3
- **Storage**: Local SSD (not S3)
- **Date**: December 2024

Results may vary on different hardware, with S3 storage, or under different workload patterns.

## Related Files

- [benchmarks/capacity_sweep.py](../benchmarks/capacity_sweep.py) - Capacity sweep benchmark
- [benchmarks/soak_test.py](../benchmarks/soak_test.py) - Long-running stability test
- [benchmarks/results/](../benchmarks/results/) - Raw benchmark results (JSON)
