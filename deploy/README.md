# Deploying the waterfall pipeline

Three tiers: **sigproc** (RF collector, rpi-four-2) → **waterfall-consumer**
(signal middleware) → **waterfall-bridge** (web tier) → browser.

```
sigproc ──UDP 8202/8212 B──▶ waterfall-service.default.svc:4820
                                        │
                              waterfall-consumer  (STFT, gap detection)
                                        │ POST /ingest  (batched rows)
                                        ▼
                              waterfall-ui.default.svc:4780
                                        │ /ws /chunks /meta /stats
                                        ▼
                         kubectl -n default port-forward svc/waterfall-ui 4780:4780
```

## Files

| File | Purpose |
|---|---|
| `Dockerfile` | One **linux/amd64** image with both binaries + the static front end |
| `kaniko-job.yaml` | In-cluster build → `ghcr.io/rodoyle/waterfall:latest` (builder pinned to an amd64 node) |
| `waterfall-consumer.yaml` | Middleware Deployment + Service `waterfall-service` (UDP 4820, TCP 4830 stats) |
| `waterfall-bridge.yaml` | Web-tier Deployment + Service `waterfall-ui` (TCP 4780) |

## Apply order — this matters

`sigproc` resolves `waterfall-service.default.svc.cluster.local` **once** and
caches the result forever, retrying only while unresolved. Two consequences:

1. **The consumer must be Running and bound to UDP 4820 before the Service
   exists.** If the name resolves while nothing is listening, the first
   datagrams are lost and (because resolution succeeded) sigproc never retries
   the lookup.
2. **sigproc must already be running the framing fix.** The image it was built
   from before the fix panicked on the first packet it ever sent
   (`vita49.rs`: 8202-byte buffer, 8212-byte write). Since the pod resolves the
   Service name at startup, an early Service creation crash-loops it.

```bash
# 1. Build the waterfall image (after pushing to GitHub — kaniko clones master).
kubectl delete job kaniko-build-waterfall -n build --ignore-not-found
kubectl apply -f deploy/kaniko-job.yaml
kubectl -n build logs job/kaniko-build-waterfall -f

# 2. Make sure sigproc runs the FIXED image, then restart onto it.
#    Confirm the imageID digest changes off the pre-fix one (2a03f608...).
kubectl -n default rollout restart deploy/sigproc
kubectl -n default get pod -l app=sigproc -o jsonpath='{.items[0].status.containerStatuses[0].imageID}{"\n"}'

# 3. Bring up the middleware FIRST and wait for Ready (bound to 4820).
kubectl apply -f deploy/waterfall-consumer.yaml
kubectl -n default rollout status deploy/waterfall-consumer
kubectl -n default logs deploy/waterfall-consumer | head -40   # SPIKE lines

# 4. Now the web tier.
kubectl apply -f deploy/waterfall-bridge.yaml
kubectl -n default rollout status deploy/waterfall-bridge

# 5. ONLY NOW create the Service sigproc is waiting for.
kubectl apply -f deploy/waterfall-service.yaml   # the DNS name, applied last
kubectl -n default logs deploy/sigproc --tail=5       # expect: "forwarding started"

# 6. Watch the transport and open the waterfall.
kubectl -n default logs deploy/waterfall-consumer -f
kubectl -n default port-forward svc/waterfall-ui 4780:4780
#   → http://127.0.0.1:4780/
```

Because the consumer Deployment and its Service are split across two files, the apply order in step 3 (Deployment) and step 5 (Service) is exactly the ordering the one-shot DNS resolution requires.

## Scheduling constraint (for the infra agent)

Neither workload pins a node. Both Deployments express the requirement as
node affinity:

- `kubernetes.io/arch In [amd64]` — the image is amd64-only (there is no arm64
  build, and the two arm64 Pis plus jetson-1 are excluded automatically),
- `kubernetes.io/hostname NotIn [rpi-four-2, sab-laptop-1]` — keep the live RF
  path off the SDR node and off the control plane.

`sab-laptop-1` is amd64 and **untainted**, so arch matching alone would allow the
pod onto the control plane; the `NotIn` is what prevents it. The scheduler still
chooses freely between `med-laptop-1` and `med-laptop-2`.

If the cluster is instead configured with labels/taints (e.g. taint the control
plane, or label the med-laptops as the middleware pool), the `NotIn` term can be
dropped in favour of that cluster-level configuration.

## Dependencies already present in the cluster

| Resource | Namespace | Used for |
|---|---|---|
| `ghcr-push` secret | `build` | kaniko push credentials |
| `ghcr-pull` secret | `default` | Deployment image pulls |
| `kaniko-scratch` PVC (SMB-backed) | `build` | kaniko layer scratch, off the nodes' disks |

## Operational notes

- **Node placement.** The workloads are constrained to `amd64` and away from
  `rpi-four-2`/`sab-laptop-1`, with a *soft* preference for `med-laptop-2`.
  That preference exists because on 2026-09-20 `med-laptop-1` was flapping
  (`NodeNotReady` <-> `NodeReady` roughly once a minute) and every stall dropped
  live datagrams: the consumer held a steady 976.5 pkt/s with `gaps = 0` while
  the node was healthy, then accumulated tens of thousands of missing samples.
  After relocation to `med-laptop-2` it held 976.6 pkt/s with `gaps = 0` across
  repeated 60 s+ observations. The infra agent may replace the soft preference
  with node labels/taints.
- **SO_RCVBUF.** The consumer asks for 8 MiB and logs what the kernel granted.
  On this cluster the grant is 425984 bytes (~50 ms of the live ~8 MB/s stream)
  because `net.core.rmem_max` defaults to 212992. Two in-repo attempts to raise
  it both fail: `securityContext.sysctls` passes API validation but the kubelet
  rejects it with `SysctlForbidden`, and a privileged initContainer cannot write
  `/proc/sys/net` in this runtime. The cluster-level remedy is
  `kubelet --allowed-unsafe-sysctls=net.core.rmem_max` (infra agent).
  Measured impact of leaving it capped: none in steady state (976.6 pkt/s,
  `gaps = 0`, `stft_dropped = 0`); it is robustness margin for node stalls.
- **No synthetic fallback.** The bridge runs `--source=ingest`. If the RF feed
  stops, `/meta` reports `stale: true`, the rows stop, and the UI readout turns
  red. Verified by scaling the consumer to 0: `stale` became true and
  `rows_ingested` stayed frozen (470132 across two readings) — the bridge never
  invents rows. A waterfall that fabricates rows is worse than one that admits
  it is dead.
- **Live signal level.** Peak |sc16| measured between 46 and 7837 with mean
  ~10-12, i.e. mostly below the 8-bit LSB of 256. That is why the RF path keeps
  full 16-bit resolution (see `docs/plans/vita49-consumer.md`).
- **Drop counters, not guesses.** `/stats` on the consumer separates
  `parse_errors`, `stft_dropped` (analysis queue full — the socket is never
  blocked), `publish_dropped` (pacing buffer latest-wins) and
  `rows_lost_to_bridge` (HTTP failures). Gaps come from the VITA 49 sample
  counter, since `stream_id` and `packet_count` are both 0.
