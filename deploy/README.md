# Deploying orbweaver

Three tiers, all in namespace `default`:

```
sigproc (rpi-four-2, USRP B210, untouched)
   │ UDP 8212 B datagrams -> waterfall-service.default.svc:4820
   ▼
orbweaver-consumer            [middleware: VITA49 -> sc16 -> STFT -> paced rows]
   │ HTTP POST /ingest -> orbweaver-ui.default.svc:4780
   ▼
orbweaver-bridge              [web tier: /, /ws, /chunks, /meta, /stats]
   ▲
   │ traefik Ingress, host orbweaver.apps.home.arpa (HTTP, port 80)
   └── browser:  http://orbweaver.apps.home.arpa
```

Naming: cluster objects and the public host are **orbweaver**; the GitHub repo,
the Rust crate, the binaries and the image stay **waterfall**
(`ghcr.io/rodoyle/waterfall:latest`), so the build pipeline is unaffected.

## Layout

| Path | Purpose |
|---|---|
| `kustomize/build/` | kaniko Job (amd64 builder) → pushes `ghcr.io/rodoyle/waterfall:latest` |
| `kustomize/base/` | canonical Deployments + Services + Ingress |
| `kustomize/overlays/default/` | namespace, image tag — the single app deploy root (includes the compat Service) |
| `kustomize/compat/` | the legacy `waterfall-service`, frozen; included by the overlay and applicable standalone |
| `Dockerfile` | the image the kaniko Job builds (referenced as `deploy/Dockerfile`) |

## Deploy

```bash
# 1. Build the image (only needed when code changes; the tag is :latest).
kubectl delete job kaniko-build-waterfall -n build --ignore-not-found
kubectl apply -k deploy/kustomize/build
kubectl -n build logs job/kaniko-build-waterfall -f

# 2. Apply the whole stack — canonical objects AND the frozen legacy Service,
#    in one reproducible path (six objects):
kubectl apply -k deploy/kustomize/overlays/default

# Optional: re-assert only the legacy Service, without touching the canonical
# objects (a no-op against the live cluster):
kubectl apply -k deploy/kustomize/compat
```

## The legacy Service — read this before touching anything

sigproc's ConfigMap contains

```toml
vita49_dest_host = "waterfall-service.default.svc.cluster.local"
vita49_dest_port = 4820
```

and its forwarder resolves that name **once** at startup, caching the address
forever; it retries only while unresolved. It cached `10.43.78.205`. So:

- **Never delete or recreate `waterfall-service`.** A new ClusterIP would be
  handed out, sigproc would keep sending to the old one, and the live RF stream
  would die until sigproc is restarted (it belongs to another repo and session).
- `kustomize/compat/waterfall-service.yaml` captures the live object verbatim,
  including a pinned `clusterIP: 10.43.78.205`, so recreating it reproduces the
  same address. Applying it is a no-op on an existing cluster.
- The renamed `orbweaver-consumer` pods keep the compatibility label
  `app: waterfall-consumer` **precisely so this Service keeps selecting them**.
  Do not remove that label while the legacy Service exists.

### Retirement checklist (when sigproc's owner migrates the ConfigMap)

1. Ask the sigproc owner to set `vita49_dest_host = "orbweaver-service.default.svc.cluster.local"`
   and roll the Deployment (a brief RF gap; their call).
2. Confirm sigproc logs `resolved to … orbweaver-service` and its drop counter
   stops rising.
3. Delete the compat resource: drop `../../compat/waterfall-service.yaml` from
   `kustomize/overlays/default/kustomization.yaml`, then
   `kubectl delete -k deploy/kustomize/compat`.
4. Drop the `app: waterfall-consumer` label from `kustomize/base/consumer.yaml`
   and re-apply the overlay.
5. Delete `kustomize/compat/` from the repo.

## Zero-interruption apply order

The RF path must never point at a Service with no ready endpoints, so:

1. Create the canonical objects first (`orbweaver-service`, `orbweaver-ui`, both
   Deployments, the Ingress). At this point the old pods still serve.
2. Wait for the new pods to be Ready — the legacy Service matches the new
   consumer pod through the compatibility label, and the old pod still matches
   too, so it never has zero endpoints.
3. Roll over: delete the superseded `waterfall-consumer` / `waterfall-bridge`
   Deployments and the `waterfall-ui` Service. Never `waterfall-service`.
4. Verify: sigproc's drop counter is unchanged, the consumer still reports
   ~976.6 pkt/s with gaps ≈ 0, and `http://orbweaver.apps.home.arpa/` renders.

## Ingress

`orbweaver.apps.home.arpa` on the cluster's traefik ingress controller (default
ingress class), HTTP only. LAN DNS for `*.apps.home.arpa` is served by
`coredns-lan` in the `lan-dns` namespace, so no per-device hosts entry is
needed. One route (`path: /`) covers the static UI, the WebSocket and the REST
endpoints; traefik upgrades websockets on an ordinary HTTP route.

TLS is deliberately absent: the stack is LAN-only and read-only (a spectrum
display plus a WebSocket), so there is no credential to protect. Adding it later
needs only a `tls:` block in `kustomize/base/ingress.yaml`.

## Placement

Neither workload pins a node. Both express the requirement as node affinity:

- `kubernetes.io/arch In [amd64]` — the image is amd64-only, so the arm64 Pi
  nodes and jetson-1 can never match;
- `kubernetes.io/hostname NotIn [rpi-four-2, sab-laptop-1]` — keep the live RF
  path off the SDR node and off the control plane;
- a **soft** preference for `med-laptop-2`, because `med-laptop-1` flaps
  (`NodeNotReady` ↔ `NodeReady` roughly once a minute) and every stall dropped
  live datagrams: the consumer held 976.5 pkt/s with zero gaps while that node
  was healthy, then accumulated tens of thousands of missing samples.

The infra repo may replace the soft preference with labels/taints.

## Operational notes

- **SO_RCVBUF.** The consumer asks for 8 MiB and logs what the kernel granted.
  On this cluster the grant is 425984 bytes (~50 ms of the live ~8 MB/s stream)
  because `net.core.rmem_max` defaults to 212992. Both in-repo remedies fail:
  `securityContext.sysctls` passes API validation but the kubelet rejects it with
  `SysctlForbidden`, and a privileged initContainer cannot write `/proc/sys/net`
  in this runtime. The cluster-level fix is
  `kubelet --allowed-unsafe-sysctls=net.core.rmem_max`. Measured impact of
  leaving it capped: **none** in steady state (976.6 pkt/s, gaps = 0,
  `stft_dropped` = 0); it is robustness margin for node stalls.
- **No synthetic fallback.** The bridge runs `--source=ingest`. If the RF feed
  stops, `/meta` reports `stale: true`, rows stop, and the UI readout turns red.
  Verified by scaling the consumer to 0: `stale` became true and `rows_ingested`
  stayed frozen — the bridge never invents rows.
- **Drop counters, not guesses.** `/stats` on the consumer separates
  `parse_errors`, `stft_dropped` (analysis queue full — the socket is never
  blocked), `publish_dropped` (pacing buffer, latest-wins) and
  `rows_lost_to_bridge` (HTTP failures). Gaps come from the VITA 49 sample
  counter, since `stream_id` and `packet_count` are both 0.
- **Verification helpers.** `bin/local_smoke.sh` runs the whole pipeline locally
  against a synthetic VITA49 sender; `bin/live_ui_check.cjs` drives a headless
  browser against a live feed and writes a screenshot artifact.

## Known follow-ups (not done here)

- The container runs as root (no `USER` in the Dockerfile); adding a non-root
  user would need a rebuild plus a check of the `/tmp` fixture write and static
  file permissions.
- The image tag is `latest`. Pinning a digest in
  `kustomize/overlays/default/kustomization.yaml` would make rollouts frozen.
- The infra repo (`src/infra`) was read-only for this work and is untouched by
  it; it owns the reusable arm64 build base, and this repo owns its own build
  job.