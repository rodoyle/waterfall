# Multicast Migration — Cilium overlay, LAN gateway, IGMP exports

**Plan file:** `docs/plans/multicast-migration.md`
**Status:** Draft — **four decisions taken 2026-11-25**, see "Decisions" below.
Not started. Phase 0 must complete before Phase 1 begins.
**Ownership (decided):** the cluster CNI migration belongs to **infra**. The UDP
gateway is best run as a **sigproc** service. **This repo** owns only its own
manifests (the waterfall backend/consumer) and coordinates with both; it does not
write into the other two work trees.

## Why this exists

The additive multicast path documented in `deploy/README.md` works, but the
frames arrive twice on the wire. The duplicate is **not** in the application:
`sigproc`'s collector emit record is 1:1, and a passive capture shows the two
copies are byte-identical with the same source MAC, same VRT counter, same TTL.
The mechanism is a **dual-homed sender**: `rpi-four-2` has `eth0`
(`192.168.0.55`) and `wlan0` (`192.168.0.54`) on the same L2, so the same frame
arrives on both footholds and an L2 device re-injects it onto the wire. Unicast
is unaffected and never duplicates.

This plan removes the dual-homed sender entirely by moving the multicast source
into the cluster overlay and egressing to the LAN through a single-homed gateway.

## Verified against the Cilium documentation

Checked before writing this up, because the plan hinges on them:

| Claim in the plan | Verified |
|---|---|
| Multicast is a supported Cilium feature | **Yes, but BETA** — "This is a beta feature" |
| Requires VXLAN mode | **Yes** — "assumes Cilium is configured with vxlan mode, which is required" |
| Kernel >= 6.0 AArch64, >= 5.10 AMD64 | **Exactly correct** |
| `cilium config set multicast-enabled true` | **Correct command** |
| `cilium multicast add --group-ip <G>` | **Correct** — adds all nodes as subscribers |
| `cilium multicast list subscriber --all` | **Correct** |
| IPsec must be disabled | **Correct** — "does not work with ipsec encryption" |

Two documented limitations the plan does not mention:

- **Group/subscriber state is imperative, not declarative.** "The operation needs
  to be done on each CiliumNode." There is no CRD to pin in Git. A node that
  joins or reboots loses its membership until the command is re-run.
- **Cilium multicast is not a multicast *router*.** It delivers within the
  overlay; it does not forward to a physical LAN. That is exactly why Phase 2
  needs a gateway, and the plan is right to separate them.
## Phase 0 — Identify the L2 copier (decided: this comes first)

The duplicate is a LAN device copying the frame, not an application defect. Name
the device before spending a CNI migration to work around it. A config change on
one device may fix the duplicate outright, and it establishes whether the
dual-homed sender is the sole trigger.

1. Use the frame-identity tooling (`scripts/diag/mcast_frame_identity.py` in
   `infra`) to capture on the wired segment and confirm the duplicate headers.
2. Walk the LAN for a device with both a wireless and a wired foothold in this
   L2: an AP in WDS/repeater mode, a mesh node with both backhauls live, or an
   extender with an Ethernet port. IGMP snooping and a querier should be present.
3. Identify the device by name, model and port.
4. **Gate (owner: infra):** the copier is identified by name, model and port, AND
   one of these is true:
   - **(a)** a change on that device makes `copies_per_packet` 1.0 at the wired
     receiver — then the duplicate is fixed at the device and Phases 1-3 are
     reassessed, possibly cancelled; or
   - **(b)** the device is proven NOT to be the sole trigger — then Phase 1
     proceeds.

   Both are measurable with `scripts/diag/mcast_duplicate_timing.py` and
   `scripts/diag/mcast_frame_identity.py`, already committed in `infra`.
   `copies_per_packet == 1.0` is the pass condition; nothing else counts.


## Phase 1 — Cilium multicast

Migrate K3s from Flannel to Cilium VXLAN, then prove pod-to-pod multicast before
adding gateway complexity.

1. Verify node prerequisites: kernel >= 6.0 on Raspberry Pi (AArch64), >= 5.10 on
   AMD64; IPsec encryption disabled on the multicast path.
2. Reconfigure K3s with `--flannel-backend=none --disable-network-policy`, install
   Cilium in VXLAN mode, confirm with `cilium status`.
3. `cilium config set multicast-enabled true`; register the group with
   `cilium multicast add --group-ip <OVERLAY_GROUP>`.
4. Confirm group and subscriber state on all nodes with
   `cilium multicast list subscriber --all`.
5. **Keep unicast alive through Phase 1 (decided).** Do NOT remove `hostNetwork`
   yet. Run the Cilium overlay alongside the existing hostNetwork unicast path
   until the overlay is proven byte-identical on the same stream. Removing
   `hostNetwork` from the capture pod and the backend is a separate, later step
   gated on that proof.
6. **Per-node multicast state must be a DaemonSet from Git, not a `cilium` CLI run
   (infra owns this, and it is a precondition for the cutover, not a follow-up).**
   Group membership is imperative and per-node with no CRD to pin. On this cluster
   that is not hypothetical: `rpi-four-2` went NotReady three times in one night,
   so a node reboot silently loses membership and no Git pin notices. Build a
   DaemonSet (or a boot Job) that reconciles the group and subscribers from a
   committed manifest, and prove it re-applies after a node restart.
7. **Gate:** the waterfall backend receives VITA49.2 data and context packets from
   the capture pod over the Cilium overlay; the DaemonSet has survived a node
   restart with membership intact; measure Pi egress bandwidth as subscribers are
   registered.

## Phase 2 — Multus and gateway

Deploy a dual-homed gateway bridging overlay multicast to the physical LAN and
proxying VITA49.2 control traffic.

> **Named exception — the gateway is dual-homed BY DESIGN.** It carries a Multus
> LAN attachment plus Cilium for the overlay. That is deliberate and correct.
> The accidental dual-home on `rpi-four-2` (eth0 `.55` and wlan0 `.54` on the same
> subnet) is what we are removing. Do not "fix" the gateway's dual-home later; it
> is the mechanism, not the defect.

1. Install Multus; add a `NetworkAttachmentDefinition` (macvlan or bridge) on the
   physical LAN NIC.
2. Deploy the gateway as single-replica `Recreate` to prevent duplicate LAN frames.
3. Data plane cluster→LAN: static kernel multicast routing via SMCRoute
   (`mgroup`/`mroute`), sender TTL >= 2.
4. Byte-preserving UDP relay as fallback if the eBPF datapath conflicts with
   kernel multicast forwarding. Join `<OVERLAY_GROUP>`, forward VRT unchanged to
   `<LAN_GROUP>`.
5. Control plane LAN→cluster: dedicated unicast UDP listener; forward commands to
   the capture pod's ClusterIP. **Security constraint:** the capture service's
   `POST /config` has no authentication today (recorded as a known limitation in
   sigproc's `docs/plans/sweep-controller.md`, whose non-goal is "no
   unauthenticated mutation endpoint"). The gateway must not widen that exposure
   or become a second unauthenticated mutation path. Authenticate at the gateway,
   or keep the control plane read-only until capture authenticates.
6. Single-controller lease: one writer for frequency mutation, everyone else
   receive-only.
7. CubicSDR device string:
   `addr=<LAN_GROUP>,port=<DATA_PORT>,ctrl_addr=<GATEWAY_LAN_IP>,ctrl_port=<CTRL_PORT>`.
8. **Gate:** CubicSDR receives live IQ; retune acknowledged; waterfall backend
   stays connected.

## Phase 3 — IGMP and exports

1. Static export on the gateway over an allowlist of `(overlay, LAN)` tuples.
2. Inspect LAN switch for IGMP snooping and a querier; prune correctly.
3. Expose gateway telemetry counters.
4. **Gate:** 10-minute sustained capture with both the cluster waterfall and the
   desktop CubicSDR subscribed; zero unattributed VRT gaps; disabling the gateway
   affects only desktop clients.

## Guardrails

- Do not use Cilium Egress Gateway — it is for unicast egress, not group forwarding.
- No bidirectional multicast. Only VITA49.2 unicast commands/replies cross LAN→cluster.
- Pin images and CNI manifests; provide a reproducible teardown/rollback script.
- Separate packet loss from buffer loss in logs; keep the "gaps vs drops" distinction
  the consumer already reports.

## Risks this plan carries — read before approving

1. ~~It is a cutover, not an additive change.~~ **Mitigated by decision:** the
   unicast path stays alive through Phase 1 and `hostNetwork` is removed only
   after the overlay is proven byte-identical. The additive guarantee is
   preserved, so a failed overlay cannot take the RF stream down.
2. **Beta feature on a live RF path.** Cilium's own docs say beta. **Accepted by
   decision**, on the condition that the unicast path is retained — a beta failure
   degrades to the current working path instead of an outage. If that condition is
   ever dropped, this acceptance lapses with it.
3. **The plan does not remove the L2 copier.** It removes the *dual-homed sender*,
   which is the likely trigger. That is not the same as proving the copier is
   gone. If the copier is an AP/WDS repeater that re-injects on its own, it will
   duplicate the gateway's frames too. Phase 3's gate would catch it, but the plan
   should name the device first.
4. **Imperative multicast state.** `cilium multicast add` is per-node and not
   declarative. A node reboot or join silently loses membership. The guardrail
   "pin in version control" cannot be met without a DaemonSet/Job that re-applies
   it on boot.
5. **macvlan is the wrong default for a dual-homed gateway.** macvlan on the host
   NIC prevents host↔pod traffic on that interface and has known multicast
   caveats. `ipvlan` or a bridge is usually the safer attachment for a multicast
   egress. The plan allows "macvlan or bridge" but does not say which to prefer.
6. **Kernel >= 6.0 on the Pi is a hard gate.** If `rpi-four-2` runs a 5.15 kernel,
   Cilium multicast cannot work on AArch64 at all and Phase 1 cannot start. This
   must be checked before anything else; it is not a "verify in passing" item.

## Decisions taken (2026-11-25)

1. **Rollback path: keep unicast alive through Phase 1.** The overlay runs
   alongside the existing hostNetwork unicast path until it is proven
   byte-identical. `hostNetwork` is removed only after that proof. The additive
   guarantee in `deploy/README.md` stays true.
2. **Order of work: identify the L2 copier first.** Phase 0 precedes Phase 1. If a
   change on the named device removes the duplicate, reassess whether the CNI
   migration is needed at all.
3. **Beta risk: accepted, conditional on the unicast path being retained.** A beta
   regression must degrade to the working path, not to an outage. If the unicast
   path is dropped, this acceptance lapses.
4. **Ownership: infra owns the CNI migration; the UDP gateway is a sigproc
   service; this repo owns its own manifests and coordinates.** No session writes
   into another's work tree.

## Still open

- **Phase 0 sign-off is infra plus the human.** Sigproc declined to sign its own
  prerequisite. If the named device turns out to be the sole cause, "duplicate
  fixed at the device" may make Phases 1-3 unnecessary — that decision needs the
  human, not a session.
- **Gateway placement is deferred to Phase 2 scheduling.** Sigproc will take the
  recommendation when Phase 2 is actually scheduled; deciding it now would be
  guessing at a design not yet reached. Sigproc would rather reuse this repo's
  VITA49 framing (`src/consumer.rs`, `src/vita49.rs`) than duplicate it.
- **Per-node multicast reproducibility is owned by infra** (offered and accepted),
  and must be built as a DaemonSet reconciled from Git, not a `cilium` CLI run.
  This is a precondition for the Phase 1 cutover, not a follow-up.

### Measurement precondition for sigproc's runbook

Sigproc's proof method reads per-source counter deltas across two `/stats`
samples, and its premise is that the multicast socket port and membership survive
a collector restart. With Cilium's per-node imperative membership, that premise is
false unless a re-apply runs first. When Phase 1 lands, infra must put the
re-apply step in the runbook and tell sigproc, who will add it to the
preconditions table in sigproc's `docs/plans/unicast-retirement.md`. Any
measurement taken after a pod restart without the re-apply is measuring something
unverified.

## Not yet known

- Kernel version on `rpi-four-2` (gates Phase 1).
- Which LAN device is the L2 copier (gates Phase 3's "zero unattributed gaps").
- Whether the desktop CubicSDR build supports the `ctrl_addr`/`ctrl_port` device
  string as written.
