# Playbook: FailedMount / FailedAttachVolume

Trigger: a lookout-watch inject with `"kind":"k8s-event"` and
`"reason":"FailedMount"` (or `FailedAttachVolume`), message like
`MountVolume.SetUp failed for volume "<vol>" : <detail>`; or pods stuck
`ContainerCreating`, surfacing as `pod.pending` findings with age. The
inject payload's `namespace`/`kind_of_object`/`name` name the pod.

## Steps

1. **One correlated read.**

   ```lookout
   lookout bundle --incident='{"kind":"k8s-event","reason":"FailedMount","namespace":"prod","kind_of_object":"Pod","name":"api-6d5f8c-x2v9k"}'
   ```

   In the `delta` section expect `pod.pending` (the pod never starts, so
   there are no logs — a `log.fetch_error` in the `logs` section is normal
   here, not a tool failure).

2. **A pod with no logs testifies through events.** Pull the deduped
   timeline for the mount failure's history — when it started, whether
   it is still recurring, and what else the kubelet reported around it:

   ```lookout
   lookout triage events --workload=Pod/prod/api-6d5f8c-x2v9k --since=1h
   ```

   The `FailedMount`/`FailedAttachVolume` entry's `count` and
   `first_seen`/`last_seen` date the problem; a `FailedScheduling` or
   volume-affinity entry alongside it reframes the diagnosis entirely.

3. **Ask what changed at onset.** Mounts break when the referenced
   object changed underneath the pod — a rotated Secret, a replaced
   ConfigMap, a node operation:

   ```lookout
   lookout triage changes Deployment/prod/api --since=30m
   ```

   A `change.secret`/`change.config` on a `relation=downstream` referent
   or a `change.node` right before `first_seen` is your prime suspect.
   (Without a sentinel store the summary says
   `source=live-approximation` and cannot see ConfigMap/Secret edits —
   step 5's `state edges` still catches the broken end state.)

4. **Identify the volume class.** The event message names the volume;
   match it against the pod's `volumes` field:

   ```lookout
   lookout triage spec Pod/prod/api-6d5f8c-x2v9k
   ```

   `volumes` renders as `name:source` — the source names a ConfigMap,
   Secret, or PVC (referents only, never payloads).

5. **ConfigMap/Secret volume** — the mount fails because the referent or a
   projected key is missing:

   ```lookout
   lookout state edges --workload=Deployment/prod/api
   ```

   `edge.missing_ref` (whole object gone) or `edge.missing_key` (key
   projected via `items:` gone) name the exact volume, key, and how many
   pods carry the broken reference. Fix the referent or the reference in
   GitOps; the kubelet retries the mount on its own.

6. **PVC volume** — check the claim, then the wider storage picture:

   ```lookout
   lookout triage spec pvc/prod/data-claim
   lookout health
   ```

   A `Pending` claim (no bound volume — provisioner or StorageClass
   problem) or `Lost` claim shows in `triage spec` as abnormal `phase`,
   and cluster-wide in `health`'s `storage` category (`pvc.pending` /
   `pvc.lost`). RWO multi-attach analysis (`state volumes` over
   `VolumeAttachment`) lands M5 — meanwhile
   `kubectl get volumeattachment` is the raw fallback for "is this volume
   still attached to another node".

7. **Verify.** After the fix:

   ```lookout
   lookout triage delta --namespace=prod --only=pods
   ```

   The `pod.pending` finding disappearing (summary `findings` drops) is
   the all-clear; the sentinel's `k8s-event-followup` inject confirms the
   mount events stopped.
