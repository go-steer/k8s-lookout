<!--
The checklist below is the process this repo has been holding in
people's heads. Delete any line that does not apply — an unchecked box
with a reason next to it is a fine answer.
-->

## What and why

<!--
What changed, and what makes it the right change rather than a
possible one. If it is a new or changed check, state the claim in one
sentence and name the legitimate look-alike you excluded.
-->

## Checklist

- [ ] `dev/tools/ci` passes locally
- [ ] Commits are signed off (`git commit -s`) — DCO
- [ ] Docs regenerated in order, if a command declaration changed:
      `dev/tools/gen-skill-refs`, then `dev/tools/gen-site-docs` **last**
- [ ] Goldens updated with `UPDATE_GOLDEN=1 go test ./...`, and the diff read
- [ ] `CHANGELOG.md` entry under `## [Unreleased]`, if user-visible
- [ ] Tests: an exact assertion per defect, **and** a healthy fixture
      proving `findings=0`
- [ ] RBAC updated (`deploy/12-clusterrole-watcher.yaml`, and
      `state.LoadClusterListRequirements`) if a new API resource is read
- [ ] `dev/tools/verify-helm-parity` passes, if anything under `deploy/`
      changed — the chart must still render the manifests exactly
- [ ] Scan membership decided — `stage1` or `excluded` with a reason
      (`TestScanCoversEveryRegisteredCommand` fails until you choose)

## Anything a reviewer should look at twice

<!--
A judgement call you are unsure about, a threshold whose default you
had to pick, a golden whose diff is larger than it looks. Naming it
here is faster than having it found.
-->

## Related

<!-- Closes #NNN, or a cross-reference. -->
