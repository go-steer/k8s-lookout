# config-drift — someone edited it by hand

A **UAT fixture**, not a failure scenario. `lookout stab drift` reads
`managedFields` and reports spec fields owned by a manager that is *not*
the GitOps controller: the `kubectl edit` at 2am that the next
reconcile will silently revert, or has already been quietly fighting.

```sh
examples/scenarios/config-drift/inject
examples/scenarios/config-drift/verify
examples/scenarios/config-drift/revert
```

## Faking a GitOps controller, honestly

There is no Argo CD in the examples cluster and the fixture does not
install one. It does not need to: a server-side apply with
`--field-manager=argocd-controller` writes exactly the `managedFields`
entry Argo CD writes, and `managedFields` is all the check reads.

The fixture gets **its own namespace** because the manager is
auto-detected as the one owning a strict majority of spec leaf fields
across everything in scope. In `lookout-demo` the demo app is owned by
`kubectl-client-side-apply`, so `argocd-controller` could never reach
50% and the auto-detection path — the interesting one — would be
untestable there. `stab drift --namespace=lookout-demo` demonstrates
the other outcome:

```
detection=none detection_reason=not-a-gitops-manager candidate=kubectl-client-side-apply share=100%
```

which is the honest answer for a cluster with no GitOps controller: it
names the leading candidate and measures nothing rather than measuring
drift against a guess.

Everything runs at `replicas: 0`. Drift is a property of the spec's
ownership, not of any running pod, so the fixture schedules nothing.

## What it creates

| Deployment | Edited by | Field | Expected |
| --- | --- | --- | --- |
| `drift-hot` | `kubectl-edit` | container `image` | `drift.manual_edit` **critical** |
| `drift-cold` | `kubectl-patch` | `terminationGracePeriodSeconds` | `drift.manual_edit` **warning** |
| `drift-clean` | nobody | — | *nothing* |

The severity split is the point. An out-of-band image change forks what
is running from what is declared; a grace period does not. A check that
called both critical would be no more useful than `kubectl diff`.

`drift-clean` is the negative control. A check that reports drift on
everything is indistinguishable from one that reports drift correctly
unless something clean stays quiet.

## What to expect

```sh
lookout stab drift --namespace=lookout-uat-drift
```

- `manager=kubectl-edit operation=Update tool=kubectl` — the manager
  string is a **tool name, never a person**. Resolving the write to a
  human needs `--identity` and a cloud audit trail.
- `fields=spec.template.spec.containers[app].image field_count=1` —
  the paths, not a diff.
- The summary carries how the controller was resolved:
  `manager=argocd-controller detection=majority share=93%`. Passing
  `--manager=argocd-controller` gives the same findings with
  `detection=declared` — the escape hatch skips the bar, it does not
  change the answer.

The full contract is asserted by `examples/uat-cases/20-fixtures.sh`;
this scenario's `verify` is only a smoke check.

## Explore by hand

```sh
lookout stab drift --namespace=lookout-uat-drift --manager=argocd-controller
lookout stab drift --namespace=lookout-demo          # detection=none
kubectl -n lookout-uat-drift get deploy drift-hot -o jsonpath='{.metadata.managedFields[*].manager}'
```

Agent-harness prompt to try:
> Has anyone edited anything in lookout-uat-drift outside of GitOps?
