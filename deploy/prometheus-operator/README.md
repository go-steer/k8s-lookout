# `deploy/prometheus-operator`: optional `ServiceMonitor` add-on

Optional Kustomize bundle installing `ServiceMonitor/lookout-watch` to scrape `Service/lookout-watch-metrics` (`:9090`, `30s` interval). Kept separate from [`deploy/base/`](../base/README.md) so clusters without `monitoring.coreos.com/v1` CRDs can apply the base sentinel cleanly.

```sh
kubectl apply -k deploy/prometheus-operator/
```

**Helm equivalent:** `--set serviceMonitor.enabled=true` (admit cross-namespace scrapers with `networkPolicy.extraIngressFrom`).
