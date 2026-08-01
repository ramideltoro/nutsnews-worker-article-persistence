# nutsnews-worker-article-persistence

Deployable worker-uplift persistence service shell for NutsNews.

## Responsibility

Consume persistence jobs, verify final-shadow and backend Worker API compatibility, and provide the only service boundary allowed to assemble final worker-uplift article aggregates.

This bootstrap establishes the persistence service runtime, health/metrics surface, non-root container, strict TypeScript tooling, injectable dependency boundaries, and local verification around least-privilege access. Final article assembly and backend domain writes remain disabled until later persistence issues and backend-owned cutover.

## Runtime Surface

- Consumes the contracted `persistence` route and asserts the downstream `publication` route for future readiness events.
- Accepts only stage payloads whose contract consumer is `persistence`.
- Pins immutable `@ramideltoro/nutsnews-worker-contracts@1.0.0` and `@ramideltoro/nutsnews-worker-runtime@1.0.0` releases and verifies the runtime's declared contracts dependency at startup and in tests.
- Provides injectable persistence inbox, final-shadow transaction runner, approved stage-view reader, broker outbox, broker transport, backend Worker API client, clock, and work-handler boundaries.
- Gates readiness on an active `persistence` main-queue consumer, final-shadow write scope, approved stage-view read scope, exact backend Worker API compatibility, shadow mode, and disabled production domain writes.
- Emits bounded structured events and Prometheus metrics when RabbitMQ cancels the consumer, drops its channel, or restores consumption.
- Emits exactly one bounded completion event per delivery for accepted, duplicate, invalid, retry, or DLQ outcomes. The Prometheus surface preserves generic runtime metrics and adds `nutsnews_worker_uplift_stage_events_total` plus a fixed-bucket `nutsnews_worker_uplift_stage_latency_seconds` histogram for Grafana stage SLOs. Runtime is the sole owner of canonical consumer and `nutsnews_worker_expected_active` families; expected activity derives from real production write ownership instead of a hardcoded shadow value. Accepted and duplicate terminal success update one monotonic `nutsnews_worker_last_success_timestamp_seconds` series. Message and correlation identifiers remain structured-log metadata only.
- Uses fixed-bucket seconds histograms for runtime and canonical stage duration; legacy `_duration_ms` summary families are not emitted.
- Exposes Runtime-owned one-hot `nutsnews_worker_health_probe` gauges from the first scrape plus bounded `nutsnews_worker_health_check` and `nutsnews_worker_health_check_duration_seconds` families for each real liveness, startup, and readiness evaluation. Runtime health families are emitted once, consumer loss makes readiness unhealthy, and generic dependency latency is recorded only when a real duration was measured.
- Seeds all six bounded `nutsnews_worker_uplift_stage_events_total` outcomes (`success`, `duplicate`, `invalid`, `retry`, `dlq`, and `failure`) so alerting can distinguish an idle outcome from an incomplete exporter.
- Treats JSON, Prometheus, health, and lifecycle telemetry as independently best effort: a throwing or rejecting sink cannot change acknowledgement, idempotency, retry, or DLQ behavior.
- Uses a dedicated persistence database role for final worker-uplift shadow aggregate/inbox/outbox writes and approved stage result view reads.
- Uses a separate scoped backend API identity for shadow and future domain commands; broad direct writes to `public` domain tables are not represented in this service.
- Contains no feed/page network access, AI generation, translation generation, publication decision logic, or legacy production writer logic.

## Final Materialization

The service consumes valid `persistenceCommand` messages whose entity reference declares `materializationKind: final_shadow_article`. The command carries durable canonicalizer, enrichment, approval, and translation stage-result references only; the service reads bodies through approved downstream views.

For each current article version, persistence builds the backend-approved `shadowAggregate` shape and calls only the scoped `uplift-record-shadow-aggregate` Worker API command in `backend_postgres_shadow` mode. The final shadow aggregate, audit metadata, and publication-readiness outbox command are committed transactionally. Publication readiness is published only after that commit and the broker receipt is recorded separately.

Exact replays return recorded success without duplicate aggregate/API/outbox side effects. Conflicting idempotency-key reuse, stale stage-result versions, and inconsistent stage references are quarantined with safe metadata only. Failed backend API calls or local transactions leave no accepted local aggregate/outbox delivery and remain retryable.

The production inbox uses fresh opaque claim tokens and compare-and-set completion, failure, and release transitions. A lost completion response is reconciled without downgrading a committed record. PostgreSQL `clock_timestamp()` issues and evaluates processing leases, with a hard five-minute ceiling; caller clocks cannot create, extend, or reclaim ownership. Long-running handlers renew the live token with a database-time CAS every minute and synchronously before their final transition. Rejected or uncertain renewal aborts cooperative work immediately and fails closed without a stale completion, failure, or release mutation. Database operations have ten-second connection/query/statement deadlines, and backend API and broker operations remain bounded below the lease. Legacy missing or malformed controls receive a fresh server-timed grace lease before reclaim, and schema-default `received` rows enter processing through an atomic transition. Claim tokens remain database control metadata and are never metric or Loki labels.

## Replay And Fault Safety

The local safety suite injects transient backend API failures, transaction write/commit failures, broker publish failures, outbox receipt failures, permanent permission-style failures, concurrent duplicate deliveries, and historical replay batches. Transient faults retry without duplicate visible aggregates. Permanent faults DLQ with safe diagnostics. If a crash occurs after final-shadow commit but before broker/outbox confirmation, replay republishes only the unconfirmed publication-readiness command.

## Feed-Health Projection

Feed-health projection commands use the same persistence route and declare `projectionKind: feed_health`. The projection path is routed separately from final materialization and writes only shadow comparison state. It maps deterministic stage outcomes into legacy-compatible `feed_health` and `feed_quality_scores` rows, including status, redacted error class, last/total item/image/accepted/rejected counts, duplicate rates, quality rates, latency-derived safety metadata, and backoff recommendation.

Duplicate, late, out-of-order, and replayed projection events are handled with projection-specific idempotency/version keys. Raw feed/article bodies, model output, credentials, and secrets are rejected. Projection failures do not block or duplicate final article materialization, and shadow mode never writes production feed-health state.

## Configuration

The HTTP server exposes `/config-schema` with names, defaults, sensitivity, and production requirements only. Runtime config records dependency presence booleans and never retains database URLs, RabbitMQ URLs, backend API URLs, or API tokens. `NUTSNEWS_ENVIRONMENT=production` requires production dependency mode, and immutable dependency discriminants prevent an injected in-memory adapter or state store from bypassing that guard.

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_PERSISTENCE_BUILD_REVISION` | `development` | required lowercase 40-character Git SHA | no |
| `NUTSNEWS_PERSISTENCE_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_PERSISTENCE_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL` | unset | required | yes |
| `NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN` | unset | required | yes |
| `NUTSNEWS_PERSISTENCE_BACKEND_API_COMPATIBILITY_VERSION` | `worker-api-v1` | optional | no |
| `NUTSNEWS_PERSISTENCE_SHADOW_SCHEMA_VERSION` | `worker-uplift-shadow-v1` | optional | no |
| `NUTSNEWS_PERSISTENCE_DATABASE_ROLE` | `nutsnews_worker_persistence` | optional | no |
| `NUTSNEWS_PERSISTENCE_BACKEND_API_IDENTITY` | `worker-uplift-persistence` | optional | no |
| `NUTSNEWS_PERSISTENCE_CONCURRENCY` | `2` | optional | no |
| `NUTSNEWS_PERSISTENCE_PREFETCH` | `4` | optional | no |
| `NUTSNEWS_PERSISTENCE_SHADOW_MODE` | `true` | must remain true here | no |
| `NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED` | `false` | must remain false here | no |

## Local Verification

```sh
npm ci
npm run ci
NODE_AUTH_TOKEN=<github-packages-token> npm run container:build
```

`npm run ci` runs lint, typecheck, unit tests, integration tests, build, SBOM generation, and a production dependency audit.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-persistence:${GITHUB_SHA}`. A push to `main` builds that traceability tag with SBOM and max-mode provenance and signs it keylessly. This repository does not publish an npm package and is deployable only through backend-owned infrastructure.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments must resolve the post-merge SHA tag and pin the resulting immutable GHCR digest (`ghcr.io/ramideltoro/nutsnews-worker-article-persistence@sha256:...`). Before changing the backend pin, retain the successful `main` publication run, resolved manifest digest, verified keyless signature, provenance/SBOM, and matching OCI source/revision labels. The only intended production image consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
