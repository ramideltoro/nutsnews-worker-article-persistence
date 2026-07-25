# nutsnews-worker-article-persistence

Deployable worker-uplift persistence service shell for NutsNews.

## Responsibility

Consume persistence jobs, verify final-shadow and backend Worker API compatibility, and provide the only service boundary allowed to assemble final worker-uplift article aggregates.

This bootstrap establishes the persistence service runtime, health/metrics surface, non-root container, strict TypeScript tooling, injectable dependency boundaries, and local verification around least-privilege access. Final article assembly and backend domain writes remain disabled until later persistence issues and backend-owned cutover.

## Runtime Surface

- Consumes the contracted `persistence` route and asserts the downstream `publication` route for future readiness events.
- Accepts only stage payloads whose contract consumer is `persistence`.
- Provides injectable persistence inbox, final-shadow transaction runner, approved stage-view reader, broker outbox, broker transport, backend Worker API client, clock, and work-handler boundaries.
- Gates readiness on final-shadow write scope, approved stage-view read scope, exact backend Worker API compatibility, shadow mode, and disabled production domain writes.
- Uses a dedicated persistence database role for final worker-uplift shadow aggregate/inbox/outbox writes and approved stage result view reads.
- Uses a separate scoped backend API identity for shadow and future domain commands; broad direct writes to `public` domain tables are not represented in this service.
- Contains no feed/page network access, AI generation, translation generation, publication decision logic, or legacy production writer logic.

## Configuration

The HTTP server exposes `/config-schema` with names, defaults, sensitivity, and production requirements only. Runtime config records dependency presence booleans and never retains database URLs, RabbitMQ URLs, backend API URLs, or API tokens.

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
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

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-persistence:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
