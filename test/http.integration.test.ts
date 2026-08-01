import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import {
  createPersistenceHttpServer,
  type PersistenceHttpServer
} from "../src/http.js";
import type {
  PersistenceReconciliationReport,
  PersistenceReconciler
} from "../src/reconciliation.js";
import { createPersistenceService } from "../src/service.js";
import { createPersistencePrometheusTelemetrySink } from "../src/telemetry.js";
import {
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery
} from "../src/test-doubles.js";

describe("createPersistenceHttpServer", () => {
  let server: PersistenceHttpServer | undefined;
  let service: ReturnType<typeof createPersistenceService> | undefined;

  afterEach(async () => {
    await server?.close();
    await service?.stop();
  });

  it("serves health, metrics, and config schema without exposing values", async () => {
    const config = loadPersistenceConfig({
      NUTSNEWS_PERSISTENCE_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      expectedActive: false
    });
    service = createPersistenceService({
      config,
      dependencies: createLocalPersistenceDependencies(),
      telemetry: metrics,
      metrics
    });
    server = createPersistenceHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await service.processDelivery(createMinimalPersistenceDelivery());
    await server.listen();

    const live = await fetch(server.url("/live"));
    const ready = await fetch(server.url("/ready"));
    const metricsResponse = await fetch(server.url("/metrics"));
    const schema = await fetch(server.url("/config-schema"));

    expect(live.status).toBe(200);
    expect(ready.status).toBe(200);
    const metricsBody = await metricsResponse.text();
    expect(metricsBody).toContain("nutsnews_worker_dependency_duration_seconds_bucket");
    expect(metricsBody).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(metricsBody).toContain("nutsnews_worker_uplift_stage_events_total");
    expect(metricsBody).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="persistence",le="30"} 1');
    expect(metricsBody).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-article-persistence"} 0');
    expect(metricsBody).toContain('probe="liveness"');
    expect(metricsBody).toContain('probe="startup"');
    expect(metricsBody).toContain('probe="readiness"');
    expect(metricsBody).toContain("nutsnews_worker_health_check_duration_seconds_bucket");
    expect(metricsBody).not.toContain("nutsnews_worker_consumer_active");
    const schemaBody = await schema.json() as {
      readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[];
    };

    expect(schemaBody.variables.some((variable) => variable.name === "NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schemaBody)).not.toContain("postgres://");
    expect(JSON.stringify(schemaBody)).not.toContain("amqp://");
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadPersistenceConfig({
      NUTSNEWS_PERSISTENCE_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });
    service = createPersistenceService({
      config,
      dependencies: createLocalPersistenceDependencies()
    });
    const reconciler: PersistenceReconciler = {
      name: "test-reconciler",
      reconcile: (request) => Promise.resolve({
        service: "persistence",
        mode: request.mode,
        status: "dry_run",
        requestedAt: "2026-07-23T00:00:00.000Z",
        maxItems: 1,
        minAgeSeconds: 900,
        selectedCount: 0,
        replayedCount: 0,
        failedClosedCount: 0,
        skippedCount: 0,
        writesPerformed: false,
        dryRun: true,
        productionVisibilityEnabled: false,
        legacyRuntimeRequired: false,
        protectedApplyRequired: true,
        candidates: [],
        errors: [],
        metrics: {
          candidateCount: 0,
          replayedCount: 0,
          failedClosedCount: 0,
          skippedCount: 0
        }
      } satisfies PersistenceReconciliationReport)
    };
    server = createPersistenceHttpServer({
      config,
      service,
      reconciler,
      reconciliationToken: "test-token"
    });

    await service.start();
    await server.listen();

    const unauthorized = await fetch(server.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(server.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });
  });
});
