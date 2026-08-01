import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_DURATION_HISTOGRAM_BUCKETS_SECONDS,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import { createPersistenceService } from "../src/service.js";
import {
  PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS,
  PERSISTENCE_STAGE_METRIC_OUTCOMES,
  createPersistencePrometheusTelemetrySink,
  type PersistencePrometheusTelemetrySink
} from "../src/telemetry.js";
import {
  InMemoryPersistenceInboxStore,
  LocalBrokerTransport,
  LocalPersistenceWorkHandler,
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery,
  createMinimalPersistenceEnvelope,
  createMinimalPersistencePayload
} from "../src/test-doubles.js";

describe("persistence Prometheus telemetry", () => {
  it("exports a fixed zero-valued canonical stage family before the first delivery", async () => {
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "shadow",
        host: "persistence-zero-series"
      }
    });
    const before = metrics.collect();
    const beforeStageEvents = metricSeries(before, "nutsnews_worker_uplift_stage_events_total");
    const beforeHistogram = metricSeries(before, "nutsnews_worker_uplift_stage_latency_seconds");

    expect(beforeStageEvents).toHaveLength(PERSISTENCE_STAGE_METRIC_OUTCOMES.length);
    expect(beforeHistogram).toHaveLength(PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS.length + 3);

    for (const outcome of PERSISTENCE_STAGE_METRIC_OUTCOMES) {
      expect(beforeStageEvents).toContain(`nutsnews_worker_uplift_stage_events_total{environment="shadow",service="persistence",outcome="${outcome}"} 0`);
    }

    for (const boundary of PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS) {
      expect(before).toContain(`nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="shadow",service="persistence",le="${String(boundary)}"} 0`);
    }

    expect(before).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="shadow",service="persistence",le="+Inf"} 0');
    expect(before).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="shadow",service="persistence"} 0');
    expect(before).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="shadow",service="persistence"} 0');

    await metrics.emit(completion("runtime.message.accepted", "success", 5));

    expect(metricSeries(metrics.collect(), "nutsnews_worker_uplift_stage_events_total")).toHaveLength(beforeStageEvents.length);
    expect(metricSeries(metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds")).toHaveLength(beforeHistogram.length);
  });

  it("records each completion once with bounded outcomes and complete fixed-bucket histograms", async () => {
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "production",
        host: "backend.nutsnews.com"
      }
    });
    const completions = [
      completion("runtime.message.accepted", "success", 5),
      completion("runtime.message.duplicate", "duplicate", 50),
      completion("runtime.message.invalid", "failure", 30_000),
      completion("runtime.message.retry", "retry", 30_001),
      completion("runtime.message.dlq", "dlq", 301_000)
    ] satisfies readonly RuntimeTelemetryEvent[];

    for (const event of completions) {
      await metrics.emit(started());
      await metrics.emit(event);
    }
    await metrics.emit({
      ...completion("runtime.message.accepted", "success", 1),
      stage: "publication"
    });
    await metrics.emit({
      ...completion("runtime.message.accepted", "success", 1),
      queue: "nutsnews.worker.persistence.v1.retry-30s"
    });

    const output = metrics.collect();
    const stageEvents = output
      .split("\n")
      .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{"));

    expect(stageEvents).toHaveLength(PERSISTENCE_STAGE_METRIC_OUTCOMES.length);

    for (const outcome of PERSISTENCE_STAGE_METRIC_OUTCOMES) {
      const expected = outcome === "failure" ? "0" : "1";
      expect(stageEvents).toContain(`nutsnews_worker_uplift_stage_events_total{environment="production",service="persistence",outcome="${outcome}"} ${expected}`);
    }

    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="0.01"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="0.005"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="0.025"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="0.05"} 2');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="30"} 3');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="60"} 4');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="300"} 4');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="production",service="persistence",le="+Inf"} 5');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="production",service="persistence"} 5');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="production",service="persistence"} 361.056');
    expect(output).toContain("nutsnews_worker_messages_total");
    expect(output).not.toContain("message-id-cardinality-probe");
    expect(output).not.toContain("idempotency-cardinality-probe");
    expect(output).not.toContain("correlation-cardinality-probe");

    for (const series of stageEvents) {
      const labelNames = [
        ...series.matchAll(/([a-z_]+)="/gu)
      ].map((match) => match[1]);

      expect(labelNames).toEqual([
        "environment",
        "service",
        "outcome"
      ]);
    }

    await metrics.emit({
      name: "runtime.message.dlq",
      level: "warn",
      at: "2026-07-31T00:00:01.000Z",
      stage: "persistence",
      queue: "nutsnews.worker.persistence.v1",
      outcome: "dlq"
    });
    const withoutDuration = metrics.collect();
    expect(withoutDuration).toContain('nutsnews_worker_uplift_stage_events_total{environment="production",service="persistence",outcome="dlq"} 2');
    expect(withoutDuration).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="production",service="persistence"} 5');
  });

  it("exports truthful liveness, startup, and readiness states before the first scrape and across lifecycle changes", async () => {
    const context = createProbeContext();

    expectProbe(context.metrics.collect(), "liveness", "ok");
    expectProbe(context.metrics.collect(), "startup", "unhealthy");
    expectProbe(context.metrics.collect(), "readiness", "unhealthy");
    expect(context.metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.start();
    expectProbe(context.metrics.collect(), "startup", "ok");
    expectProbe(context.metrics.collect(), "readiness", "ok");
    await context.service.health.liveness();
    await context.service.health.startup();
    await context.service.health.readiness();

    expectProbe(context.metrics.collect(), "liveness", "ok");
    expectProbe(context.metrics.collect(), "startup", "ok");
    expectProbe(context.metrics.collect(), "readiness", "ok");

    await context.metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "error",
      at: "2026-07-31T00:00:01.000Z",
      stage: "persistence",
      queue: "nutsnews.worker.persistence.v1",
      outcome: "channel-dropped",
      attributes: {
        activeConsumers: 0,
        state: "channel-dropped"
      }
    });
    expectProbe(context.metrics.collect(), "readiness", "unhealthy");
    expectHealthCheck(context.metrics.collect(), "readiness", "rabbitmq-consumer", "unhealthy");
    expect(metricSeries(context.metrics.collect(), "nutsnews_worker_consumers")).toEqual([
      expect.stringMatching(/nutsnews_worker_consumers\{.*queue="nutsnews\.worker\.persistence\.v1".*\} 0$/u)
    ]);
    expect(context.metrics.collect()).not.toContain("nutsnews_worker_consumer_active");

    await context.metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "info",
      at: "2026-07-31T00:00:02.000Z",
      stage: "persistence",
      queue: "nutsnews.worker.persistence.v1",
      outcome: "active",
      attributes: {
        activeConsumers: 1,
        state: "active"
      }
    });
    expectProbe(context.metrics.collect(), "readiness", "unhealthy");
    expect(metricSeries(context.metrics.collect(), "nutsnews_worker_consumers")).toEqual([
      expect.stringMatching(/nutsnews_worker_consumers\{.*queue="nutsnews\.worker\.persistence\.v1".*\} 1$/u)
    ]);

    await context.service.health.readiness();
    expectProbe(context.metrics.collect(), "readiness", "ok");

    await context.service.consumer?.cancel();
    expectProbe(context.metrics.collect(), "readiness", "unhealthy");
    expectHealthCheck(context.metrics.collect(), "readiness", "rabbitmq-consumer", "unhealthy");

    await context.service.stop();
    expectProbe(context.metrics.collect(), "liveness", "ok");
    expectProbe(context.metrics.collect(), "startup", "unhealthy");
    expectProbe(context.metrics.collect(), "readiness", "unhealthy");
  });

  it("does not manufacture dependency latency when no duration was measured", async () => {
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "test",
        host: "persistence-test"
      }
    });

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-31T00:00:00.000Z",
      stage: "persistence",
      queue: "nutsnews.worker.persistence.v1",
      outcome: "success",
      attributes: {
        dependency: "persistence-shell"
      }
    });

    expect(metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");
  });

  it("keeps rejecting telemetry and metric sinks from changing exact-one delivery outcomes", async () => {
    const config = loadPersistenceConfig({
      HOSTNAME: "persistence-rejecting-telemetry-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });
    const workHandler = new LocalPersistenceWorkHandler();
    const dependencies = createLocalPersistenceDependencies({
      workHandler
    });
    const events: RuntimeTelemetryEvent[] = [];
    const rejectingSink: PersistencePrometheusTelemetrySink = {
      allowedLabels: RUNTIME_ALLOWED_METRIC_LABELS,
      emit: (event) => {
        events.push(event);
        return Promise.reject(new Error("telemetry unavailable"));
      },
      collect: () => {
        throw new Error("metrics unavailable");
      },
      setInFlight: () => {
        throw new Error("metrics unavailable");
      },
      setShutdownDraining: () => {
        throw new Error("metrics unavailable");
      },
      setHealthProbe: () => {
        throw new Error("metrics unavailable");
      }
    };
    const service = createPersistenceService({
      config,
      dependencies,
      telemetry: rejectingSink,
      metrics: rejectingSink
    });
    const broker = dependencies.brokerTransport as LocalBrokerTransport;

    await expect(service.start()).resolves.toBeUndefined();
    events.length = 0;

    const accepted = persistenceDelivery(1);
    await expect(broker.deliverPersistence(accepted)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(broker.deliverPersistence(accepted)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    await expect(broker.deliverPersistence(persistenceDelivery(2, {
      schemaVersion: 99
    }))).resolves.toMatchObject({
      action: "dlq",
      reason: "invalid-payload"
    });

    workHandler.result = {
      status: "retry",
      reason: "transient-persistence-error",
      retryAfterMs: 2_000
    };
    await expect(broker.deliverPersistence(persistenceDelivery(3))).resolves.toMatchObject({
      action: "retry",
      reason: "transient-persistence-error"
    });

    workHandler.result = {
      status: "terminal-failure",
      reason: "terminal-persistence-error"
    };
    await expect(broker.deliverPersistence(persistenceDelivery(4))).resolves.toMatchObject({
      action: "dlq",
      reason: "terminal-persistence-error"
    });

    const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => event.name !== "runtime.message.started")).toHaveLength(5);
    expect(workHandler.handled).toHaveLength(3);
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it("contains inbox operation failures inside one retry or DLQ completion and balances in-flight work", async () => {
    const config = loadPersistenceConfig({
      HOSTNAME: "persistence-inbox-failure-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });
    const workHandler = new LocalPersistenceWorkHandler();
    const dependencies = createLocalPersistenceDependencies({
      workHandler
    });
    const inbox = dependencies.inboxStore as InMemoryPersistenceInboxStore;
    const broker = dependencies.brokerTransport as LocalBrokerTransport;
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const events: RuntimeTelemetryEvent[] = [];
    const service = createPersistenceService({
      config,
      dependencies,
      telemetry: {
        emit: async (event) => {
          events.push(event);
          await metrics.emit(event);
        }
      },
      metrics
    });

    await service.start();
    events.length = 0;

    try {
      vi.spyOn(inbox, "verifyPayloadFingerprint").mockRejectedValueOnce(new Error("fingerprint store unavailable"));
      await expect(broker.deliverPersistence(persistenceDelivery(10))).resolves.toMatchObject({
        action: "retry",
        reason: "payload-fingerprint-verification-error"
      });

      vi.spyOn(inbox, "claim").mockRejectedValueOnce(new Error("claim store unavailable"));
      await expect(broker.deliverPersistence(persistenceDelivery(11))).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-claim-error"
      });

      vi.spyOn(inbox, "markCompleted").mockRejectedValueOnce(new Error("completion store unavailable"));
      await expect(broker.deliverPersistence(persistenceDelivery(12))).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-completion-error"
      });

      workHandler.result = {
        status: "retry",
        reason: "persistence-handler-retry"
      };
      vi.spyOn(inbox, "markFailed").mockRejectedValueOnce(new Error("failure store unavailable"));
      await expect(broker.deliverPersistence(persistenceDelivery(13))).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-failure-record-error"
      });

      const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));
      expect(messageEvents.map((event) => event.name)).toEqual([
        "runtime.message.started",
        "runtime.message.retry",
        "runtime.message.started",
        "runtime.message.retry",
        "runtime.message.started",
        "runtime.message.retry",
        "runtime.message.started",
        "runtime.message.retry"
      ]);
      expect(messageEvents.filter((event) => event.name !== "runtime.message.started")).toHaveLength(4);
      expect(workHandler.handled).toHaveLength(2);

      const inFlightSeries = metrics.collect()
        .split("\n")
        .find((line) => line.startsWith("nutsnews_worker_inflight{") && line.includes('queue="nutsnews.worker.persistence.v1"'));
      expect(inFlightSeries).toBeDefined();
      expect(inFlightSeries?.endsWith("} 0")).toBe(true);
      expect(metrics.collect()).toContain('nutsnews_worker_uplift_stage_events_total{environment="test",service="persistence",outcome="retry"} 4');
    } finally {
      await service.stop();
    }
  });

  it("exports each Runtime-owned health check and duration family exactly once", async () => {
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "production",
        host: "backend.nutsnews.com"
      },
      cardinality: {
        healthChecks: [
          "persistence-inbox"
        ]
      }
    });

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness",
        checks: [
          {
            name: "persistence-inbox",
            status: "ok",
            critical: true,
            durationMs: 5
          }
        ]
      }
    });

    const output = metrics.collect();
    const lines = output.split("\n");
    const healthCheckSamples = lines.filter((line) => line.startsWith("nutsnews_worker_health_check{"));
    const durationSamples = metricSeries(output, "nutsnews_worker_health_check_duration_seconds");

    expect(lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_check "))).toHaveLength(1);
    expect(lines.filter((line) => line === "# TYPE nutsnews_worker_health_check gauge")).toHaveLength(1);
    expect(healthCheckSamples).toHaveLength(3);
    expect(new Set(healthCheckSamples.map(seriesWithoutValue)).size).toBe(healthCheckSamples.length);
    expect(healthCheckSamples).toContain('nutsnews_worker_health_check{environment="production",host="backend.nutsnews.com",service="nutsnews-worker-article-persistence",version="0.1.0",outcome="ok",probe="readiness",check="persistence-inbox"} 1');
    expect(lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_check_duration_seconds "))).toHaveLength(1);
    expect(lines.filter((line) => line === "# TYPE nutsnews_worker_health_check_duration_seconds histogram")).toHaveLength(1);
    expect(durationSamples).toHaveLength(RUNTIME_DURATION_HISTOGRAM_BUCKETS_SECONDS.length + 3);
    expect(new Set(durationSamples.map(seriesWithoutValue)).size).toBe(durationSamples.length);
    expect(durationSamples).toContain('nutsnews_worker_health_check_duration_seconds_bucket{environment="production",host="backend.nutsnews.com",service="nutsnews-worker-article-persistence",version="0.1.0",probe="readiness",check="persistence-inbox",le="0.005"} 1');
    expect(durationSamples).toContain('nutsnews_worker_health_check_duration_seconds_count{environment="production",host="backend.nutsnews.com",service="nutsnews-worker-article-persistence",version="0.1.0",probe="readiness",check="persistence-inbox"} 1');
  });

  it("updates Runtime last-success time for accepted and duplicate outcomes without moving backward", async () => {
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "production",
        host: "backend.nutsnews.com"
      }
    });
    const firstSuccessAt = "2026-08-01T00:00:10.900Z";
    const olderDuplicateAt = "2026-08-01T00:00:05.000Z";
    const latestDuplicateAt = "2026-08-01T00:00:30.100Z";

    expect(metricSeries(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toHaveLength(0);

    await metrics.emit({
      ...completion("runtime.message.accepted", "success", 5),
      at: firstSuccessAt
    });
    expect(metricSeries(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toEqual([
      `nutsnews_worker_last_success_timestamp_seconds{environment="production",service="nutsnews-worker-article-persistence"} ${String(Math.floor(Date.parse(firstSuccessAt) / 1_000))}`
    ]);

    await metrics.emit({
      ...completion("runtime.message.duplicate", "duplicate", 5),
      at: olderDuplicateAt
    });
    expect(metricSeries(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toEqual([
      `nutsnews_worker_last_success_timestamp_seconds{environment="production",service="nutsnews-worker-article-persistence"} ${String(Math.floor(Date.parse(firstSuccessAt) / 1_000))}`
    ]);

    await metrics.emit({
      ...completion("runtime.message.retry", "retry", 5),
      at: "2026-08-01T00:01:00.000Z"
    });
    await metrics.emit({
      ...completion("runtime.message.duplicate", "duplicate", 5),
      at: latestDuplicateAt
    });
    expect(metricSeries(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toEqual([
      `nutsnews_worker_last_success_timestamp_seconds{environment="production",service="nutsnews-worker-article-persistence"} ${String(Math.floor(Date.parse(latestDuplicateAt) / 1_000))}`
    ]);
  });
});

function metricSeries(output: string, family: string): string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith(family) && !line.startsWith(`# ${family}`));
}

function seriesWithoutValue(series: string): string {
  return series.slice(0, series.lastIndexOf(" "));
}

function createProbeContext() {
  const config = loadPersistenceConfig({
    HOSTNAME: "persistence-probe-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalPersistenceDependencies();
  const metrics = createPersistencePrometheusTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const service = createPersistenceService({
    config,
    dependencies,
    telemetry: metrics,
    metrics
  });

  return {
    metrics,
    service
  };
}

function expectProbe(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  expected: "ok" | "degraded" | "unhealthy"
): void {
  const outcomes = [
    "ok",
    "degraded",
    "unhealthy"
  ] as const;
  const probeSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_health_probe{")
    && line.includes('environment="test"')
    && line.includes('service="nutsnews-worker-article-persistence"')
    && line.includes(`probe="${probe}"`));
  const values = outcomes.map((outcome) => probeSamples.some((line) => line.includes(`outcome="${outcome}"`)
    && line.endsWith(" 1")) ? 1 : 0);

  expect(probeSamples).toHaveLength(3);
  expect(values.reduce<number>((sum, value) => sum + value, 0)).toBe(1);
  expect(values[outcomes.indexOf(expected)]).toBe(1);
}

function expectHealthCheck(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  check: string,
  expected: "ok" | "degraded" | "unhealthy"
): void {
  const samples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_health_check{")
    && line.includes(`probe="${probe}"`)
    && line.includes(`check="${check}"`));

  expect(samples).toHaveLength(3);
  expect(samples.filter((line) => line.includes(`outcome="${expected}"`) && line.endsWith(" 1"))).toHaveLength(1);
}

function persistenceDelivery(
  sequence: number,
  payloadOverrides: Readonly<Record<string, unknown>> = {}
): ReturnType<typeof createMinimalPersistenceDelivery> {
  const suffix = sequence.toString(16).padStart(2, "0");
  const idempotencyKey = `persistence:telemetry-test:${String(sequence)}`;

  return {
    ...createMinimalPersistenceDelivery(),
    envelope: createMinimalPersistenceEnvelope({
      messageId: `018f1598-2dd5-7c4f-9f92-8f7a7f8b58${suffix}`,
      idempotencyKey
    }),
    payload: createMinimalPersistencePayload({
      idempotencyKey,
      ...payloadOverrides
    })
  };
}

function started(): RuntimeTelemetryEvent {
  return {
    name: "runtime.message.started",
    level: "info",
    at: "2026-07-31T00:00:00.000Z",
    stage: "persistence",
    queue: "nutsnews.worker.persistence.v1",
    outcome: "started"
  };
}

function completion(
  name: Extract<RuntimeTelemetryEvent["name"], `runtime.message.${string}`>,
  outcome: NonNullable<RuntimeTelemetryEvent["outcome"]>,
  durationMs: number
): RuntimeTelemetryEvent {
  return {
    name,
    level: outcome === "success" || outcome === "duplicate" ? "info" : "warn",
    at: "2026-07-31T00:00:01.000Z",
    stage: "persistence",
    queue: "nutsnews.worker.persistence.v1",
    outcome,
    durationMs,
    attempt: 1,
    messageId: "message-id-cardinality-probe",
    idempotencyKey: "idempotency-cardinality-probe",
    correlationId: "correlation-cardinality-probe"
  };
}
