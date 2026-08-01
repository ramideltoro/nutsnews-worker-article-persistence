import {
  createBufferedRuntimeTelemetrySink,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import { PersistencePermanentError } from "../src/errors.js";
import { createPersistenceService } from "../src/service.js";
import {
  PERSISTENCE_STAGE_METRIC_OUTCOMES,
  createPersistencePrometheusTelemetrySink
} from "../src/telemetry.js";
import {
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceBrokerOutbox,
  LocalStageViewReader,
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery
} from "../src/test-doubles.js";

describe("persistence transaction, replay, and DLQ safety", () => {
  it("recovers when broker publish fails after final-shadow commit but before ack", async () => {
    const context = createSafetyContext();

    context.broker.failNextPublish = true;
    await context.service.start();

    await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
      action: "retry",
      reason: "broker-publish-not-confirmed"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.finalShadow.transactionalOutboxCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(0);
    expect(context.outbox.records).toHaveLength(0);

    await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.outbox.records).toHaveLength(1);
    expect(context.telemetry.events
      .filter((event) => event.name.startsWith("runtime.message."))
      .map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.accepted"
    ]);

    await context.service.stop();
  });

  it("recovers when broker confirm succeeds but outbox receipt recording fails", async () => {
    const context = createSafetyContext();

    context.outbox.failNextRecord = true;
    await context.service.start();

    await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
      action: "retry",
      reason: "outbox-receipt-write-lost"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.outbox.records).toHaveLength(0);

    await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(2);
    expect(context.broker.published[0]?.envelope.messageId).toBe(context.broker.published[1]?.envelope.messageId);
    expect(context.outbox.records).toHaveLength(1);

    await context.service.stop();
  });

  it("sends permanent backend/database faults to DLQ without local accepted output", async () => {
    const context = createSafetyContext();

    context.backendApi.nextShadowAggregateError = new PersistencePermanentError("permission-denied");
    await context.service.start();

    await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
      action: "dlq",
      reason: "permission-denied"
    });

    expect(context.finalShadow.materializations).toHaveLength(0);
    expect(context.finalShadow.transactionalOutboxCommands).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);
    expect(context.outbox.records).toHaveLength(0);
    expect(context.telemetry.events
      .filter((event) => event.name.startsWith("runtime.message."))
      .map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(context.telemetry.events.find((event) => event.name === "runtime.message.dlq")?.attributes).toMatchObject({
      reason: "permission-denied"
    });

    await context.service.stop();
  });

  it("keeps concurrent duplicate deliveries from creating duplicate visible results", async () => {
    const context = createSafetyContext();
    const delivery = createMinimalPersistenceDelivery();

    await context.service.start();

    const results = await Promise.all(Array.from({ length: 20 }, () => context.broker.deliverPersistence(delivery)));
    const ackCount = results.filter((result) => result.action === "ack").length;
    const retryCount = results.filter((result) => result.action === "retry" && result.reason === "idempotency-in-progress").length;

    expect(ackCount).toBe(1);
    expect(retryCount).toBe(19);
    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    expect(context.finalShadow.materializations).toHaveLength(1);

    await context.service.stop();
  });

  it("replays a historical batch without duplicate aggregates, API calls, or outbox records", async () => {
    const context = createSafetyContext();
    const delivery = createMinimalPersistenceDelivery();

    await context.service.start();

    for (let index = 0; index < 100; index += 1) {
      await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
        action: "ack"
      });
    }

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.outbox.records).toHaveLength(1);
    expect(context.finalShadow.transactions.length).toBeLessThanOrEqual(2);

    const lifecycleEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    const completionEvents = lifecycleEvents.filter((event) => event.name !== "runtime.message.started");

    expect(lifecycleEvents.filter((event) => event.name === "runtime.message.started")).toHaveLength(100);
    expect(completionEvents).toHaveLength(100);
    expect(completionEvents.filter((event) => event.name === "runtime.message.accepted")).toHaveLength(1);
    expect(completionEvents.filter((event) => event.name === "runtime.message.duplicate")).toHaveLength(99);

    const collectedMetrics = context.metrics.collect();
    const messageSeries = collectedMetrics
      .split("\n")
      .filter((line) => line.startsWith("nutsnews_worker_messages_total{"));
    const stageEventSeries = collectedMetrics
      .split("\n")
      .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{"));

    expect(messageSeries).toHaveLength(2);
    expect(messageSeries.find((line) => line.includes('outcome="success"'))).toMatch(/ 1$/u);
    expect(messageSeries.find((line) => line.includes('outcome="duplicate"'))).toMatch(/ 99$/u);
    expect(stageEventSeries).toHaveLength(PERSISTENCE_STAGE_METRIC_OUTCOMES.length);
    expect(stageEventSeries.find((line) => line.includes('outcome="success"'))).toMatch(/ 1$/u);
    expect(stageEventSeries.find((line) => line.includes('outcome="duplicate"'))).toMatch(/ 99$/u);

    for (const outcome of [
      "invalid",
      "retry",
      "dlq"
    ]) {
      expect(stageEventSeries.find((line) => line.includes(`outcome="${outcome}"`))).toMatch(/ 0$/u);
    }
    expect(collectedMetrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="persistence",le="30"} 100');
    expect(collectedMetrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="persistence",le="+Inf"} 100');
    expect(collectedMetrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="local",service="persistence"} 100');
    expect(collectedMetrics).not.toContain("018f1598-2dd5-7c4f-9f92-8f7a7f8b5801");
    expect(collectedMetrics).not.toContain("persistence:final-shadow:article-001:v1");

    for (const series of messageSeries) {
      const labelNames = [
        ...series.matchAll(/([a-z_]+)="/gu)
      ].map((match) => match[1]);

      expect(labelNames).toEqual([
        "environment",
        "host",
        "service",
        "version",
        "queue",
        "outcome"
      ]);
    }

    for (const series of stageEventSeries) {
      const labelNames = [
        ...series.matchAll(/([a-z_]+)="/gu)
      ].map((match) => match[1]);

      expect(labelNames).toEqual([
        "environment",
        "service",
        "outcome"
      ]);
    }

    await context.service.stop();
  });
});

function createSafetyContext() {
  const config = loadPersistenceConfig({
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalPersistenceDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink(500);
  const metrics = createPersistencePrometheusTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const serviceTelemetry: RuntimeTelemetrySink = {
    async emit(event) {
      await telemetry.emit(event);
      await metrics.emit(event);
    }
  };
  const service = createPersistenceService({
    config,
    dependencies,
    telemetry: serviceTelemetry,
    metrics
  });

  return {
    backendApi: dependencies.backendApiClient as LocalBackendWorkerApiClient,
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    finalShadow: dependencies.finalShadowTransactions as LocalFinalShadowTransactionRunner,
    metrics,
    outbox: dependencies.brokerOutbox as LocalPersistenceBrokerOutbox,
    service,
    stageViews: dependencies.stageViewReader as LocalStageViewReader,
    telemetry
  };
}
