import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import { createPersistenceService } from "../src/service.js";
import {
  InMemoryPersistenceInboxStore,
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceBrokerOutbox,
  LocalPersistenceWorkHandler,
  LocalStageViewReader,
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery
} from "../src/test-doubles.js";

describe("createPersistenceService", () => {
  it("starts, becomes ready, registers persistence and publication routes, and drains cleanly", async () => {
    const context = createServiceContext();

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer?.stage).toBe("persistence");
    expect(context.broker.assertedRoutes.map((route) => route.stage)).toEqual([
      "persistence",
      "publication"
    ]);
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("ok");
    expect(context.metrics.collect()).toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("delegates valid persistence deliveries and acks duplicate replays", async () => {
    const context = createServiceContext();
    const delivery = createMinimalPersistenceDelivery();

    await context.service.start();

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
      commandKind: "save_summaries",
      backendOperation: "save-article-summaries-batch"
    });

    await context.service.stop();
  });

  it("rejects payloads that are not consumed by the persistence service", async () => {
    const context = createServiceContext();

    await context.service.start();

    await expect(context.broker.deliverPersistence({
      envelope: createMinimalPersistenceDelivery().envelope,
      payload: translationTaskPayload(),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "dlq",
      reason: "payload-consumer-mismatch"
    });

    expect(context.workHandler.handled).toHaveLength(0);

    await context.service.stop();
  });

  it("waits for in-flight persistence work during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.workHandler.handleGate = gate.promise;
    context.workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverPersistence();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.workHandler.handled).toHaveLength(0);

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("keeps liveness independent from backend API readiness but blocks readiness on compatibility", async () => {
    const context = createServiceContext();

    context.backendApi.version = "worker-api-v0";
    await context.service.start();

    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });

  it("blocks readiness when permission scopes are degraded", async () => {
    const context = createServiceContext();

    context.finalShadow.status = "degraded";
    context.stageViews.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });
});

function createServiceContext() {
  const config = loadPersistenceConfig({
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalPersistenceDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPrometheusRuntimeTelemetrySink({
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
    telemetry,
    metrics
  });

  return {
    backendApi: dependencies.backendApiClient as LocalBackendWorkerApiClient,
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    finalShadow: dependencies.finalShadowTransactions as LocalFinalShadowTransactionRunner,
    inbox: dependencies.inboxStore as InMemoryPersistenceInboxStore,
    metrics,
    outbox: dependencies.brokerOutbox as LocalPersistenceBrokerOutbox,
    service,
    stageViews: dependencies.stageViewReader as LocalStageViewReader,
    telemetry,
    workHandler: dependencies.workHandler as LocalPersistenceWorkHandler
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}

function translationTaskPayload(): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    idempotencyKey: "approval:translation:article-001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    articleId: "article-001",
    sourceLanguage: "en",
    targetLanguages: [
      "fr"
    ],
    reason: "new_article",
    existingLanguageCodes: []
  };
}
