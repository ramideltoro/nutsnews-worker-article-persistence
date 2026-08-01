import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION
} from "@ramideltoro/nutsnews-worker-contracts";
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
import { createPersistenceService } from "../src/service.js";
import { createPersistencePrometheusTelemetrySink } from "../src/telemetry.js";
import {
  InMemoryPersistenceInboxStore,
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceBrokerOutbox,
  LocalPersistenceWorkHandler,
  LocalStageViewReader,
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery,
  createMinimalPersistenceEnvelope,
  createMinimalPersistencePayload
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
    expect(context.metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("materializes valid persistence deliveries and acks duplicate replays without duplicate side effects", async () => {
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

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.finalShadow.aggregates).toHaveLength(1);
    expect(context.finalShadow.audits).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.outbox.records).toHaveLength(1);
    expect(context.telemetry.events
      .filter((event) => event.name.startsWith("runtime.message."))
      .map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate"
    ]);
    const shadowCommand = context.backendApi.shadowAggregateCommands[0];

    expect(shadowCommand?.shadowAggregate.payloadDigest).toMatch(/^sha256:/);
    expect(context.backendApi.shadowAggregateCommands[0]).toMatchObject({
      operation: "uplift-record-shadow-aggregate",
      providerMode: "backend_postgres_shadow",
      actorService: "worker-uplift-persistence",
      schemaVersion: 1,
      operationVersion: 1,
      expectedArticleVersion: 1,
      shadowAggregate: {
        articleIdentityHash: "article-hash-001",
        canonicalUrlHash: "canonical-url-hash-001",
        originalUrlHash: "original-url-hash-001",
        aggregateVersion: 1,
        titleRef: "backend://worker-uplift/enrichment/article-001/title/v1",
        imageUrlRef: "backend://worker-uplift/enrichment/article-001/image/v1",
        category: "Science",
        positivityScore: 8.5,
        approvalVersion: 1,
        translationLanguages: [
          "fr",
          "ja"
        ],
        publicationStatus: "ready",
        payloadRef: "backend://worker-uplift/final-shadow/article-hash-001/v1",
        diagnosticMetadata: {
          safeMetadataOnly: true,
          approved: true,
          acceptedLanguageCount: 2,
          missingLanguageCount: 0
        }
      }
    });
    expect(context.broker.published[0]?.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness,
      readinessStatus: "ready",
      articleId: "article-001",
      requiredLanguageCodes: [
        "fr",
        "ja"
      ],
      availableLanguageCodes: [
        "fr",
        "ja"
      ],
      missingLanguageCodes: [],
      snapshotRefreshRequired: true
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

    expect(context.finalShadow.materializations).toHaveLength(0);
    expect(context.telemetry.events
      .filter((event) => event.name.startsWith("runtime.message."))
      .map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.invalid"
    ]);

    await context.service.stop();
  });

  it("acknowledges translation summary persistence commands without final side effects", async () => {
    const context = createServiceContext();
    const delivery = createTranslationSummaryPersistenceDelivery("fr");

    await context.service.start();

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.finalShadow.materializations).toHaveLength(0);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);

    await context.service.stop();
  });

  it("materializes aggregate translation status messages into final shadow state", async () => {
    const context = createServiceContext();
    const delivery = createTranslationStatusPersistenceDelivery();

    await context.service.start();

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.finalShadow.materializations[0]?.request).toMatchObject({
      commandId: "translation-result:article-001:v1",
      payloadBackendOperation: "save-accepted-articles-batch",
      stageRefs: {
        translations: [
          {
            languageCode: "fr",
            version: 1
          },
          {
            languageCode: "ja",
            version: 1
          }
        ]
      }
    });
    expect(context.broker.published[0]?.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness,
      readinessStatus: "ready",
      snapshotRefreshRequired: true
    });

    await context.service.stop();
  });

  it("waits for in-flight persistence work during shutdown without wall-clock sleeps", async () => {
    const workHandler = new LocalPersistenceWorkHandler();
    const context = createServiceContext({
      workHandler
    });
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    workHandler.handleGate = gate.promise;
    workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverPersistence();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(workHandler.handled).toHaveLength(0);

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("quarantines conflicting idempotency-key reuse with a changed payload", async () => {
    const context = createServiceContext();
    const delivery = createMinimalPersistenceDelivery();
    const conflictingDelivery = {
      ...delivery,
      payload: createMinimalPersistencePayload({
        commandId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5704"
      })
    };

    await context.service.start();

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverPersistence(conflictingDelivery)).resolves.toMatchObject({
      action: "dlq",
      reason: "idempotency-payload-conflict"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.finalShadow.quarantines).toHaveLength(1);
    expect(context.finalShadow.quarantines[0]).toMatchObject({
      reason: "idempotency-payload-conflict",
      idempotencyKey: "persistence:final-shadow:article-001:v1"
    });
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);

    await context.service.stop();
  });

  it("rolls back local final-shadow writes after backend API failure and remains recoverable", async () => {
    const context = createServiceContext();
    const delivery = createMinimalPersistenceDelivery();

    context.backendApi.failNextShadowAggregate = true;
    await context.service.start();

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "backend-api-transient"
    });

    expect(context.finalShadow.materializations).toHaveLength(0);
    expect(context.finalShadow.transactionalOutboxCommands).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);

    await context.service.stop();
  });

  it("rolls back local final-shadow transaction failures and recovers through backend idempotency", async () => {
    const context = createServiceContext();
    const delivery = createMinimalPersistenceDelivery();

    context.finalShadow.failNextRecord = true;
    await context.service.start();

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "serialization-failure"
    });

    expect(context.finalShadow.materializations).toHaveLength(0);
    expect(context.finalShadow.transactionalOutboxCommands).toHaveLength(0);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(0);

    await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.outbox.records).toHaveLength(1);

    await context.service.stop();
  });

  it("quarantines stale stage result versions before backend API or outbox side effects", async () => {
    const context = createServiceContext();

    context.stageViews.materializationInputs = {
      ...context.stageViews.materializationInputs,
      canonical: {
        ...context.stageViews.materializationInputs.canonical,
        resultVersion: 2
      }
    };

    await context.service.start();

    await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
      action: "dlq",
      reason: "stale-stage-version"
    });

    expect(context.finalShadow.materializations).toHaveLength(0);
    expect(context.finalShadow.quarantines).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);

    await context.service.stop();
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
  it("reports readiness unhealthy when the main queue consumer is cancelled", async () => {
    const context = createServiceContext();

    await context.service.start();
    await context.service.consumer?.cancel();

    const readiness = await context.service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    const consumerCheck = readiness.checks.find((check) => check.name === "rabbitmq-consumer");
    expect(consumerCheck?.status).toBe("unhealthy");
    expect(consumerCheck?.details).toMatchObject({
      queue: "nutsnews.worker.persistence.v1",
      activeConsumers: 0
    });

    await context.service.stop();
  });
});

function createServiceContext(options: {
  readonly workHandler?: LocalPersistenceWorkHandler;
} = {}) {
  const config = loadPersistenceConfig({
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalPersistenceDependencies({
    ...(options.workHandler === undefined ? {} : {
      workHandler: options.workHandler
    })
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
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
    inbox: dependencies.inboxStore as InMemoryPersistenceInboxStore,
    metrics,
    outbox: dependencies.brokerOutbox as LocalPersistenceBrokerOutbox,
    service,
    stageViews: dependencies.stageViewReader as LocalStageViewReader,
    telemetry,
    workHandler: dependencies.workHandler
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

function createTranslationSummaryPersistenceDelivery(languageCode: string) {
  const idempotencyKey = `translation:persistence:article-001:${languageCode}:v1`;

  return {
    envelope: createMinimalPersistenceEnvelope({
      idempotencyKey
    }),
    payload: {
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
      schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
      pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
      stageExecutionId: languageCode === "fr"
        ? "018f1598-2dd5-7c4f-9f92-8f7a7f8b5721"
        : "018f1598-2dd5-7c4f-9f92-8f7a7f8b5722",
      sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5701",
      idempotencyKey,
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      producedAt: "2026-07-23T00:00:00.000Z",
      commandId: languageCode === "fr"
        ? "018f1598-2dd5-7c4f-9f92-8f7a7f8b5731"
        : "018f1598-2dd5-7c4f-9f92-8f7a7f8b5732",
      commandKind: "save_summaries",
      backendOperation: "save-article-summaries-batch",
      entityRefs: [
        {
          articleId: "article-001",
          articleVersion: 1,
          sourceLanguage: "en",
          targetLanguage: languageCode,
          summaryRef: {
            kind: "backend-record",
            uri: `backend://worker-uplift/translation/article-001/${languageCode}/summary`,
            mediaType: "application/json",
            articleId: "article-001",
            targetLanguage: languageCode,
            resultId: `translation-result-${languageCode}`
          },
          qualityRef: {
            kind: "backend-record",
            uri: `backend://worker-uplift/translation/article-001/${languageCode}/quality`,
            mediaType: "application/json",
            resultId: `translation-result-${languageCode}`
          }
        }
      ],
      writeMode: "upsert",
      providerMode: "backend_postgres_primary"
    },
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function createTranslationStatusPersistenceDelivery() {
  const idempotencyKey = "translation:result:article-001:1";

  return {
    envelope: createMinimalPersistenceEnvelope({
      idempotencyKey
    }),
    payload: {
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationResult,
      schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
      pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
      stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5741",
      sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5701",
      idempotencyKey,
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      producedAt: "2026-07-23T00:00:00.000Z",
      articleId: "article-001",
      translationStatus: "complete",
      completedLanguageCodes: [
        "fr",
        "ja"
      ],
      missingLanguageCodes: [],
      summaryRefs: [
        {
          kind: "backend-record",
          uri: "backend://worker-uplift/translation/article-001/fr/summary",
          mediaType: "application/json",
          articleId: "article-001",
          targetLanguage: "fr",
          resultId: "translation-result-fr"
        },
        {
          kind: "backend-record",
          uri: "backend://worker-uplift/translation/article-001/ja/summary",
          mediaType: "application/json",
          articleId: "article-001",
          targetLanguage: "ja",
          resultId: "translation-result-ja"
        }
      ]
    },
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}
