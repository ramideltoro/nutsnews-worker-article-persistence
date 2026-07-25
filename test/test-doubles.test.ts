import {
  getWorkerRoute,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceBrokerOutbox,
  LocalStageViewReader,
  createMinimalPersistenceDelivery
} from "../src/test-doubles.js";

describe("persistence test doubles", () => {
  it("requires a registered local broker consumer before delivery", async () => {
    const broker = new LocalBrokerTransport();

    await broker.connect();

    await expect(broker.deliverPersistence(createMinimalPersistenceDelivery())).rejects.toThrow("No local consumer is registered for persistence.");
  });

  it("records local transaction and outbox boundaries without external dependencies", async () => {
    const runner = new LocalFinalShadowTransactionRunner();
    const outbox = new LocalPersistenceBrokerOutbox();
    const route = getWorkerRoute("publication");
    const command = {
      envelope: {
        schemaId: route.schemaId,
        schemaVersion: 1,
        route: "publication",
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5901",
        causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5801",
        correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5601",
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        idempotencyKey: "persistence:publication:article-001",
        aggregate: {
          type: "article",
          id: "article-001",
          version: 1
        },
        occurredAt: "2026-07-23T00:00:00.000Z",
        attempt: {
          count: 1,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        },
        producer: {
          name: "persistence",
          version: "0.1.0"
        },
        payloadRef: {
          kind: "backend-record",
          uri: "backend://worker-uplift/persistence/article-001/publication-readiness",
          mediaType: "application/json",
          sizeBytes: 512
        }
      } satisfies WorkerMessageEnvelope,
      payload: {}
    };

    await expect(runner.withTransaction((transaction) => Promise.resolve(transaction.transactionId))).resolves.toBe("local-final-shadow-transaction-1");
    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: "publication",
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });

    expect(runner.transactions).toHaveLength(1);
    expect(outbox.records).toHaveLength(1);
  });

  it("documents least-privilege database and backend API boundaries", () => {
    const finalShadow = new LocalFinalShadowTransactionRunner();
    const stageViews = new LocalStageViewReader();
    const backendApi = new LocalBackendWorkerApiClient();

    expect(finalShadow.checkWriteScope()).toMatchObject({
      status: "ok",
      details: {
        databaseRole: "nutsnews_worker_persistence",
        allowedWriteScopes: [
          "worker_uplift.final_article_aggregate",
          "worker_uplift.persistence_inbox",
          "worker_uplift.persistence_outbox"
        ],
        deniedWriteScopes: [
          "worker_uplift.upstream_stage_owned_tables",
          "public.domain_tables",
          "legacy_ingestion_tables"
        ]
      }
    });
    expect(stageViews.checkReadScope()).toMatchObject({
      status: "ok",
      details: {
        allowedWriteScopes: [],
        allowedReadScopes: [
          "worker_uplift.v_approval_decisions",
          "worker_uplift.v_translation_results",
          "worker_uplift.v_stage_worker_runs"
        ]
      }
    });
    expect(backendApi.checkCompatibility("worker-api-v1")).toMatchObject({
      status: "ok",
      version: "worker-api-v1",
      productionDomainWritesEnabled: false,
      requiredScopes: [
        "worker:persistence:shadow",
        "worker:persistence:future-domain-command"
      ]
    });
  });
});
