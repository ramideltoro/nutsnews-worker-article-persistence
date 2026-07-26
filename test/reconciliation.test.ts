import { createHash } from "node:crypto";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import type {
  BrokerConsumerHandle,
  BrokerDeliveryHandler,
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport
} from "@ramideltoro/nutsnews-worker-runtime";
import type { QueryResultRow } from "pg";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  PostgresPersistenceBrokerOutbox,
  PostgresPersistenceOutboxReconciler
} from "../src/production.js";
import { PERSISTENCE_RECONCILIATION_CONFIRMATION } from "../src/reconciliation.js";

const now = "2026-07-23T00:00:00.000Z";
const clock = {
  now: () => new Date(now)
};

describe("persistence outbox reconciliation", () => {
  it("records full envelope and payload so service-owned replay can hydrate payload_ref", async () => {
    const pool = new FakePool([]);
    const outbox = new PostgresPersistenceBrokerOutbox(pool.asPool());
    const command = publicationCommand();

    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: getWorkerRoute("publication").exchange,
      routingKey: getWorkerRoute("publication").routingKey,
      confirmed: true,
      confirmedAt: now
    });

    const diagnostic = JSON.parse(String(firstQuery(pool).values[14])) as Readonly<Record<string, unknown>>;

    expect(diagnostic).toMatchObject({
      envelope: {
        messageId: command.envelope.messageId,
        correlationId: command.envelope.correlationId,
        causationId: command.envelope.causationId,
        idempotencyKey: command.envelope.idempotencyKey
      },
      payload: {
        schemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness,
        idempotencyKey: command.envelope.idempotencyKey
      },
      payloadSchemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness
    });
  });

  it("dry-runs deterministic candidates without publishing", async () => {
    const command = publicationCommand();
    const pool = new FakePool([
      outboxRow(command)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresPersistenceOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {}
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.candidates[0]).toMatchObject({
      idempotencyKey: command.envelope.idempotencyKey,
      destinationStage: "publication",
      status: "selected"
    });
    expect(transport.published).toHaveLength(0);
  });

  it("applies replay with a new message ID while preserving routing metadata and audit history", async () => {
    const command = publicationCommand();
    const pool = new FakePool([
      outboxRow(command)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresPersistenceOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {
        NUTSNEWS_PERSISTENCE_RECONCILIATION_APPLY_ENABLED: "true"
      }
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: PERSISTENCE_RECONCILIATION_CONFIRMATION
    });

    expect(report).toMatchObject({
      status: "applied",
      replayedCount: 1,
      writesPerformed: true,
      productionVisibilityEnabled: false
    });
    expect(transport.published).toHaveLength(1);
    const replay = transport.published[0];
    expect(replay?.envelope.messageId).not.toBe(command.envelope.messageId);
    expect(replay?.envelope.idempotencyKey).toBe(command.envelope.idempotencyKey);
    expect(replay?.envelope.correlationId).toBe(command.envelope.correlationId);
    expect(replay?.envelope.causationId).toBe(command.envelope.causationId);
    expect(replay?.envelope.aggregate).toEqual(command.envelope.aggregate);
    expect(pool.queries.some((query) => query.sql.includes("reconciliationAuditHistory"))).toBe(true);
  });

  it("fails closed without publishing when the authoritative envelope is missing", async () => {
    const command = publicationCommand();
    const pool = new FakePool([
      {
        ...outboxRow(command),
        diagnostic_metadata: {
          payload: command.payload,
          payloadSchemaId: command.payload.schemaId
        }
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresPersistenceOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {
        NUTSNEWS_PERSISTENCE_RECONCILIATION_APPLY_ENABLED: "true"
      }
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: PERSISTENCE_RECONCILIATION_CONFIRMATION
    });

    expect(report.status).toBe("failed_closed");
    expect(report.errors).toContain("1:missing-stored-envelope");
    expect(report.writesPerformed).toBe(false);
    expect(transport.published).toHaveLength(0);
  });
});

function publicationCommand(): BrokerPublishCommand {
  const route = getWorkerRoute("publication");
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3301",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3302",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3303",
    idempotencyKey: "persistence:publication:article-001:1",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    articleId: "article-001",
    readinessStatus: "ready",
    requiredLanguageCodes: [
      "fr"
    ],
    availableLanguageCodes: [
      "fr"
    ],
    missingLanguageCodes: [],
    snapshotRefreshRequired: true
  };
  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "publication",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3310",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3303",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3300",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: payload.idempotencyKey,
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "persistence",
      version: "0.1.0",
      instanceId: "test-host"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/persistence/article-001/publication-readiness",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload),
      digest: sha256Digest(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function outboxRow(command: BrokerPublishCommand): QueryResultRow {
  return {
    id: "1",
    outbox_message_id: command.envelope.messageId,
    pipeline_run_id: command.payload.pipelineRunId,
    stage_execution_id: command.payload.stageExecutionId,
    destination_stage: command.envelope.route,
    routing_key: getWorkerRoute(command.envelope.route).routingKey,
    entity_kind: command.envelope.aggregate.type,
    entity_id: command.envelope.aggregate.id,
    schema_version: command.envelope.schemaVersion,
    operation_version: command.envelope.aggregate.version,
    idempotency_key: command.envelope.idempotencyKey,
    payload_ref: command.envelope.payloadRef.uri,
    payload_digest: sha256Digest(command.payload),
    created_at: new Date("2026-07-22T23:00:00.000Z"),
    published_at: new Date("2026-07-22T23:00:01.000Z"),
    confirmed_at: new Date("2026-07-22T23:00:02.000Z"),
    status: "confirmed",
    diagnostic_metadata: {
      envelope: command.envelope,
      payload: command.payload,
      payloadSchemaId: command.payload.schemaId
    }
  };
}

function sha256Digest(value: unknown): string {
  return `sha256:${createHash("sha256").update(canonicalJson(value)).digest("hex")}`;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(sortJsonValue(value));
}

function sortJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => sortJsonValue(item));
  }

  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [
          key,
          sortJsonValue(item)
        ])
    );
  }

  return value;
}

function firstQuery(pool: FakePool): { readonly sql: string; readonly values: readonly unknown[] } {
  const query = pool.queries[0];

  if (query === undefined) {
    throw new Error("expected a captured query");
  }

  return query;
}

class FakePool {
  readonly queries: { readonly sql: string; readonly values: readonly unknown[] }[] = [];

  constructor(private readonly rows: readonly QueryResultRow[]) {}

  asPool() {
    return this as never;
  }

  query(sql: string, values: readonly unknown[] = []): Promise<{ readonly rows: QueryResultRow[]; readonly rowCount: number }> {
    this.queries.push({
      sql,
      values
    });

    if (sql.trimStart().startsWith("SELECT")) {
      return Promise.resolve({
        rows: [...this.rows],
        rowCount: this.rows.length
      });
    }

    return Promise.resolve({
      rows: [],
      rowCount: 1
    });
  }
}

class FakeBrokerTransport implements RuntimeBrokerTransport {
  readonly name = "fake-broker";
  readonly published: BrokerPublishCommand[] = [];

  connect(): Promise<void> {
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    void routes;
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: now
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    void stage;
    void handler;
    throw new Error("consume is not supported in fake transport");
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}
