import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  PersistenceBackendApiCompatibility,
  PersistenceBackendWorkerApiClient,
  PersistenceBrokerOutbox,
  PersistenceDatabaseTransaction,
  PersistenceDependencies,
  PersistenceDependencyProbe,
  PersistenceFinalShadowTransactionRunner,
  PersistenceInboxStore,
  PersistenceInboxFingerprintResult,
  PersistencePermissionProbe,
  PersistenceStageViewReader,
  PersistenceWorkHandler,
  PersistenceWorkTools
} from "./dependencies.js";
import { sha256Digest } from "./digest.js";
import { createFinalShadowMaterializationHandler } from "./materialization.js";
import type {
  PersistenceBackendShadowAggregateCommand,
  PersistenceBackendShadowAggregateResult,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalMaterializationWriteResult,
  PersistenceQuarantineRecord
} from "./materialization-types.js";

export class ManualPersistenceClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class InMemoryPersistenceInboxStore implements PersistenceInboxStore {
  readonly name: string = "local-persistence-inbox";
  status: PersistenceDependencyProbe["status"] = "ok";
  private readonly store;
  private readonly fingerprints = new Map<string, string>();

  constructor(clock: RuntimeClock = new ManualPersistenceClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local persistence inbox ready" : "local persistence inbox degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  verifyPayloadFingerprint(idempotencyKey: string, fingerprint: string): Promise<PersistenceInboxFingerprintResult> {
    const existing = this.fingerprints.get(idempotencyKey);

    if (existing === undefined) {
      this.fingerprints.set(idempotencyKey, fingerprint);
      return Promise.resolve({
        status: "accepted"
      });
    }

    if (existing === fingerprint) {
      return Promise.resolve({
        status: "duplicate"
      });
    }

    return Promise.resolve({
      status: "conflict",
      existingFingerprint: existing
    });
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }
}

interface LocalTransactionPendingWrites {
  readonly materializations: PersistenceFinalMaterializationRecord[];
  readonly quarantines: PersistenceQuarantineRecord[];
}

export class LocalFinalShadowTransactionRunner implements PersistenceFinalShadowTransactionRunner {
  readonly name: string = "local-final-shadow-transactions";
  status: PersistenceDependencyProbe["status"] = "ok";
  readonly transactions: PersistenceDatabaseTransaction[] = [];
  readonly materializations: PersistenceFinalMaterializationRecord[] = [];
  readonly aggregates: PersistenceFinalMaterializationRecord["aggregate"][] = [];
  readonly audits: PersistenceFinalMaterializationRecord["audit"][] = [];
  readonly transactionalOutboxCommands: BrokerPublishCommand[] = [];
  readonly quarantines: PersistenceQuarantineRecord[] = [];
  databaseRole = "nutsnews_worker_persistence";
  shadowSchemaVersion = "worker-uplift-shadow-v1";
  allowedWriteScopes = [
    "worker_uplift.final_article_aggregate",
    "worker_uplift.persistence_inbox",
    "worker_uplift.persistence_outbox"
  ];
  allowedReadScopes = [
    "worker_uplift.approved_stage_result_views"
  ];
  deniedWriteScopes = [
    "worker_uplift.upstream_stage_owned_tables",
    "public.domain_tables",
    "legacy_ingestion_tables"
  ];
  failNextRecord = false;
  private readonly receipts = new Map<string, PersistenceFinalMaterializationRecord>();
  private readonly pendingWrites = new Map<string, LocalTransactionPendingWrites>();

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local final shadow transaction runner ready" : "local final shadow transaction runner degraded"
    };
  }

  checkWriteScope(): PersistencePermissionProbe {
    return permissionProbe(this.status, "final shadow write scope ready", {
      databaseRole: this.databaseRole,
      shadowSchemaVersion: this.shadowSchemaVersion,
      allowedWriteScopes: this.allowedWriteScopes,
      allowedReadScopes: this.allowedReadScopes,
      deniedWriteScopes: this.deniedWriteScopes
    });
  }

  async withTransaction<T>(operation: (transaction: PersistenceDatabaseTransaction) => Promise<T>): Promise<T> {
    const transaction = {
      transactionId: `local-final-shadow-transaction-${String(this.transactions.length + 1)}`
    };
    const pending = {
      materializations: [],
      quarantines: []
    };

    this.transactions.push(transaction);
    this.pendingWrites.set(transaction.transactionId, pending);

    try {
      const result = await operation(transaction);

      this.commit(pending);
      return result;
    } finally {
      this.pendingWrites.delete(transaction.transactionId);
    }
  }

  recordFinalMaterialization(
    transaction: PersistenceDatabaseTransaction,
    record: PersistenceFinalMaterializationRecord
  ): Promise<PersistenceFinalMaterializationWriteResult> {
    if (this.failNextRecord) {
      this.failNextRecord = false;
      return Promise.reject(new Error("local final shadow write failed"));
    }

    const existingReceipt = this.receipts.get(record.request.idempotencyKey);

    if (existingReceipt !== undefined) {
      if (existingReceipt.request.payloadDigest !== record.request.payloadDigest) {
        return Promise.resolve({
          status: "conflict",
          reason: "idempotency-conflict"
        });
      }

      return Promise.resolve({
        status: "duplicate",
        aggregate: existingReceipt.aggregate
      });
    }

    const latestAggregateVersion = this.aggregates
      .filter((aggregate) => aggregate.articleIdentityHash === record.aggregate.articleIdentityHash)
      .reduce((latest, aggregate) => Math.max(latest, aggregate.aggregateVersion), 0);

    if (latestAggregateVersion >= record.aggregate.aggregateVersion) {
      return Promise.resolve({
        status: "stale",
        reason: "stale-final-shadow-aggregate-version"
      });
    }

    this.pendingFor(transaction).materializations.push(record);
    return Promise.resolve({
      status: "recorded",
      aggregate: record.aggregate,
      publicationReadinessCommand: record.publicationReadinessCommand
    });
  }

  recordQuarantine(transaction: PersistenceDatabaseTransaction, record: PersistenceQuarantineRecord): Promise<void> {
    this.pendingFor(transaction).quarantines.push(record);
    return Promise.resolve();
  }

  private pendingFor(transaction: PersistenceDatabaseTransaction): LocalTransactionPendingWrites {
    const pending = this.pendingWrites.get(transaction.transactionId);

    if (pending === undefined) {
      throw new Error(`No local pending transaction exists for ${transaction.transactionId}.`);
    }

    return pending;
  }

  private commit(pending: LocalTransactionPendingWrites): void {
    for (const materialization of pending.materializations) {
      this.materializations.push(materialization);
      this.aggregates.push(materialization.aggregate);
      this.audits.push(materialization.audit);
      this.transactionalOutboxCommands.push(materialization.publicationReadinessCommand);
      this.receipts.set(materialization.request.idempotencyKey, materialization);
    }

    this.quarantines.push(...pending.quarantines);
  }
}

export class LocalStageViewReader implements PersistenceStageViewReader {
  readonly name: string = "local-stage-view-reader";
  status: PersistenceDependencyProbe["status"] = "ok";
  materializationInputs: PersistenceFinalMaterializationInputs = createLocalFinalMaterializationInputs();
  databaseRole = "nutsnews_worker_persistence";
  shadowSchemaVersion = "worker-uplift-shadow-v1";
  allowedReadScopes = [
    "worker_uplift_views.canonical_article_projection",
    "worker_uplift_views.approval_projection",
    "worker_uplift_views.translation_coverage_projection",
    "worker_uplift_views.final_shadow_article_projection"
  ];
  allowedWriteScopes: readonly string[] = [];
  deniedWriteScopes = [
    "worker_uplift.approval_state",
    "worker_uplift.translation_state",
    "public.domain_tables"
  ];

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local stage view reader ready" : "local stage view reader degraded"
    };
  }

  checkReadScope(): PersistencePermissionProbe {
    return permissionProbe(this.status, "approved stage result view scope ready", {
      databaseRole: this.databaseRole,
      shadowSchemaVersion: this.shadowSchemaVersion,
      allowedWriteScopes: this.allowedWriteScopes,
      allowedReadScopes: this.allowedReadScopes,
      deniedWriteScopes: this.deniedWriteScopes
    });
  }

  readFinalMaterializationInputs(request: PersistenceFinalMaterializationRequest): PersistenceFinalMaterializationInputs {
    void request;
    return this.materializationInputs;
  }
}

export class LocalBackendWorkerApiClient implements PersistenceBackendWorkerApiClient {
  readonly name: string = "local-backend-worker-api";
  status: PersistenceDependencyProbe["status"] = "ok";
  version = "worker-api-v1";
  readonly shadowAggregateCommands: PersistenceBackendShadowAggregateCommand[] = [];
  failNextShadowAggregate = false;
  requiredScopes = [
    "worker:persistence:shadow",
    "worker:persistence:future-domain-command"
  ];
  private readonly receipts = new Map<string, string>();

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local backend Worker API ready" : "local backend Worker API degraded"
    };
  }

  checkCompatibility(expectedVersion: string): PersistenceBackendApiCompatibility {
    const versionMatches = this.version === expectedVersion;
    const status = this.status === "ok" && versionMatches ? "ok" : "unhealthy";

    return {
      status,
      summary: versionMatches ? "backend Worker API compatibility ready" : "backend Worker API compatibility mismatch",
      version: this.version,
      requiredScopes: this.requiredScopes,
      productionDomainWritesEnabled: false
    };
  }

  recordShadowAggregate(command: PersistenceBackendShadowAggregateCommand): PersistenceBackendShadowAggregateResult {
    if (this.failNextShadowAggregate) {
      this.failNextShadowAggregate = false;
      throw new Error("local backend shadow aggregate API failed");
    }

    const payloadDigest = sha256Digest(command);
    const existingDigest = this.receipts.get(command.idempotencyKey);

    if (existingDigest !== undefined) {
      if (existingDigest !== payloadDigest) {
        return {
          status: "conflict",
          reason: "backend-idempotency-conflict"
        };
      }

      return {
        status: "duplicate",
        productionSideEffect: false,
        response: {
          ok: true,
          recorded: true,
          duplicate: true
        }
      };
    }

    this.receipts.set(command.idempotencyKey, payloadDigest);
    this.shadowAggregateCommands.push(command);
    return {
      status: "recorded",
      productionSideEffect: false,
      response: {
        ok: true,
        operation: command.operation,
        mode: "shadow",
        recorded: true,
        productionSideEffect: false
      }
    };
  }
}

export class LocalPersistenceBrokerOutbox implements PersistenceBrokerOutbox {
  readonly name: string = "local-persistence-broker-outbox";
  status: PersistenceDependencyProbe["status"] = "ok";
  readonly records: { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] = [];

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local persistence broker outbox ready" : "local persistence broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    this.records.push({
      command,
      receipt
    });
    return Promise.resolve();
  }
}

export class LocalPersistenceWorkHandler implements PersistenceWorkHandler {
  readonly name: string = "local-persistence-work-handler";
  readonly handled: RuntimeMessageContext[] = [];
  result: RuntimeHandlerResult = {
    status: "ok"
  };
  handleGate: Promise<unknown> | undefined;
  onHandleStart: (() => void) | undefined;

  async handle(context: RuntimeMessageContext, tools: PersistenceWorkTools): Promise<RuntimeHandlerResult> {
    void tools;
    this.onHandleStart?.();
    await this.handleGate;
    this.handled.push(context);

    return this.result;
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly inFlightDeliveryCount = 0;
  readonly assertedRoutes: WorkerRoute[] = [];
  readonly published: BrokerPublishCommand[] = [];
  private connected = false;
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertConnected();
    this.assertedRoutes.push(...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.assertConnected();
    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    this.assertConnected();
    this.consumers.set(stage, handler);

    return Promise.resolve({
      stage,
      cancel: () => {
        this.consumers.delete(stage);
        return Promise.resolve();
      }
    });
  }

  deliverPersistence(delivery: RuntimeMessageDelivery = createMinimalPersistenceDelivery()): Promise<RuntimeMessageProcessingResult> {
    const handler = this.consumers.get("persistence");

    if (handler === undefined) {
      return Promise.reject(new Error("No local consumer is registered for persistence."));
    }

    return handler(delivery);
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.connected = false;
    this.consumers.clear();
    return Promise.resolve();
  }

  private assertConnected(): void {
    if (!this.connected) {
      throw new Error("Local broker transport is not connected.");
    }
  }
}

export function createLocalFinalMaterializationInputs(
  overrides: Partial<PersistenceFinalMaterializationInputs> = {}
): PersistenceFinalMaterializationInputs {
  return {
    canonical: {
      articleId: "article-001",
      articleVersion: 1,
      articleIdentityHash: "article-hash-001",
      canonicalUrlHash: "canonical-url-hash-001",
      originalUrlHash: "original-url-hash-001",
      sourceFeedUrl: "https://example.com/feed.xml",
      resultVersion: 1
    },
    enrichment: {
      articleId: "article-001",
      articleVersion: 1,
      titleRef: "backend://worker-uplift/enrichment/article-001/title/v1",
      imageUrlRef: "backend://worker-uplift/enrichment/article-001/image/v1",
      category: "Science",
      resultVersion: 1
    },
    approval: {
      articleId: "article-001",
      articleVersion: 1,
      decision: "approved",
      positivityScore: 8.5,
      approvalVersion: 1
    },
    translations: [
      {
        articleId: "article-001",
        articleVersion: 1,
        languageCode: "fr",
        summaryRef: "backend://worker-uplift/translation/article-001/fr/v1",
        qualityStatus: "accepted",
        translationVersion: 1
      },
      {
        articleId: "article-001",
        articleVersion: 1,
        languageCode: "ja",
        summaryRef: "backend://worker-uplift/translation/article-001/ja/v1",
        qualityStatus: "accepted",
        translationVersion: 1
      }
    ],
    ...overrides
  };
}

export function createLocalPersistenceDependencies(options: {
  readonly clock?: RuntimeClock;
  readonly workHandler?: PersistenceWorkHandler;
} = {}): PersistenceDependencies {
  const clock = options.clock ?? new ManualPersistenceClock();
  const dependencies: PersistenceDependencies = {
    clock,
    inboxStore: new InMemoryPersistenceInboxStore(clock),
    finalShadowTransactions: new LocalFinalShadowTransactionRunner(),
    stageViewReader: new LocalStageViewReader(),
    brokerOutbox: new LocalPersistenceBrokerOutbox(),
    brokerTransport: new LocalBrokerTransport(),
    backendApiClient: new LocalBackendWorkerApiClient(),
    workHandler: options.workHandler ?? new LocalPersistenceWorkHandler()
  };

  if (options.workHandler !== undefined) {
    return dependencies;
  }

  return {
    ...dependencies,
    workHandler: createFinalShadowMaterializationHandler(dependencies)
  };
}

export function createMinimalPersistenceEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("persistence");
  const occurredAt = "2026-07-23T00:00:00.000Z";
  const envelope = {
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "persistence",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5801",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5701",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5601",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "persistence:final-shadow:article-001:v1",
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: "translation",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/persistence/article-001/final-shadow-command",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(createMinimalPersistencePayload())
    },
    ...overrides
  };

  return assertWorkerEnvelope(envelope);
}

export function createMinimalPersistencePayload(
  overrides: Readonly<Record<string, unknown>> = {}
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5701",
    idempotencyKey: "persistence:final-shadow:article-001:v1",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    commandId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b5703",
    commandKind: "save_article",
    backendOperation: "save-accepted-articles-batch",
    entityRefs: [
      {
        articleId: "article-001",
        articleVersion: 1,
        materializationKind: "final_shadow_article",
        requiredLanguageCodes: [
          "fr",
          "ja"
        ],
        stageResultRefs: {
          canonical: {
            uri: "backend://worker-uplift/views/canonical/article-001/v1",
            version: 1,
            digest: "sha256:canonical"
          },
          enrichment: {
            uri: "backend://worker-uplift/views/enrichment/article-001/v1",
            version: 1,
            digest: "sha256:enrichment"
          },
          approval: {
            uri: "backend://worker-uplift/views/approval/article-001/v1",
            version: 1,
            digest: "sha256:approval"
          },
          translations: [
            {
              languageCode: "fr",
              uri: "backend://worker-uplift/views/translation/article-001/fr/v1",
              version: 1,
              digest: "sha256:translation-fr"
            },
            {
              languageCode: "ja",
              uri: "backend://worker-uplift/views/translation/article-001/ja/v1",
              version: 1,
              digest: "sha256:translation-ja"
            }
          ]
        }
      }
    ],
    writeMode: "upsert",
    providerMode: "backend_postgres_primary",
    ...overrides
  };
}

export function createMinimalPersistenceDelivery(): RuntimeMessageDelivery {
  return {
    envelope: createMinimalPersistenceEnvelope(),
    payload: createMinimalPersistencePayload(),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function permissionProbe(
  status: PersistenceDependencyProbe["status"],
  summary: string,
  details: PersistencePermissionProbe["details"]
): PersistencePermissionProbe {
  return {
    status,
    summary: status === "ok" ? summary : `${summary} degraded`,
    details
  };
}
