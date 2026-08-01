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
  type RuntimeIdempotencyClaimReleaseResult,
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
  PersistenceFeedHealthProjectionStore,
  PersistenceFinalShadowTransactionRunner,
  PersistenceInboxClaimRenewalResult,
  PersistenceInboxStore,
  PersistenceInboxFingerprintResult,
  PersistencePermissionProbe,
  PersistenceStageViewReader,
  PersistenceWorkHandler,
  PersistenceWorkTools
} from "./dependencies.js";
import { sha256Digest } from "./digest.js";
import {
  PersistenceTransientError
} from "./errors.js";
import { createPersistenceRoutingWorkHandler } from "./feed-health-projection.js";
import type {
  FeedHealthLegacyRow,
  FeedHealthProjectionEvent,
  FeedHealthProjectionState,
  FeedHealthProjectionWriteResult,
  FeedQualityLegacyRow
} from "./feed-health-types.js";
import type {
  PersistenceBackendShadowAggregateCommand,
  PersistenceBackendShadowAggregateResult,
  PersistenceBackendPrimaryWriteResult,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalMaterializationWriteResult,
  PersistenceQuarantineRecord,
  PersistenceSaveAcceptedArticleCommand,
  PersistenceSaveArticleSummariesCommand
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
  private readonly ownedClaims = new Map<string, string>();

  constructor(clock: RuntimeClock = new ManualPersistenceClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local persistence inbox ready" : "local persistence inbox degraded"
    };
  }

  async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext,
    payloadFingerprint?: string
  ): Promise<RuntimeIdempotencyClaimResult> {
    void payloadFingerprint;
    const claim = await this.store.claim(idempotencyKey, context);

    if (claim.status === "claimed") {
      this.ownedClaims.set(idempotencyKey, claim.claimToken);
    }

    return claim;
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

  renewClaim(idempotencyKey: string, claimToken: string): Promise<PersistenceInboxClaimRenewalResult> {
    return Promise.resolve(this.ownedClaims.get(idempotencyKey) === claimToken
      ? {
          status: "renewed"
        }
      : {
          status: "not-owned"
        });
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    await this.store.markCompleted(idempotencyKey, completion);
    this.clearOwnedClaim(idempotencyKey, completion.claimToken);
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    await this.store.markFailed(idempotencyKey, failure);
    this.clearOwnedClaim(idempotencyKey, failure.claimToken);
  }

  releaseClaim(
    idempotencyKey: string,
    failure: RuntimeIdempotencyFailure
  ): Promise<RuntimeIdempotencyClaimReleaseResult> {
    return this.store.releaseClaim(idempotencyKey, failure).then((result) => {
      if (result.status === "released") {
        this.clearOwnedClaim(idempotencyKey, failure.claimToken);
      }

      return result;
    });
  }

  private clearOwnedClaim(idempotencyKey: string, claimToken: string): void {
    if (this.ownedClaims.get(idempotencyKey) === claimToken) {
      this.ownedClaims.delete(idempotencyKey);
    }
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
  failNextCommit = false;
  nextRecordError: Error | undefined;
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

      if (this.failNextCommit) {
        this.failNextCommit = false;
        throw new PersistenceTransientError("database-commit-lost");
      }

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
    if (this.nextRecordError !== undefined) {
      const error = this.nextRecordError;

      this.nextRecordError = undefined;
      return Promise.reject(error);
    }

    if (this.failNextRecord) {
      this.failNextRecord = false;
      return Promise.reject(new PersistenceTransientError("serialization-failure"));
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
        aggregate: existingReceipt.aggregate,
        publicationReadinessCommand: existingReceipt.publicationReadinessCommand
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
  productionDomainWritesEnabled = false;
  readonly shadowAggregateCommands: PersistenceBackendShadowAggregateCommand[] = [];
  readonly acceptedArticleCommands: PersistenceSaveAcceptedArticleCommand[] = [];
  readonly articleSummaryCommands: PersistenceSaveArticleSummariesCommand[] = [];
  failNextShadowAggregate = false;
  nextShadowAggregateError: Error | undefined;
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
      productionDomainWritesEnabled: this.productionDomainWritesEnabled
    };
  }

  recordShadowAggregate(command: PersistenceBackendShadowAggregateCommand): PersistenceBackendShadowAggregateResult {
    if (this.nextShadowAggregateError !== undefined) {
      const error = this.nextShadowAggregateError;

      this.nextShadowAggregateError = undefined;
      throw error;
    }

    if (this.failNextShadowAggregate) {
      this.failNextShadowAggregate = false;
      throw new PersistenceTransientError("backend-api-transient");
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

  saveAcceptedArticle(command: PersistenceSaveAcceptedArticleCommand): PersistenceBackendPrimaryWriteResult {
    return this.recordPrimary(command, this.acceptedArticleCommands, 1);
  }

  saveArticleSummaries(command: PersistenceSaveArticleSummariesCommand): PersistenceBackendPrimaryWriteResult {
    return this.recordPrimary(command, this.articleSummaryCommands, command.summaries.length);
  }

  private recordPrimary<T extends PersistenceSaveAcceptedArticleCommand | PersistenceSaveArticleSummariesCommand>(
    command: T,
    commands: T[],
    affectedCount: number
  ): PersistenceBackendPrimaryWriteResult {
    const payloadDigest = sha256Digest(command);
    const existingDigest = this.receipts.get(command.idempotencyKey);

    if (existingDigest !== undefined) {
      if (existingDigest !== payloadDigest) {
        throw new Error("backend-idempotency-conflict");
      }
      return {
        status: "duplicate",
        affectedCount,
        response: {
          ok: true,
          duplicate: true
        }
      };
    }
    this.receipts.set(command.idempotencyKey, payloadDigest);
    commands.push(command);
    return {
      status: "recorded",
      affectedCount,
      response: {
        ok: true
      }
    };
  }
}

export class LocalPersistenceBrokerOutbox implements PersistenceBrokerOutbox {
  readonly name: string = "local-persistence-broker-outbox";
  status: PersistenceDependencyProbe["status"] = "ok";
  readonly records: { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] = [];
  failNextRecord = false;

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local persistence broker outbox ready" : "local persistence broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    if (this.failNextRecord) {
      this.failNextRecord = false;
      return Promise.reject(new PersistenceTransientError("outbox-receipt-write-lost"));
    }

    this.records.push({
      command,
      receipt
    });
    return Promise.resolve();
  }

  hasReceipt(command: BrokerPublishCommand): Promise<boolean> {
    return Promise.resolve(this.records.some((record) => record.command.envelope.idempotencyKey === command.envelope.idempotencyKey));
  }
}

export class LocalFeedHealthProjectionStore implements PersistenceFeedHealthProjectionStore {
  readonly name = "local-feed-health-projection-store";
  status: PersistenceDependencyProbe["status"] = "ok";
  readonly projectedEvents: FeedHealthProjectionEvent[] = [];
  readonly staleEvents: FeedHealthProjectionEvent[] = [];
  private readonly receipts = new Map<string, string>();
  private readonly states = new Map<string, FeedHealthProjectionState>();

  probe(): PersistenceDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local feed-health projection store ready" : "local feed-health projection store degraded"
    };
  }

  project(event: FeedHealthProjectionEvent): Promise<FeedHealthProjectionWriteResult> {
    const existingDigest = this.receipts.get(event.idempotencyKey);

    if (existingDigest !== undefined) {
      if (existingDigest !== event.payloadDigest) {
        return Promise.resolve({
          status: "conflict",
          reason: "feed-health-idempotency-conflict"
        });
      }

      const state = this.states.get(event.feedKey) ?? createInitialFeedHealthState(event);
      return Promise.resolve({
        status: "duplicate",
        state
      });
    }

    const current = this.states.get(event.feedKey);

    if (current !== undefined && event.eventVersion <= current.lastEventVersion) {
      this.receipts.set(event.idempotencyKey, event.payloadDigest);
      this.staleEvents.push(event);
      return Promise.resolve({
        status: "stale",
        state: current
      });
    }

    const next = applyFeedHealthEvent(current ?? createInitialFeedHealthState(event), event);

    this.receipts.set(event.idempotencyKey, event.payloadDigest);
    this.states.set(event.feedKey, next);
    this.projectedEvents.push(event);
    return Promise.resolve({
      status: "projected",
      state: next,
      feedHealthRow: toFeedHealthLegacyRow(next),
      feedQualityRow: toFeedQualityLegacyRow(next)
    });
  }

  readLegacyFeedHealthRows(): Promise<readonly FeedHealthLegacyRow[]> {
    return Promise.resolve(Array.from(this.states.values()).map(toFeedHealthLegacyRow));
  }

  readLegacyFeedQualityRows(): Promise<readonly FeedQualityLegacyRow[]> {
    return Promise.resolve(Array.from(this.states.values()).map(toFeedQualityLegacyRow));
  }
}

function createInitialFeedHealthState(event: FeedHealthProjectionEvent): FeedHealthProjectionState {
  return {
    feedKey: event.feedKey,
    source: event.source,
    feedUrl: event.feedUrl,
    lastEventVersion: 0,
    lastStatus: "duplicate",
    lastItemCount: 0,
    lastDuplicateCount: 0,
    lastImageCount: 0,
    lastEligibleCount: 0,
    lastRejectedCount: 0,
    lastAcceptedCount: 0,
    consecutiveFailureCount: 0,
    totalFetchCount: 0,
    totalSuccessCount: 0,
    totalFailureCount: 0,
    totalItemCount: 0,
    totalDuplicateCount: 0,
    totalImageCount: 0,
    totalEligibleCount: 0,
    totalRejectedCount: 0,
    totalAcceptedCount: 0,
    totalLatencyMs: 0,
    updatedAt: event.occurredAt,
    backoffRecommendation: "none",
    errorSamples: []
  };
}

function applyFeedHealthEvent(
  state: FeedHealthProjectionState,
  event: FeedHealthProjectionEvent
): FeedHealthProjectionState {
  const isFetchAttempt = event.outcomeKind === "fetch_attempt";
  const isFailure = event.outcomeStatus === "failure";
  const isSuccess = event.outcomeStatus === "success";
  const errorSample = event.errorClass === undefined
    ? []
    : [
        {
          occurredAt: event.occurredAt,
          outcomeStage: event.outcomeStage,
          outcomeKind: event.outcomeKind,
          errorClass: event.errorClass
        }
      ];
  const errorSamples = [
    ...state.errorSamples,
    ...errorSample
  ].slice(-5);
  const consecutiveFailureCount = isFailure
    ? state.consecutiveFailureCount + 1
    : isSuccess
      ? 0
      : state.consecutiveFailureCount;

  return {
    feedKey: state.feedKey,
    source: event.source,
    feedUrl: event.feedUrl,
    lastEventVersion: event.eventVersion,
    lastAttemptAt: event.occurredAt,
    lastStatus: event.outcomeStatus,
    lastItemCount: event.counts.itemCount,
    lastDuplicateCount: event.counts.duplicateCount,
    lastImageCount: event.counts.imageCount,
    lastEligibleCount: event.counts.eligibleCount,
    lastRejectedCount: event.counts.rejectedCount,
    lastAcceptedCount: event.counts.acceptedCount,
    consecutiveFailureCount,
    totalFetchCount: state.totalFetchCount + (isFetchAttempt ? 1 : 0),
    totalSuccessCount: state.totalSuccessCount + (isFetchAttempt && isSuccess ? 1 : 0),
    totalFailureCount: state.totalFailureCount + (isFailure ? 1 : 0),
    totalItemCount: state.totalItemCount + event.counts.itemCount,
    totalDuplicateCount: state.totalDuplicateCount + event.counts.duplicateCount,
    totalImageCount: state.totalImageCount + event.counts.imageCount,
    totalEligibleCount: state.totalEligibleCount + event.counts.eligibleCount,
    totalRejectedCount: state.totalRejectedCount + event.counts.rejectedCount,
    totalAcceptedCount: state.totalAcceptedCount + event.counts.acceptedCount,
    totalLatencyMs: state.totalLatencyMs + event.latencyMs,
    updatedAt: event.occurredAt,
    backoffRecommendation: event.backoffRecommendation,
    errorSamples,
    ...(isSuccess ? {
      lastSuccessAt: event.occurredAt
    } : state.lastSuccessAt === undefined ? {} : {
      lastSuccessAt: state.lastSuccessAt
    }),
    ...(isFailure ? {
      lastFailureAt: event.occurredAt,
      lastErrorClass: event.errorClass ?? "unknown_failure"
    } : {
      ...(state.lastFailureAt === undefined ? {} : {
        lastFailureAt: state.lastFailureAt
      }),
      ...(state.lastErrorClass === undefined ? {} : {
        lastErrorClass: state.lastErrorClass
      })
    })
  };
}

function toFeedHealthLegacyRow(state: FeedHealthProjectionState): FeedHealthLegacyRow {
  return {
    source: state.source,
    feed_url: state.feedUrl,
    last_checked_at: state.lastAttemptAt ?? state.updatedAt,
    last_success_at: state.lastSuccessAt ?? null,
    last_failure_at: state.lastFailureAt ?? null,
    last_status: state.lastStatus,
    last_error_message: state.lastErrorClass ?? null,
    last_article_count: state.lastItemCount,
    last_image_count: state.lastImageCount,
    last_accepted_count: state.lastAcceptedCount,
    last_rejected_count: state.lastRejectedCount,
    consecutive_failure_count: state.consecutiveFailureCount,
    total_fetch_count: state.totalFetchCount,
    total_success_count: state.totalSuccessCount,
    total_failure_count: state.totalFailureCount,
    total_article_count: state.totalItemCount,
    total_image_count: state.totalImageCount,
    total_accepted_count: state.totalAcceptedCount,
    total_rejected_count: state.totalRejectedCount,
    updated_at: state.updatedAt
  };
}

function toFeedQualityLegacyRow(state: FeedHealthProjectionState): FeedQualityLegacyRow {
  const successRate = pct(state.totalSuccessCount, state.totalFetchCount);
  const failureRate = pct(state.totalFailureCount, Math.max(1, state.totalFetchCount + state.totalFailureCount));
  const duplicateRate = pct(state.totalDuplicateCount, state.totalItemCount + state.totalDuplicateCount);
  const thumbnailRate = pct(state.totalImageCount, state.totalItemCount);
  const acceptedRate = pct(state.totalAcceptedCount, state.totalEligibleCount);
  const qualityScore = Math.max(0, Math.min(100, successRate + thumbnailRate + acceptedRate - failureRate - duplicateRate));

  return {
    source: state.source,
    feed_url: state.feedUrl,
    quality_score: round2(qualityScore),
    success_rate: successRate,
    thumbnail_rate: thumbnailRate,
    accepted_rate: acceptedRate,
    failure_rate: failureRate,
    duplicate_rate: duplicateRate,
    total_fetch_count: state.totalFetchCount,
    total_success_count: state.totalSuccessCount,
    total_failure_count: state.totalFailureCount,
    total_article_count: state.totalItemCount,
    total_image_count: state.totalImageCount,
    total_accepted_count: state.totalAcceptedCount,
    total_rejected_count: state.totalRejectedCount,
    unique_reviewed_url_count: state.totalEligibleCount,
    unique_published_url_count: state.totalAcceptedCount
  };
}

function pct(numerator: number, denominator: number): number {
  return denominator <= 0 ? 0 : round2((numerator / denominator) * 100);
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
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
    tools.signal.throwIfAborted();
    this.onHandleStart?.();
    await this.handleGate;
    tools.signal.throwIfAborted();
    this.handled.push(context);

    return this.result;
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly inFlightDeliveryCount = 0;
  readonly assertedRoutes: WorkerRoute[] = [];
  readonly published: BrokerPublishCommand[] = [];
  failNextPublish = false;
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

    if (this.failNextPublish) {
      this.failNextPublish = false;
      return Promise.reject(new PersistenceTransientError("broker-publish-not-confirmed"));
    }

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
      canonicalUrl: "https://publisher.example.test/community/article-001",
      title: "Neighbors build a free community library",
      description: "Residents created a shared library for local families.",
      imageUrl: "https://publisher.example.test/images/article-001.jpg",
      publishedAt: "2026-07-23T00:00:00.000Z",
      category: "Community | Uplifting",
      sourceSummary: "Neighbors created a free library that gives local families easier access to books.",
      sourceLanguage: "en",
      model: "qwen2.5:3b",
      positivityScore: 8.5,
      approvalVersion: 1
    },
    translations: [
      {
        articleId: "article-001",
        articleVersion: 1,
        languageCode: "fr",
        sourceLanguage: "en",
        title: "Des voisins créent une bibliothèque gratuite",
        summary: "Des voisins ont créé une bibliothèque gratuite pour les familles du quartier.",
        model: "qwen2.5:3b",
        summaryRef: "backend://worker-uplift/translation/article-001/fr/v1",
        qualityStatus: "accepted",
        translationVersion: 1
      },
      {
        articleId: "article-001",
        articleVersion: 1,
        languageCode: "ja",
        sourceLanguage: "en",
        title: "住民が無料の地域図書館を開設",
        summary: "住民が地域の家族のために無料の図書館を作りました。",
        model: "qwen2.5:3b",
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
  readonly productionDomainWritesEnabled?: boolean;
} = {}): PersistenceDependencies {
  const clock = options.clock ?? new ManualPersistenceClock();
  const backendApiClient = new LocalBackendWorkerApiClient();

  backendApiClient.productionDomainWritesEnabled = options.productionDomainWritesEnabled ?? false;
  const dependencies: PersistenceDependencies = {
    adapterMode: "in_memory",
    stateStoreMode: "in_memory",
    productionDomainWritesEnabled: options.productionDomainWritesEnabled ?? false,
    clock,
    inboxStore: new InMemoryPersistenceInboxStore(clock),
    finalShadowTransactions: new LocalFinalShadowTransactionRunner(),
    stageViewReader: new LocalStageViewReader(),
    brokerOutbox: new LocalPersistenceBrokerOutbox(),
    feedHealthProjectionStore: new LocalFeedHealthProjectionStore(),
    brokerTransport: new LocalBrokerTransport(),
    backendApiClient,
    workHandler: options.workHandler ?? new LocalPersistenceWorkHandler()
  };

  if (options.workHandler !== undefined) {
    return dependencies;
  }

  return {
    ...dependencies,
    workHandler: createPersistenceRoutingWorkHandler(dependencies)
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

export function createMinimalFeedHealthProjectionEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("persistence");
  const occurredAt = "2026-07-23T00:00:00.000Z";
  const envelope = {
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "persistence",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b6801",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b6701",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b6601",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed-health:example-feed:v1",
    aggregate: {
      type: "feed",
      id: "example-feed",
      version: 1
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: "fetcher",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/projections/feed-health/example-feed/v1",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(createMinimalFeedHealthProjectionPayload())
    },
    ...overrides
  };

  return assertWorkerEnvelope(envelope);
}

export function createMinimalFeedHealthProjectionPayload(
  overrides: Readonly<Record<string, unknown>> = {}
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b6702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b6701",
    idempotencyKey: "feed-health:example-feed:v1",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    commandId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b6703",
    commandKind: "save_feed_health",
    backendOperation: "save-feed-health-batch",
    entityRefs: [
      {
        projectionKind: "feed_health",
        projectionId: "example-feed:v1",
        feedKey: "example-feed",
        feedUrl: "https://example.com/feed.xml",
        source: "Example",
        outcomeStage: "fetcher",
        outcomeKind: "fetch_attempt",
        outcomeStatus: "success",
        eventVersion: 1,
        occurredAt: "2026-07-23T00:00:00.000Z",
        latencyMs: 250,
        counts: {
          itemCount: 10,
          duplicateCount: 2,
          imageCount: 8,
          eligibleCount: 6,
          rejectedCount: 1,
          acceptedCount: 5
        },
        backoffRecommendation: "none"
      }
    ],
    writeMode: "upsert",
    providerMode: "backend_postgres_primary",
    ...overrides
  };
}

export function createMinimalFeedHealthProjectionDelivery(): RuntimeMessageDelivery {
  return {
    envelope: createMinimalFeedHealthProjectionEnvelope(),
    payload: createMinimalFeedHealthProjectionPayload(),
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
