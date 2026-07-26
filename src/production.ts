import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  WORKER_DELIVERY_BEHAVIOR,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload,
  validateWorkerEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  runtimeNow
} from "@ramideltoro/nutsnews-worker-runtime";
import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeIdempotencyClaimContext,
  RuntimeIdempotencyClaimResult,
  RuntimeIdempotencyCompletion,
  RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type { PersistenceConfig } from "./config.js";
import type {
  FeedHealthLegacyRow,
  FeedHealthProjectionEvent,
  FeedHealthProjectionState,
  FeedHealthProjectionWriteResult,
  FeedQualityLegacyRow
} from "./feed-health-types.js";
import { createPersistenceRoutingWorkHandler } from "./feed-health-projection.js";
import { PersistencePermanentError, PersistenceTransientError } from "./errors.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-payload-transport.js";
import { sha256Digest } from "./digest.js";
import {
  PERSISTENCE_RECONCILIATION_CONFIRMATION,
  type PersistenceReconciliationCandidate,
  type PersistenceReconciliationReport,
  type PersistenceReconciliationRequest,
  type PersistenceReconciler
} from "./reconciliation.js";
import type {
  PersistenceBackendApiCompatibility,
  PersistenceBackendWorkerApiClient,
  PersistenceBrokerOutbox,
  PersistenceDatabaseTransaction,
  PersistenceDependencies,
  PersistenceDependencyProbe,
  PersistenceFeedHealthProjectionStore,
  PersistenceFinalShadowTransactionRunner,
  PersistenceInboxFingerprintResult,
  PersistenceInboxStore,
  PersistencePermissionProbe,
  PersistenceStageViewReader,
  PersistenceWorkHandler
} from "./dependencies.js";
import type {
  PersistenceBackendShadowAggregateCommand,
  PersistenceBackendShadowAggregateResult,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalMaterializationWriteResult,
  PersistenceQuarantineRecord
} from "./materialization-types.js";

const PERSISTENCE_SCHEMA = "worker_uplift_persistence";
const FINAL_SCHEMA = "worker_uplift_final";
const VIEWS_SCHEMA = "worker_uplift_views";

export type ProductionPersistenceDependencies = PersistenceDependencies & {
  readonly reconciler: PersistenceReconciler;
  readonly reconciliationToken?: string;
  close(): Promise<void>;
};

interface ProductionPersistenceDependencyOptions {
  readonly config: PersistenceConfig;
  readonly clock: RuntimeClock;
  readonly env?: NodeJS.ProcessEnv;
  readonly workHandler?: PersistenceWorkHandler;
}

interface PgPersistenceTransaction extends PersistenceDatabaseTransaction {
  readonly client: PoolClient;
}

interface CanonicalProjectionRow extends QueryResultRow {
  readonly article_identity_hash: string;
  readonly canonical_url_hash: string;
  readonly original_url_hash: string;
  readonly source_feed_url: string | null;
  readonly operation_version: number;
}

interface EnrichmentProjectionRow extends QueryResultRow {
  readonly article_identity_hash: string;
  readonly enrichment_version: number;
  readonly title_ref: string | null;
  readonly image_url_ref: string | null;
  readonly category: string | null;
}

interface ApprovalProjectionRow extends QueryResultRow {
  readonly article_identity_hash: string;
  readonly approval_version: number;
  readonly decision: string;
  readonly positivity_score: string | number | null;
}

interface TranslationProjectionRow extends QueryResultRow {
  readonly article_identity_hash: string;
  readonly language_code: string;
  readonly translation_version: number;
  readonly summary_ref: string | null;
  readonly quality_status: string;
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export function createProductionPersistenceDependencies(
  options: ProductionPersistenceDependencyOptions
): ProductionPersistenceDependencies {
  const env = options.env ?? process.env;
  const pool = new Pool({
    connectionString: requiredEnv(env, "NUTSNEWS_PERSISTENCE_DATABASE_URL"),
    max: Math.max(3, options.config.concurrency + 2),
    application_name: options.config.serviceName
  });
  const brokerTransport = new PayloadRabbitMqTransport({
    url: requiredEnv(env, "NUTSNEWS_PERSISTENCE_RABBITMQ_URL"),
    prefetch: options.config.prefetch,
    clock: options.clock
  });
  const inboxStore = new PostgresPersistenceInboxStore(pool);
  const finalShadowTransactions = new PostgresPersistenceFinalShadowTransactionRunner(pool, options.config);
  const stageViewReader = new PostgresPersistenceStageViewReader(pool, options.config);
  const brokerOutbox = new PostgresPersistenceBrokerOutbox(pool);
  const reconciler = new PostgresPersistenceOutboxReconciler({
    pool,
    brokerTransport,
    clock: options.clock,
    env
  });
  const reconciliationToken = reconciliationTokenFromEnv(env);
  const feedHealthProjectionStore = new PostgresPersistenceFeedHealthProjectionStore(pool);
  const backendApiClient = new HttpPersistenceBackendWorkerApiClient({
    baseUrl: requiredEnv(env, "NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL"),
    token: requiredEnv(env, "NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN"),
    expectedVersion: options.config.compatibility.backendApiVersion
  });
  const dependencies: Omit<ProductionPersistenceDependencies, "workHandler" | "close"> = {
    clock: options.clock,
    inboxStore,
    finalShadowTransactions,
    stageViewReader,
    brokerOutbox,
    reconciler,
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    }),
    feedHealthProjectionStore,
    brokerTransport,
    backendApiClient
  };

  return {
    ...dependencies,
    workHandler: options.workHandler ?? createPersistenceRoutingWorkHandler(dependencies as unknown as PersistenceDependencies),
    async close(): Promise<void> {
      await brokerTransport.close();
      await pool.end();
    }
  };
}

export class PostgresPersistenceInboxStore implements PersistenceInboxStore {
  readonly name = "postgres-persistence-inbox";
  private readonly pendingFingerprints = new Map<string, string>();

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<PersistenceDependencyProbe> {
    return probePool(this.pool, "persistence inbox database ready");
  }

  async verifyPayloadFingerprint(idempotencyKey: string, fingerprint: string): Promise<PersistenceInboxFingerprintResult> {
    const existing = await this.pool.query<{ readonly payload_digest: string }>(
      `SELECT payload_digest
       FROM ${PERSISTENCE_SCHEMA}.inbox
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );
    const row = existing.rows[0];

    if (row !== undefined) {
      return row.payload_digest === fingerprint
        ? {
            status: "duplicate"
          }
        : {
            status: "conflict",
            existingFingerprint: row.payload_digest
          };
    }

    this.pendingFingerprints.set(idempotencyKey, fingerprint);
    return {
      status: "accepted"
    };
  }

  async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    const payloadDigest = this.pendingFingerprints.get(idempotencyKey)
      ?? context.envelope.payloadRef.digest
      ?? sha256Digest(context.envelope.payloadRef);
    const inserted = await this.pool.query<{ readonly received_at: Date }>(
      `INSERT INTO ${PERSISTENCE_SCHEMA}.inbox (
        message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, received_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, 'processing', $14::jsonb)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING received_at`,
      [
        context.envelope.messageId,
        context.envelope.correlationId,
        context.envelope.messageId,
        context.envelope.producer.name,
        context.envelope.causationId,
        context.envelope.aggregate.type,
        context.envelope.aggregate.id,
        context.envelope.schemaVersion,
        Math.max(1, context.envelope.aggregate.version),
        idempotencyKey,
        context.envelope.payloadRef.uri,
        payloadDigest,
        context.receivedAt,
        JSON.stringify({
          route: context.envelope.route,
          attempt: context.envelope.attempt,
          payloadFingerprint: payloadDigest
        })
      ]
    );

    this.pendingFingerprints.delete(idempotencyKey);

    if ((inserted.rowCount ?? 0) > 0) {
      return {
        status: "claimed",
        firstSeenAt: context.receivedAt,
        replay: false
      };
    }

    const existing = await this.pool.query<{
      readonly status: string;
      readonly received_at: Date;
      readonly processed_at: Date | null;
    }>(
      `SELECT status, received_at, processed_at
       FROM ${PERSISTENCE_SCHEMA}.inbox
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );
    const row = existing.rows[0];

    if (row === undefined) {
      return {
        status: "in-progress",
        firstSeenAt: context.receivedAt
      };
    }

    const firstSeenAt = row.received_at.toISOString();

    if (row.status === "processed" || row.status === "duplicate") {
      return {
        status: "already-completed",
        firstSeenAt,
        completedAt: (row.processed_at ?? row.received_at).toISOString()
      };
    }

    if (row.status === "failed" || row.status === "parked") {
      await this.pool.query(
        `UPDATE ${PERSISTENCE_SCHEMA}.inbox
         SET status = 'processing',
             sanitized_error_code = NULL,
             sanitized_error_message = NULL,
             diagnostic_metadata = diagnostic_metadata || $2::jsonb
         WHERE idempotency_key = $1`,
        [
          idempotencyKey,
          JSON.stringify({
            replayedAt: context.receivedAt,
            replayMessageId: context.envelope.messageId
          })
        ]
      );

      return {
        status: "claimed",
        firstSeenAt,
        replay: true
      };
    }

    return {
      status: "in-progress",
      firstSeenAt
    };
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    await this.pool.query(
      `UPDATE ${PERSISTENCE_SCHEMA}.inbox
       SET status = 'processed',
           processed_at = $2::timestamptz,
           diagnostic_metadata = diagnostic_metadata || $3::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        completion.completedAt,
        JSON.stringify({
          completedMessageId: completion.messageId,
          completedStage: completion.stage
        })
      ]
    );
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    await this.pool.query(
      `UPDATE ${PERSISTENCE_SCHEMA}.inbox
       SET status = 'failed',
           sanitized_error_code = $2,
           sanitized_error_message = $3,
           diagnostic_metadata = diagnostic_metadata || $4::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        sanitizeCode(failure.reason),
        sanitizeMessage(failure.reason),
        JSON.stringify({
          failedAt: failure.failedAt,
          failedMessageId: failure.messageId,
          retryable: failure.retryable
        })
      ]
    );
  }
}

export class PostgresPersistenceFinalShadowTransactionRunner implements PersistenceFinalShadowTransactionRunner {
  readonly name = "postgres-final-shadow-transactions";

  constructor(
    private readonly pool: Pool,
    private readonly config: PersistenceConfig
  ) {}

  async probe(): Promise<PersistenceDependencyProbe> {
    return probePool(this.pool, "final shadow database ready");
  }

  async checkWriteScope(): Promise<PersistencePermissionProbe> {
    const probe = await probePool(this.pool, "final shadow write scope ready");

    return permissionProbe(probe.status, probe.summary, {
      databaseRole: this.config.security.databaseRole,
      shadowSchemaVersion: this.config.compatibility.shadowSchemaVersion,
      allowedWriteScopes: [
        `${FINAL_SCHEMA}.article_shadow_aggregates`,
        `${PERSISTENCE_SCHEMA}.inbox`,
        `${PERSISTENCE_SCHEMA}.outbox`,
        `${PERSISTENCE_SCHEMA}.write_requests`
      ],
      allowedReadScopes: [
        `${VIEWS_SCHEMA}.canonical_article_projection`,
        `${VIEWS_SCHEMA}.enrichment_projection`,
        `${VIEWS_SCHEMA}.approval_projection`,
        `${VIEWS_SCHEMA}.translation_coverage_projection`
      ],
      deniedWriteScopes: [
        "public.domain_tables",
        "public.public_feed_snapshot",
        "worker_uplift_approval.approval_decisions",
        "worker_uplift_translation.translation_records"
      ]
    });
  }

  async withTransaction<T>(operation: (transaction: PersistenceDatabaseTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    const transaction: PgPersistenceTransaction = {
      transactionId: randomUUID(),
      client
    };

    try {
      await client.query("BEGIN");
      const value = await operation(transaction);
      await client.query("COMMIT");
      return value;
    } catch (error: unknown) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async recordFinalMaterialization(
    transaction: PersistenceDatabaseTransaction,
    record: PersistenceFinalMaterializationRecord
  ): Promise<PersistenceFinalMaterializationWriteResult> {
    const client = transactionClient(transaction);
    const stale = await client.query<{ readonly aggregate_version: number | null }>(
      `SELECT max(aggregate_version)::integer AS aggregate_version
       FROM ${FINAL_SCHEMA}.article_shadow_aggregates
       WHERE article_identity_hash = $1`,
      [record.aggregate.articleIdentityHash]
    );
    const latestVersion = stale.rows[0]?.aggregate_version;

    if (latestVersion !== null && latestVersion !== undefined && latestVersion > record.aggregate.aggregateVersion) {
      return {
        status: "stale",
        reason: "stale-final-aggregate-version"
      };
    }

    const existing = await client.query<{ readonly payload_digest: string }>(
      `SELECT payload_digest
       FROM ${FINAL_SCHEMA}.article_shadow_aggregates
       WHERE article_identity_hash = $1
         AND aggregate_version = $2`,
      [
        record.aggregate.articleIdentityHash,
        record.aggregate.aggregateVersion
      ]
    );
    const existingAggregate = existing.rows[0];

    if (existingAggregate !== undefined) {
      if (existingAggregate.payload_digest !== record.aggregate.payloadDigest) {
        return {
          status: "conflict",
          reason: "conflicting-final-shadow-aggregate"
        };
      }

      await this.recordWriteRequest(client, record, "accepted", true);
      return {
        status: "duplicate",
        aggregate: record.aggregate,
        publicationReadinessCommand: record.publicationReadinessCommand
      };
    }

    await client.query(
      `INSERT INTO ${FINAL_SCHEMA}.article_shadow_aggregates (
        article_identity_hash, canonical_url_hash, original_url_hash, aggregate_version,
        source_feed_url, title_ref, image_url_ref, category, positivity_score,
        approval_version, translation_languages, publication_status, payload_ref,
        payload_digest, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::text[], $12, $13, $14, $15::jsonb)
      ON CONFLICT (article_identity_hash, aggregate_version)
      DO UPDATE SET payload_digest = EXCLUDED.payload_digest,
                    diagnostic_metadata = ${FINAL_SCHEMA}.article_shadow_aggregates.diagnostic_metadata || EXCLUDED.diagnostic_metadata,
                    updated_at = now()`,
      [
        record.aggregate.articleIdentityHash,
        record.aggregate.canonicalUrlHash,
        record.aggregate.originalUrlHash,
        record.aggregate.aggregateVersion,
        record.aggregate.sourceFeedUrl ?? null,
        record.aggregate.titleRef,
        record.aggregate.imageUrlRef,
        record.aggregate.category ?? null,
        record.aggregate.positivityScore ?? null,
        record.aggregate.approvalVersion,
        record.aggregate.translationLanguages,
        record.aggregate.publicationStatus,
        record.aggregate.payloadRef,
        record.aggregate.payloadDigest,
        JSON.stringify(record.aggregate.diagnosticMetadata)
      ]
    );
    await this.recordWriteRequest(client, record, "accepted", false);

    return {
      status: "recorded",
      aggregate: record.aggregate,
      publicationReadinessCommand: record.publicationReadinessCommand
    };
  }

  async recordQuarantine(transaction: PersistenceDatabaseTransaction, record: PersistenceQuarantineRecord): Promise<void> {
    const client = transactionClient(transaction);
    const syntheticIdentity = `quarantine:${shortHash(record.idempotencyKey)}`;

    await client.query(
      `INSERT INTO ${PERSISTENCE_SCHEMA}.write_requests (
        article_identity_hash, request_kind, operation_version, backend_api_operation,
        request_ref, status, diagnostic_metadata
      ) VALUES ($1, 'article', $2, $3, $4, 'failed', $5::jsonb)
      ON CONFLICT (article_identity_hash, request_kind, operation_version)
      DO UPDATE SET status = 'failed',
                    diagnostic_metadata = ${PERSISTENCE_SCHEMA}.write_requests.diagnostic_metadata || EXCLUDED.diagnostic_metadata`,
      [
        record.articleId ?? syntheticIdentity,
        Math.max(1, record.articleVersion ?? 1),
        `quarantine:${record.reason}`,
        `backend://worker-uplift/persistence/quarantine/${encodeURIComponent(record.idempotencyKey)}`,
        JSON.stringify({
          ...record.diagnosticMetadata,
          idempotencyKey: record.idempotencyKey,
          messageId: record.messageId,
          correlationId: record.correlationId,
          reason: record.reason,
          payloadDigest: record.payloadDigest,
          safeMetadataOnly: true
        })
      ]
    );
  }

  private async recordWriteRequest(
    client: PoolClient,
    record: PersistenceFinalMaterializationRecord,
    status: "accepted" | "retrying",
    duplicate: boolean
  ): Promise<void> {
    await client.query(
      `INSERT INTO ${PERSISTENCE_SCHEMA}.write_requests (
        article_identity_hash, request_kind, operation_version, backend_api_operation,
        request_ref, response_ref, status, diagnostic_metadata
      ) VALUES ($1, 'article', $2, $3, $4, $5, $6, $7::jsonb)
      ON CONFLICT (article_identity_hash, request_kind, operation_version)
      DO UPDATE SET response_ref = EXCLUDED.response_ref,
                    status = EXCLUDED.status,
                    diagnostic_metadata = ${PERSISTENCE_SCHEMA}.write_requests.diagnostic_metadata || EXCLUDED.diagnostic_metadata`,
      [
        record.aggregate.articleIdentityHash,
        record.aggregate.aggregateVersion,
        record.request.backendOperation,
        record.aggregate.payloadRef,
        `${record.aggregate.payloadRef}/publication-readiness`,
        status,
        JSON.stringify({
          audit: record.audit,
          commandId: record.request.commandId,
          idempotencyKey: record.request.idempotencyKey,
          duplicate,
          payloadDigest: record.aggregate.payloadDigest,
          publicationReadinessIdempotencyKey: record.publicationReadinessCommand.envelope.idempotencyKey,
          safeMetadataOnly: true
        })
      ]
    );
  }
}

export class PostgresPersistenceStageViewReader implements PersistenceStageViewReader {
  readonly name = "postgres-stage-view-reader";

  constructor(
    private readonly pool: Pool,
    private readonly config: PersistenceConfig
  ) {}

  async probe(): Promise<PersistenceDependencyProbe> {
    return probePool(this.pool, "approved stage views ready");
  }

  async checkReadScope(): Promise<PersistencePermissionProbe> {
    const probe = await probePool(this.pool, "approved stage view read scope ready");

    return permissionProbe(probe.status, probe.summary, {
      databaseRole: this.config.security.databaseRole,
      shadowSchemaVersion: this.config.compatibility.shadowSchemaVersion,
      allowedWriteScopes: [],
      allowedReadScopes: [
        `${VIEWS_SCHEMA}.canonical_article_projection`,
        `${VIEWS_SCHEMA}.enrichment_projection`,
        `${VIEWS_SCHEMA}.approval_projection`,
        `${VIEWS_SCHEMA}.translation_coverage_projection`,
        `${VIEWS_SCHEMA}.final_shadow_article_projection`
      ],
      deniedWriteScopes: [
        "worker_uplift_approval.approval_decisions",
        "worker_uplift_translation.translation_records",
        "public.domain_tables"
      ]
    });
  }

  async readFinalMaterializationInputs(
    request: PersistenceFinalMaterializationRequest
  ): Promise<PersistenceFinalMaterializationInputs> {
    const [
      canonical,
      enrichment,
      approval,
      translations
    ] = await Promise.all([
      this.readCanonical(request),
      this.readEnrichment(request),
      this.readApproval(request),
      this.readTranslations(request)
    ]);

    return {
      canonical,
      enrichment,
      approval,
      translations
    };
  }

  private async readCanonical(request: PersistenceFinalMaterializationRequest): Promise<PersistenceFinalMaterializationInputs["canonical"]> {
    const result = await this.pool.query<CanonicalProjectionRow>(
      `SELECT article_identity_hash, canonical_url_hash, original_url_hash, source_feed_url, operation_version
       FROM ${VIEWS_SCHEMA}.canonical_article_projection
       WHERE (article_identity_hash = $1 OR canonical_url_hash = $1)
         AND operation_version = $2
       ORDER BY operation_version DESC
       LIMIT 1`,
      [
        request.articleId,
        request.stageRefs.canonical.version
      ]
    );
    const row = result.rows[0];

    if (row !== undefined) {
      return {
        articleId: request.articleId,
        articleVersion: request.articleVersion,
        articleIdentityHash: row.article_identity_hash,
        canonicalUrlHash: row.canonical_url_hash,
        originalUrlHash: row.original_url_hash,
        ...(row.source_feed_url === null ? {} : {
          sourceFeedUrl: row.source_feed_url
        }),
        resultVersion: row.operation_version
      };
    }

    return {
      articleId: request.articleId,
      articleVersion: request.articleVersion,
      articleIdentityHash: request.articleId,
      canonicalUrlHash: request.stageRefs.canonical.digest ?? shortHash(request.stageRefs.canonical.uri),
      originalUrlHash: request.stageRefs.canonical.digest ?? shortHash(`${request.stageRefs.canonical.uri}:original`),
      resultVersion: request.stageRefs.canonical.version
    };
  }

  private async readEnrichment(request: PersistenceFinalMaterializationRequest): Promise<PersistenceFinalMaterializationInputs["enrichment"]> {
    const result = await this.pool.query<EnrichmentProjectionRow>(
      `SELECT article_identity_hash, enrichment_version, title_ref, image_url_ref, category
       FROM ${VIEWS_SCHEMA}.enrichment_projection
       WHERE article_identity_hash = $1
         AND enrichment_version = $2
       ORDER BY enrichment_version DESC
       LIMIT 1`,
      [
        request.articleId,
        request.stageRefs.enrichment.version
      ]
    );
    const row = result.rows[0];

    return {
      articleId: request.articleId,
      articleVersion: request.articleVersion,
      titleRef: row?.title_ref ?? `${request.stageRefs.enrichment.uri}/title`,
      imageUrlRef: row?.image_url_ref ?? `${request.stageRefs.enrichment.uri}/image`,
      ...(row?.category === undefined || row.category === null ? {} : {
        category: row.category
      }),
      resultVersion: row?.enrichment_version ?? request.stageRefs.enrichment.version
    };
  }

  private async readApproval(request: PersistenceFinalMaterializationRequest): Promise<PersistenceFinalMaterializationInputs["approval"]> {
    const result = await this.pool.query<ApprovalProjectionRow>(
      `SELECT article_identity_hash, approval_version, decision, positivity_score
       FROM ${VIEWS_SCHEMA}.approval_projection
       WHERE article_identity_hash = $1
         AND approval_version = $2
       ORDER BY approval_version DESC
       LIMIT 1`,
      [
        request.articleId,
        request.stageRefs.approval.version
      ]
    );
    const row = result.rows[0];

    return {
      articleId: request.articleId,
      articleVersion: request.articleVersion,
      decision: approvalDecision(row?.decision),
      ...(row?.positivity_score === undefined || row.positivity_score === null ? {} : {
        positivityScore: Number(row.positivity_score)
      }),
      approvalVersion: row?.approval_version ?? request.stageRefs.approval.version
    };
  }

  private async readTranslations(request: PersistenceFinalMaterializationRequest): Promise<PersistenceFinalMaterializationInputs["translations"]> {
    const result = await this.pool.query<TranslationProjectionRow>(
      `SELECT article_identity_hash, language_code, translation_version, summary_ref, quality_status
       FROM ${VIEWS_SCHEMA}.translation_coverage_projection
       WHERE article_identity_hash = $1
         AND language_code = ANY($2::text[])`,
      [
        request.articleId,
        request.stageRefs.translations.map((translation) => translation.languageCode)
      ]
    );
    const rows = result.rows;

    return request.stageRefs.translations.map((reference) => {
      const row = rows.find((candidate) => (
        candidate.language_code === reference.languageCode
        && candidate.translation_version === reference.version
      ));

      return {
        articleId: request.articleId,
        articleVersion: request.articleVersion,
        languageCode: reference.languageCode,
        summaryRef: row?.summary_ref ?? reference.uri,
        qualityStatus: translationQualityStatus(row?.quality_status),
        translationVersion: row?.translation_version ?? reference.version
      };
    });
  }
}

export class PostgresPersistenceBrokerOutbox implements PersistenceBrokerOutbox {
  readonly name = "postgres-persistence-broker-outbox";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<PersistenceDependencyProbe> {
    return probePool(this.pool, "persistence broker outbox ready");
  }

  async hasReceipt(command: BrokerPublishCommand): Promise<boolean> {
    const result = await this.pool.query<{ readonly id: number }>(
      `SELECT id
       FROM ${PERSISTENCE_SCHEMA}.outbox
       WHERE idempotency_key = $1
         AND status = 'confirmed'
       LIMIT 1`,
      [command.envelope.idempotencyKey]
    );

    return (result.rowCount ?? 0) > 0;
  }

  async record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    const payload = command.payload;

    await this.pool.query(
      `INSERT INTO ${PERSISTENCE_SCHEMA}.outbox (
        outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, published_at, confirmed_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, $14::timestamptz, 'confirmed', $15::jsonb)
      ON CONFLICT (idempotency_key)
      DO UPDATE SET confirmed_at = EXCLUDED.confirmed_at,
                    status = 'confirmed',
                    diagnostic_metadata = ${PERSISTENCE_SCHEMA}.outbox.diagnostic_metadata || EXCLUDED.diagnostic_metadata`,
      [
        receipt.messageId,
        stringFrom(payload.pipelineRunId, command.envelope.correlationId),
        stringFrom(payload.stageExecutionId, command.envelope.messageId),
        command.envelope.route,
        receipt.routingKey,
        command.envelope.aggregate.type,
        command.envelope.aggregate.id,
        command.envelope.schemaVersion,
        Math.max(1, command.envelope.aggregate.version),
        command.envelope.idempotencyKey,
        command.envelope.payloadRef.uri,
        command.envelope.payloadRef.digest ?? sha256Digest(payload),
        receipt.confirmedAt,
        receipt.confirmedAt,
        JSON.stringify({
          envelope: command.envelope,
          exchange: receipt.exchange,
          payloadSchemaId: payload.schemaId,
          payload
        })
      ]
    );
  }
}

interface PersistenceOutboxReconcilerOptions {
  readonly pool: Pool;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly clock: RuntimeClock;
  readonly env: NodeJS.ProcessEnv;
}

interface PersistenceOutboxRow extends QueryResultRow {
  readonly id: string | number;
  readonly outbox_message_id: string;
  readonly pipeline_run_id: string;
  readonly stage_execution_id: string;
  readonly destination_stage: string;
  readonly routing_key: string;
  readonly entity_kind: string;
  readonly entity_id: string;
  readonly schema_version: number;
  readonly operation_version: number;
  readonly idempotency_key: string;
  readonly payload_ref: string;
  readonly payload_digest: string;
  readonly created_at: Date;
  readonly published_at: Date | null;
  readonly confirmed_at: Date | null;
  readonly status: string;
  readonly diagnostic_metadata: unknown;
}

interface LegacyPublicationReadinessMetadataRow extends QueryResultRow {
  readonly request_ref: string;
  readonly response_ref: string | null;
  readonly request_status: string;
  readonly write_diagnostic_metadata: unknown;
  readonly aggregate_payload_ref: string;
  readonly aggregate_payload_digest: string;
  readonly publication_status: string;
  readonly aggregate_version: number | string;
}

interface HydratedReplay {
  readonly row: PersistenceOutboxRow;
  readonly candidate: PersistenceReconciliationCandidate;
  readonly command: BrokerPublishCommand;
}

interface RecoveredEnvelope {
  readonly selectedReason: string;
  readonly envelope: Readonly<Record<string, unknown>>;
}

export class PostgresPersistenceOutboxReconciler implements PersistenceReconciler {
  readonly name = "postgres-persistence-outbox-reconciler";

  constructor(private readonly options: PersistenceOutboxReconcilerOptions) {}

  async reconcile(request: PersistenceReconciliationRequest): Promise<PersistenceReconciliationReport> {
    const requestedAt = runtimeNow(this.options.clock);
    const mode = request.mode === "apply" ? "apply" : "dry-run";
    const maxItems = boundedInteger(request.maxItems, 100, 1, 100);
    const minAgeSeconds = boundedInteger(request.minAgeSeconds, 900, 0, 86_400);
    const reason = safeReason(request.reason);
    const runId = safeRunId(request.runId);

    if (this.killSwitchActive()) {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "kill_switch_active",
        errors: [
          "persistence reconciliation stop switch is active"
        ],
        candidates: []
      });
    }

    if (mode === "apply") {
      const applyError = this.applyGateError(request, runId);

      if (applyError !== undefined) {
        return report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "failed_closed",
          errors: [
            applyError
          ],
          candidates: []
        });
      }
    }

    const rows = await this.selectCandidates(maxItems, minAgeSeconds, runId);
    const hydrated = await Promise.all(rows.map((row) => this.hydrate(row, requestedAt)));
    const failed = hydrated.filter((candidate): candidate is PersistenceReconciliationCandidate => "status" in candidate);

    if (failed.length > 0) {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "failed_closed",
        errors: failed.map((candidate) => `${candidate.outboxId}:${candidate.failedClosedReason ?? "unrecoverable-payload"}`),
        candidates: failed
      });
    }

    const replayable = hydrated as HydratedReplay[];

    if (mode === "dry-run") {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "dry_run",
        candidates: replayable.map((item) => item.candidate),
        errors: []
      });
    }

    const replayed: PersistenceReconciliationCandidate[] = [];

    for (const item of replayable) {
      if (this.killSwitchActive()) {
        return report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "kill_switch_active",
          candidates: replayed,
          errors: [
            "persistence reconciliation stop switch became active"
          ]
        });
      }

      const receipt = await this.options.brokerTransport.publish(item.command);
      await this.recordReplay(item.row, item.command, receipt, requestedAt, runId ?? "untracked", reason);
      replayed.push({
        ...item.candidate,
        status: "replayed",
        replayMessageId: receipt.messageId
      });
    }

    return report({
      mode,
      requestedAt,
      runId,
      reason,
      maxItems,
      minAgeSeconds,
      status: "applied",
      candidates: replayed,
      errors: []
    });
  }

  private async selectCandidates(maxItems: number, minAgeSeconds: number, runId: string | undefined): Promise<readonly PersistenceOutboxRow[]> {
    const result = await this.options.pool.query<PersistenceOutboxRow>(
      `SELECT id, outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
              entity_kind, entity_id, schema_version, operation_version, idempotency_key,
              payload_ref, payload_digest, created_at, published_at, confirmed_at, status, diagnostic_metadata
       FROM ${PERSISTENCE_SCHEMA}.outbox
       WHERE status = 'confirmed'
         AND confirmed_at IS NOT NULL
         AND created_at <= now() - ($2::integer * interval '1 second')
         AND ($3::text IS NULL OR diagnostic_metadata->>'lastReconciliationRunId' IS DISTINCT FROM $3::text)
       ORDER BY created_at ASC, id ASC
       LIMIT $1`,
      [
        maxItems,
        minAgeSeconds,
        runId ?? null
      ]
    );

    return result.rows;
  }

  private async hydrate(row: PersistenceOutboxRow, requestedAt: string): Promise<HydratedReplay | PersistenceReconciliationCandidate> {
    const baseCandidate = candidateFromRow(row, "confirmed-outbox-replay");
    const diagnostic = recordValue(row.diagnostic_metadata);
    const payload = diagnostic.payload;
    const storedEnvelope = diagnostic.envelope;

    if (!isRecord(payload)) {
      return failedCandidate(baseCandidate, "missing-stored-payload");
    }

    if (row.payload_digest !== sha256Digest(payload)) {
      return failedCandidate(baseCandidate, "payload-digest-mismatch");
    }

    const payloadValidation = validateStagePayload(payload);

    if (!payloadValidation.ok) {
      return failedCandidate(baseCandidate, `invalid-stored-payload:${payloadValidation.issues[0]?.code ?? "unknown"}`);
    }

    const recovered = isRecord(storedEnvelope)
      ? {
          selectedReason: "confirmed-outbox-replay",
          envelope: storedEnvelope
        }
      : await this.recoverLegacyPublicationReadinessEnvelope(row, payload, requestedAt);

    if (typeof recovered === "string") {
      return failedCandidate(baseCandidate, recovered);
    }

    const envelope = recovered.envelope;

    if (row.payload_ref !== stringFrom(recordValue(envelope.payloadRef).uri, "")) {
      return failedCandidate(baseCandidate, "payload-ref-mismatch");
    }

    const replayMessageId = randomUUID();
    const attempt = recordValue(envelope.attempt);
    const replayEnvelope = {
      ...envelope,
      messageId: replayMessageId,
      occurredAt: requestedAt,
      attempt: {
        ...attempt,
        lastAttemptAt: requestedAt
      }
    };
    const envelopeValidation = validateWorkerEnvelope(replayEnvelope);

    if (!envelopeValidation.ok) {
      return failedCandidate(baseCandidate, `invalid-stored-envelope:${envelopeValidation.issues[0]?.code ?? "unknown"}`);
    }

    if (envelopeValidation.value.route !== row.destination_stage) {
      return failedCandidate(baseCandidate, "destination-stage-mismatch");
    }

    return {
      row,
      candidate: {
        ...baseCandidate,
        selectedReason: recovered.selectedReason,
        replayMessageId
      },
      command: {
        envelope: envelopeValidation.value,
        payload
      }
    };
  }

  private async recoverLegacyPublicationReadinessEnvelope(
    row: PersistenceOutboxRow,
    payload: Readonly<Record<string, unknown>>,
    requestedAt: string
  ): Promise<RecoveredEnvelope | string> {
    if (row.destination_stage !== "publication") {
      return "missing-stored-envelope";
    }

    if (stringFrom(payload.schemaId, "") !== STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness) {
      return "missing-stored-envelope";
    }

    const publicationRef = recordValue(payload.publicationRef);
    const publicationRefUri = stringFrom(publicationRef.uri, "");
    const publicationRefDigest = stringFrom(publicationRef.payloadDigest, "");
    const articleVersion = positiveInteger(publicationRef.articleVersion)
      ?? positiveInteger(publicationRef.currentArticleVersion)
      ?? positiveInteger(publicationRef.aggregateVersion)
      ?? positiveInteger(row.operation_version);

    if (articleVersion === undefined) {
      return "legacy-publication-version-missing";
    }

    if (stringFrom(payload.idempotencyKey, "") !== row.idempotency_key) {
      return "legacy-idempotency-mismatch";
    }

    if (stringFrom(payload.pipelineRunId, "") !== row.pipeline_run_id) {
      return "legacy-pipeline-run-mismatch";
    }

    if (stringFrom(payload.stageExecutionId, "") !== row.stage_execution_id) {
      return "legacy-stage-execution-mismatch";
    }

    if (stringFrom(payload.articleId, "") !== row.entity_id) {
      return "legacy-article-id-mismatch";
    }

    if (publicationRefUri.length === 0 || publicationRefDigest.length === 0) {
      return "legacy-publication-ref-missing";
    }

    if (row.payload_ref !== `${publicationRefUri}/publication-readiness`) {
      return "legacy-payload-ref-mismatch";
    }

    const metadata = await this.readLegacyPublicationMetadata(row);

    if (metadata === undefined) {
      return "legacy-publication-metadata-missing";
    }

    if (metadata === "ambiguous") {
      return "legacy-publication-metadata-ambiguous";
    }

    const aggregateVersion = positiveInteger(metadata.aggregate_version);

    if (aggregateVersion === undefined || aggregateVersion !== articleVersion) {
      return "legacy-final-aggregate-version-mismatch";
    }

    if (metadata.request_status !== "accepted") {
      return "legacy-write-request-not-accepted";
    }

    if (metadata.request_ref !== publicationRefUri || metadata.response_ref !== row.payload_ref) {
      return "legacy-write-request-ref-mismatch";
    }

    if (metadata.aggregate_payload_ref !== publicationRefUri) {
      return "legacy-final-payload-ref-mismatch";
    }

    if (metadata.aggregate_payload_digest !== publicationRefDigest) {
      return "legacy-final-payload-digest-mismatch";
    }

    if (stringFrom(publicationRef.canonicalIdentityHash, row.entity_id) !== row.entity_id) {
      return "legacy-canonical-identity-mismatch";
    }

    const finalAggregateVersion = positiveInteger(publicationRef.finalAggregateVersion)
      ?? positiveInteger(publicationRef.aggregateVersion);

    if (finalAggregateVersion !== undefined && finalAggregateVersion !== aggregateVersion) {
      return "legacy-publication-ref-version-mismatch";
    }

    const expectedReadinessStatus = metadata.publication_status === "ready"
      ? "ready"
      : "blocked_missing_translations";

    if (stringFrom(payload.readinessStatus, "") !== expectedReadinessStatus) {
      return "legacy-readiness-status-mismatch";
    }

    if (payload.snapshotRefreshRequired !== (metadata.publication_status === "ready")) {
      return "legacy-snapshot-refresh-mismatch";
    }

    const writeDiagnostic = recordValue(metadata.write_diagnostic_metadata);
    const writeAudit = recordValue(writeDiagnostic.audit);
    const sourceMessageId = stringFrom(payload.sourceMessageId, "");
    const correlationId = stringFrom(writeAudit.correlationId, "");
    const traceparent = stringFrom(payload.traceparent, "");

    if (stringFrom(writeDiagnostic.publicationReadinessIdempotencyKey, "") !== row.idempotency_key) {
      return "legacy-readiness-idempotency-mismatch";
    }

    if (stringFrom(writeDiagnostic.payloadDigest, "") !== publicationRefDigest) {
      return "legacy-write-payload-digest-mismatch";
    }

    if (sourceMessageId.length === 0 || stringFrom(writeAudit.messageId, "") !== sourceMessageId) {
      return "legacy-causation-id-mismatch";
    }

    if (correlationId.length === 0) {
      return "legacy-correlation-id-missing";
    }

    if (traceparent.length > 0 && stringFrom(writeAudit.traceparent, traceparent) !== traceparent) {
      return "legacy-traceparent-mismatch";
    }

    if (stringFrom(writeAudit.articleId, row.entity_id) !== row.entity_id) {
      return "legacy-audit-article-id-mismatch";
    }

    const auditArticleVersion = positiveInteger(writeAudit.articleVersion);

    if (auditArticleVersion !== undefined && auditArticleVersion !== articleVersion) {
      return "legacy-audit-article-version-mismatch";
    }

    const route = getWorkerRoute("publication");
    const envelope = {
      schemaId: route.schemaId,
      schemaVersion: row.schema_version,
      route: "publication",
      messageId: row.outbox_message_id,
      causationId: sourceMessageId,
      correlationId,
      ...(traceparent.length === 0 ? {} : {
        traceparent
      }),
      idempotencyKey: row.idempotency_key,
      aggregate: {
        type: row.entity_kind,
        id: row.entity_id,
        version: articleVersion
      },
      occurredAt: stringFrom(payload.producedAt, requestedAt),
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: stringFrom(payload.producedAt, requestedAt)
      },
      producer: {
        name: "persistence",
        version: "0.1.0"
      },
      payloadRef: {
        kind: "backend-record",
        uri: row.payload_ref,
        mediaType: "application/json",
        sizeBytes: getStagePayloadSizeBytes(payload),
        digest: row.payload_digest
      }
    };

    return {
      selectedReason: "legacy-publication-readiness-recovered",
      envelope
    };
  }

  private async readLegacyPublicationMetadata(row: PersistenceOutboxRow): Promise<LegacyPublicationReadinessMetadataRow | "ambiguous" | undefined> {
    const result = await this.options.pool.query<LegacyPublicationReadinessMetadataRow>(
      `SELECT wr.request_ref, wr.response_ref, wr.status AS request_status,
              wr.diagnostic_metadata AS write_diagnostic_metadata,
              final.payload_ref AS aggregate_payload_ref,
              final.payload_digest AS aggregate_payload_digest,
              final.publication_status, final.aggregate_version
       FROM ${PERSISTENCE_SCHEMA}.write_requests wr
       INNER JOIN ${FINAL_SCHEMA}.article_shadow_aggregates final
          ON final.article_identity_hash = wr.article_identity_hash
         AND final.aggregate_version = wr.operation_version
       WHERE wr.article_identity_hash = $1
         AND wr.request_kind = 'article'
         AND wr.operation_version = $2
         AND wr.status = 'accepted'
         AND wr.response_ref = $3
         AND wr.diagnostic_metadata->>'publicationReadinessIdempotencyKey' = $4
       ORDER BY wr.id ASC
       LIMIT 2`,
      [
        row.entity_id,
        row.operation_version,
        row.payload_ref,
        row.idempotency_key
      ]
    );

    if (result.rows.length > 1) {
      return "ambiguous";
    }

    return result.rows[0];
  }

  private async recordReplay(
    row: PersistenceOutboxRow,
    command: BrokerPublishCommand,
    receipt: BrokerPublishReceipt,
    requestedAt: string,
    runId: string,
    reason: string | undefined
  ): Promise<void> {
    const audit = {
      event: "persistence.outbox.replayed",
      runId,
      reason: reason ?? "unspecified",
      originalMessageId: row.outbox_message_id,
      replayMessageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      correlationId: command.envelope.correlationId,
      causationId: command.envelope.causationId,
      articleId: command.envelope.aggregate.id,
      articleVersion: command.envelope.aggregate.version,
      replayedAt: requestedAt,
      exchange: receipt.exchange,
      routingKey: receipt.routingKey
    };

    await this.options.pool.query(
      `UPDATE ${PERSISTENCE_SCHEMA}.outbox
       SET diagnostic_metadata =
         jsonb_set(
           diagnostic_metadata || $2::jsonb,
           '{reconciliationAuditHistory}',
           coalesce(diagnostic_metadata->'reconciliationAuditHistory', '[]'::jsonb) || $3::jsonb,
           true
         )
       WHERE id = $1`,
      [
        row.id,
        JSON.stringify({
          lastReconciliationRunId: runId,
          reconciliationLastReplayAt: requestedAt,
          reconciliationLastReplayMessageId: command.envelope.messageId
        }),
        JSON.stringify([
          audit
        ])
      ]
    );
  }

  private applyGateError(request: PersistenceReconciliationRequest, runId: string | undefined): string | undefined {
    if (!this.applyEnabled()) {
      return "persistence reconciliation apply is disabled by configuration";
    }

    if (request.protectedConfirmation !== PERSISTENCE_RECONCILIATION_CONFIRMATION) {
      return `protectedConfirmation must be ${PERSISTENCE_RECONCILIATION_CONFIRMATION}`;
    }

    if (runId === undefined) {
      return "runId is required for apply";
    }

    return undefined;
  }

  private applyEnabled(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_APPLY_ENABLED)
      || flagEnabled(this.options.env.NUTSNEWS_PERSISTENCE_RECONCILIATION_APPLY_ENABLED);
  }

  private killSwitchActive(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_STOP)
      || flagEnabled(this.options.env.NUTSNEWS_PERSISTENCE_RECONCILIATION_STOP);
  }
}

export class PostgresPersistenceFeedHealthProjectionStore implements PersistenceFeedHealthProjectionStore {
  readonly name = "postgres-persistence-feed-health-projection";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<PersistenceDependencyProbe> {
    return probePool(this.pool, "persistence feed health projection ready");
  }

  async project(event: FeedHealthProjectionEvent): Promise<FeedHealthProjectionWriteResult> {
    const state = projectionStateFromEvent(event);

    await this.pool.query(
      `INSERT INTO ${PERSISTENCE_SCHEMA}.write_requests (
        article_identity_hash, request_kind, operation_version, backend_api_operation,
        request_ref, status, diagnostic_metadata
      ) VALUES ($1, 'feed_health', $2, 'uplift-save-feed-health-batch', $3, 'accepted', $4::jsonb)
      ON CONFLICT (article_identity_hash, request_kind, operation_version)
      DO UPDATE SET status = 'accepted',
                    diagnostic_metadata = EXCLUDED.diagnostic_metadata`,
      [
        event.feedKey,
        event.eventVersion,
        `backend://worker-uplift/persistence/feed-health/${encodeURIComponent(event.feedKey)}/v${String(event.eventVersion)}`,
        JSON.stringify({
          projectionState: state,
          payloadDigest: event.payloadDigest,
          idempotencyKey: event.idempotencyKey,
          sourceMessageId: event.sourceMessageId,
          safeMetadataOnly: true
        })
      ]
    );

    return {
      status: "projected",
      state,
      feedHealthRow: feedHealthRowFromState(state),
      feedQualityRow: feedQualityRowFromState(state)
    };
  }

  async readLegacyFeedHealthRows(): Promise<readonly FeedHealthLegacyRow[]> {
    const result = await this.pool.query<{ readonly diagnostic_metadata: unknown }>(
      `SELECT diagnostic_metadata
       FROM ${PERSISTENCE_SCHEMA}.write_requests
       WHERE request_kind = 'feed_health'
         AND status = 'accepted'
       ORDER BY created_at DESC
       LIMIT 500`
    );

    return result.rows.flatMap((row) => {
      const state = recordValue(row.diagnostic_metadata).projectionState;
      return isFeedHealthProjectionState(state) ? [feedHealthRowFromState(state)] : [];
    });
  }

  async readLegacyFeedQualityRows(): Promise<readonly FeedQualityLegacyRow[]> {
    const result = await this.pool.query<{ readonly diagnostic_metadata: unknown }>(
      `SELECT diagnostic_metadata
       FROM ${PERSISTENCE_SCHEMA}.write_requests
       WHERE request_kind = 'feed_health'
         AND status = 'accepted'
       ORDER BY created_at DESC
       LIMIT 500`
    );

    return result.rows.flatMap((row) => {
      const state = recordValue(row.diagnostic_metadata).projectionState;
      return isFeedHealthProjectionState(state) ? [feedQualityRowFromState(state)] : [];
    });
  }
}

export class HttpPersistenceBackendWorkerApiClient implements PersistenceBackendWorkerApiClient {
  readonly name = "http-backend-worker-api";

  private readonly baseUrl: string;
  private readonly token: string;
  private readonly expectedVersion: string;
  private readonly fetcher: FetchLike;

  constructor(options: {
    readonly baseUrl: string;
    readonly token: string;
    readonly expectedVersion: string;
    readonly fetcher?: FetchLike;
  }) {
    this.baseUrl = stripTrailingSlashes(options.baseUrl);
    this.token = options.token;
    this.expectedVersion = options.expectedVersion;
    this.fetcher = options.fetcher ?? fetch;
  }

  async probe(): Promise<PersistenceDependencyProbe> {
    try {
      const response = await this.fetcher(healthUrl(this.baseUrl), {
        method: "GET",
        signal: AbortSignal.timeout(5_000)
      });

      return response.ok
        ? {
            status: "ok",
            summary: "backend Worker API ready"
          }
        : {
            status: "unhealthy",
            summary: `backend Worker API health returned ${String(response.status)}`
          };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: error instanceof Error ? error.message : "backend Worker API health failed"
      };
    }
  }

  async checkCompatibility(expectedVersion: string): Promise<PersistenceBackendApiCompatibility> {
    const probe = await this.probe();
    const version = expectedVersion || this.expectedVersion;

    return {
      status: probe.status,
      summary: probe.summary,
      version,
      requiredScopes: [
        "worker-uplift-persistence",
        "uplift-record-shadow-aggregate"
      ],
      productionDomainWritesEnabled: false
    };
  }

  async recordShadowAggregate(
    command: PersistenceBackendShadowAggregateCommand
  ): Promise<PersistenceBackendShadowAggregateResult> {
    const response = await this.fetcher(`${this.baseUrl}/${command.operation}`, {
      method: "POST",
      headers: {
        "authorization": `Bearer ${this.token}`,
        "content-type": "application/json"
      },
      body: JSON.stringify(command),
      signal: AbortSignal.timeout(10_000)
    });
    const body = await parseJsonResponse(response);

    if (response.ok) {
      return {
        status: booleanValue(body.duplicate) ? "duplicate" : "recorded",
        productionSideEffect: false,
        response: body
      };
    }

    const error = stringFrom(body.error, response.statusText);

    if (response.status === 409) {
      return {
        status: error.includes("stale") ? "stale" : "conflict",
        reason: error
      };
    }

    if (response.status === 401 || response.status === 403) {
      throw new PersistencePermanentError("backend-api-unauthorized", error);
    }

    throw new PersistenceTransientError("backend-api-transient", error);
  }
}

async function probePool(pool: Pool, summary: string): Promise<PersistenceDependencyProbe> {
  try {
    await pool.query("SELECT 1");

    return {
      status: "ok",
      summary
    };
  } catch (error: unknown) {
    return {
      status: "unhealthy",
      summary: error instanceof Error ? error.message : "database probe failed"
    };
  }
}

function permissionProbe(
  status: PersistenceDependencyProbe["status"],
  summary: string,
  details: PersistencePermissionProbe["details"]
): PersistencePermissionProbe {
  return {
    status,
    summary,
    details
  };
}

function transactionClient(transaction: PersistenceDatabaseTransaction): PoolClient {
  const client = (transaction as Partial<PgPersistenceTransaction>).client;

  if (client === undefined) {
    throw new Error("Persistence operation requires a Postgres transaction.");
  }

  return client;
}

function requiredEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production persistence dependencies.`);
  }

  return value;
}

function reconciliationTokenFromEnv(env: NodeJS.ProcessEnv): string | undefined {
  const directToken = optionalEnv(env, "NUTSNEWS_PERSISTENCE_RECONCILIATION_TOKEN")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN");

  if (directToken !== undefined) {
    return directToken;
  }

  const tokenFile = optionalEnv(env, "NUTSNEWS_PERSISTENCE_RECONCILIATION_TOKEN_FILE")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN_FILE");

  if (tokenFile === undefined) {
    return undefined;
  }

  const value = readFileSync(tokenFile, "utf8").trim();

  return value.length > 0 ? value : undefined;
}

function optionalEnv(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key]?.trim();

  return value === undefined || value.length === 0 ? undefined : value;
}

function report(input: {
  readonly mode: PersistenceReconciliationRequest["mode"];
  readonly requestedAt: string;
  readonly runId?: string | undefined;
  readonly reason?: string | undefined;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly status: PersistenceReconciliationReport["status"];
  readonly candidates: readonly PersistenceReconciliationCandidate[];
  readonly errors: readonly string[];
}): PersistenceReconciliationReport {
  const replayedCount = input.candidates.filter((candidate) => candidate.status === "replayed").length;
  const failedClosedCount = input.candidates.filter((candidate) => candidate.status === "failed_closed").length;
  const skippedCount = input.status === "failed_closed"
    ? Math.max(0, input.candidates.length - failedClosedCount)
    : 0;
  const base = {
    service: "persistence",
    mode: input.mode,
    status: input.status,
    requestedAt: input.requestedAt,
    maxItems: input.maxItems,
    minAgeSeconds: input.minAgeSeconds,
    selectedCount: input.candidates.length,
    replayedCount,
    failedClosedCount,
    skippedCount,
    writesPerformed: replayedCount > 0,
    dryRun: input.mode === "dry-run",
    productionVisibilityEnabled: false,
    legacyRuntimeRequired: false,
    protectedApplyRequired: true,
    candidates: input.candidates,
    errors: input.errors,
    metrics: {
      candidateCount: input.candidates.length,
      replayedCount,
      failedClosedCount,
      skippedCount
    }
  } satisfies Omit<PersistenceReconciliationReport, "runId" | "reason">;

  return {
    ...base,
    ...(input.runId === undefined ? {} : {
      runId: input.runId
    }),
    ...(input.reason === undefined ? {} : {
      reason: input.reason
    })
  };
}

function candidateFromRow(row: PersistenceOutboxRow, selectedReason: string): PersistenceReconciliationCandidate {
  return {
    outboxId: String(row.id),
    idempotencyKey: row.idempotency_key,
    destinationStage: row.destination_stage,
    routingKey: row.routing_key,
    entityKind: row.entity_kind,
    entityId: row.entity_id,
    payloadRef: row.payload_ref,
    payloadDigest: row.payload_digest,
    selectedReason,
    status: "selected"
  };
}

function failedCandidate(
  candidate: PersistenceReconciliationCandidate,
  failedClosedReason: string
): PersistenceReconciliationCandidate {
  return {
    ...candidate,
    status: "failed_closed",
    failedClosedReason
  };
}

function boundedInteger(value: unknown, defaultValue: number, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return defaultValue;
  }

  return Math.max(min, Math.min(max, value));
}

function safeRunId(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();

  return /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/u.test(trimmed) ? trimmed : undefined;
}

function safeReason(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.replace(/[\r\n\t]+/gu, " ").trim();

  return trimmed.length === 0 ? undefined : trimmed.slice(0, 160);
}

function flagEnabled(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
}

function sanitizeCode(reason: string): string {
  return reason.replace(/[^a-zA-Z0-9_.:-]/g, "_").slice(0, 128);
}

function sanitizeMessage(reason: string): string {
  return reason.slice(0, 512);
}

function approvalDecision(value: string | undefined): PersistenceFinalMaterializationInputs["approval"]["decision"] {
  if (value === "approved" || value === "rejected" || value === "needs_review") {
    return value;
  }

  return "needs_review";
}

function translationQualityStatus(value: string | undefined): PersistenceFinalMaterializationInputs["translations"][number]["qualityStatus"] {
  if (value === "accepted" || value === "failed" || value === "needs_review" || value === "pending") {
    return value;
  }

  return "pending";
}

function stringFrom(value: unknown, fallback: string): string {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function positiveInteger(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isInteger(value) && value > 0) {
    return value;
  }

  if (typeof value === "string" && /^[1-9][0-9]*$/u.test(value)) {
    return Number(value);
  }

  return undefined;
}

function booleanValue(value: unknown): boolean {
  return value === true;
}

function recordValue(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFeedHealthProjectionState(value: unknown): value is FeedHealthProjectionState {
  const record = recordValue(value);

  return typeof record.feedKey === "string"
    && typeof record.source === "string"
    && typeof record.feedUrl === "string"
    && typeof record.updatedAt === "string";
}

function projectionStateFromEvent(event: FeedHealthProjectionEvent): FeedHealthProjectionState {
  return {
    feedKey: event.feedKey,
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
    consecutiveFailureCount: event.outcomeStatus === "failure" ? 1 : 0,
    totalFetchCount: event.outcomeKind === "fetch_attempt" ? 1 : 0,
    totalSuccessCount: event.outcomeKind === "fetch_attempt" && event.outcomeStatus === "success" ? 1 : 0,
    totalFailureCount: event.outcomeStatus === "failure" ? 1 : 0,
    totalItemCount: event.counts.itemCount,
    totalDuplicateCount: event.counts.duplicateCount,
    totalImageCount: event.counts.imageCount,
    totalEligibleCount: event.counts.eligibleCount,
    totalRejectedCount: event.counts.rejectedCount,
    totalAcceptedCount: event.counts.acceptedCount,
    totalLatencyMs: event.latencyMs,
    updatedAt: event.occurredAt,
    backoffRecommendation: event.backoffRecommendation,
    errorSamples: event.errorClass === undefined
      ? []
      : [
          {
            occurredAt: event.occurredAt,
            outcomeStage: event.outcomeStage,
            outcomeKind: event.outcomeKind,
            errorClass: event.errorClass
          }
        ],
    ...(event.outcomeStatus === "success" ? {
      lastSuccessAt: event.occurredAt
    } : {}),
    ...(event.outcomeStatus === "failure" ? {
      lastFailureAt: event.occurredAt,
      lastErrorClass: event.errorClass ?? "unknown_failure"
    } : {})
  };
}

function feedHealthRowFromState(state: FeedHealthProjectionState): FeedHealthLegacyRow {
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

function feedQualityRowFromState(state: FeedHealthProjectionState): FeedQualityLegacyRow {
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

async function parseJsonResponse(response: Response): Promise<Readonly<Record<string, unknown>>> {
  try {
    return recordValue(await response.json());
  } catch {
    return {};
  }
}

function stripTrailingSlashes(value: string): string {
  let end = value.length;

  while (end > 0 && value.charCodeAt(end - 1) === 47) {
    end -= 1;
  }

  return value.slice(0, end);
}

function healthUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.pathname = "/healthz";
  url.search = "";
  url.hash = "";
  return url.toString();
}

function shortHash(value: string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex").slice(0, 32)}`;
}
