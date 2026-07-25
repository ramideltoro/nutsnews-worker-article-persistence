import type {
  BrokerPublishCommand
} from "@ramideltoro/nutsnews-worker-runtime";

export interface PersistenceStageResultReference {
  readonly uri: string;
  readonly version: number;
  readonly digest?: string;
}

export interface PersistenceTranslationStageResultReference extends PersistenceStageResultReference {
  readonly languageCode: string;
}

export interface PersistenceFinalStageResultReferences {
  readonly canonical: PersistenceStageResultReference;
  readonly enrichment: PersistenceStageResultReference;
  readonly approval: PersistenceStageResultReference;
  readonly translations: readonly PersistenceTranslationStageResultReference[];
}

export interface PersistenceFinalMaterializationRequest {
  readonly commandId: string;
  readonly idempotencyKey: string;
  readonly messageId: string;
  readonly correlationId: string;
  readonly pipelineRunId: string;
  readonly stageExecutionId: string;
  readonly sourceMessageId: string;
  readonly traceparent: string;
  readonly producedAt: string;
  readonly articleId: string;
  readonly articleVersion: number;
  readonly writeMode: "upsert";
  readonly payloadProviderMode: "backend_postgres_primary";
  readonly backendProviderMode: "backend_postgres_shadow";
  readonly payloadBackendOperation: "save-accepted-articles-batch";
  readonly backendOperation: "uplift-record-shadow-aggregate";
  readonly stageRefs: PersistenceFinalStageResultReferences;
  readonly requiredLanguageCodes: readonly string[];
  readonly payloadDigest: string;
}

export interface PersistenceCanonicalStageResult {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly articleIdentityHash: string;
  readonly canonicalUrlHash: string;
  readonly originalUrlHash: string;
  readonly sourceFeedUrl?: string;
  readonly resultVersion: number;
}

export interface PersistenceEnrichmentStageResult {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly titleRef: string;
  readonly imageUrlRef: string;
  readonly category?: string;
  readonly resultVersion: number;
}

export interface PersistenceApprovalStageResult {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly decision: "approved" | "rejected" | "needs_review";
  readonly positivityScore?: number;
  readonly approvalVersion: number;
}

export interface PersistenceTranslationStageResult {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly languageCode: string;
  readonly summaryRef: string;
  readonly qualityStatus: "accepted" | "failed" | "needs_review" | "pending";
  readonly translationVersion: number;
}

export interface PersistenceFinalMaterializationInputs {
  readonly canonical: PersistenceCanonicalStageResult;
  readonly enrichment: PersistenceEnrichmentStageResult;
  readonly approval: PersistenceApprovalStageResult;
  readonly translations: readonly PersistenceTranslationStageResult[];
}

export interface PersistenceFinalShadowAggregate {
  readonly articleIdentityHash: string;
  readonly canonicalUrlHash: string;
  readonly originalUrlHash: string;
  readonly aggregateVersion: number;
  readonly sourceFeedUrl?: string;
  readonly titleRef: string;
  readonly imageUrlRef: string;
  readonly category?: string;
  readonly positivityScore?: number;
  readonly approvalVersion: number;
  readonly translationLanguages: readonly string[];
  readonly publicationStatus: "shadow_only" | "ready" | "blocked" | "published";
  readonly payloadRef: string;
  readonly payloadDigest: string;
  readonly diagnosticMetadata: Readonly<Record<string, string | number | boolean>>;
}

export interface PersistenceFinalMaterializationAudit {
  readonly idempotencyKey: string;
  readonly commandId: string;
  readonly messageId: string;
  readonly correlationId: string;
  readonly pipelineRunId: string;
  readonly stageExecutionId: string;
  readonly sourceMessageId: string;
  readonly traceparent: string;
  readonly articleId: string;
  readonly articleVersion: number;
  readonly aggregateVersion: number;
  readonly canonicalVersion: number;
  readonly enrichmentVersion: number;
  readonly approvalVersion: number;
  readonly translationVersions: Readonly<Record<string, number>>;
  readonly status: "recorded_success" | "recorded_duplicate" | "quarantined";
  readonly reason?: string;
  readonly payloadDigest: string;
}

export interface PersistenceFinalMaterializationRecord {
  readonly request: PersistenceFinalMaterializationRequest;
  readonly inputs: PersistenceFinalMaterializationInputs;
  readonly aggregate: PersistenceFinalShadowAggregate;
  readonly audit: PersistenceFinalMaterializationAudit;
  readonly publicationReadinessCommand: BrokerPublishCommand;
}

export interface PersistenceQuarantineRecord {
  readonly idempotencyKey: string;
  readonly messageId: string;
  readonly correlationId: string;
  readonly pipelineRunId?: string;
  readonly stageExecutionId?: string;
  readonly sourceMessageId?: string;
  readonly articleId?: string;
  readonly articleVersion?: number;
  readonly reason: string;
  readonly payloadDigest?: string;
  readonly diagnosticMetadata: Readonly<Record<string, string | number | boolean>>;
}

export type PersistenceFinalMaterializationWriteResult =
  | {
      readonly status: "recorded";
      readonly aggregate: PersistenceFinalShadowAggregate;
      readonly publicationReadinessCommand: BrokerPublishCommand;
    }
  | {
      readonly status: "duplicate";
      readonly aggregate: PersistenceFinalShadowAggregate;
    }
  | {
      readonly status: "conflict" | "stale";
      readonly reason: string;
    };

export interface PersistenceBackendShadowAggregateCommand {
  readonly operation: "uplift-record-shadow-aggregate";
  readonly providerMode: "backend_postgres_shadow";
  readonly idempotencyKey: string;
  readonly messageId: string;
  readonly correlationId: string;
  readonly pipelineRunId: string;
  readonly stageExecutionId: string;
  readonly sourceMessageId: string;
  readonly actorService: "worker-uplift-persistence";
  readonly schemaVersion: 1;
  readonly operationVersion: number;
  readonly expectedArticleVersion: number;
  readonly shadowAggregate: PersistenceFinalShadowAggregate;
}

export type PersistenceBackendShadowAggregateResult =
  | {
      readonly status: "recorded" | "duplicate";
      readonly productionSideEffect: false;
      readonly response: Readonly<Record<string, unknown>>;
    }
  | {
      readonly status: "conflict" | "stale";
      readonly reason: string;
    };
