import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";
import type {
  FeedHealthLegacyRow,
  FeedHealthProjectionEvent,
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

export interface PersistenceDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export type PersistenceInboxFingerprintResult =
  | {
      readonly status: "accepted" | "duplicate";
    }
  | {
      readonly status: "conflict";
      readonly existingFingerprint: string;
    };

export type PersistenceInboxClaimRenewalResult = {
  readonly status: "renewed";
} | {
  readonly status: "not-owned";
};

export interface PersistenceInboxStore extends RuntimeIdempotencyStore {
  readonly name: string;
  readonly claimLeaseMs?: number;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  verifyPayloadFingerprint(idempotencyKey: string, fingerprint: string): Promise<PersistenceInboxFingerprintResult>;
  renewClaim(idempotencyKey: string, claimToken: string): Promise<PersistenceInboxClaimRenewalResult>;
  claim(
    idempotencyKey: string,
    context: Parameters<RuntimeIdempotencyStore["claim"]>[1],
    payloadFingerprint?: string
  ): ReturnType<RuntimeIdempotencyStore["claim"]>;
}

export interface PersistenceDatabaseTransaction {
  readonly transactionId: string;
}

export interface PersistencePermissionProbe extends PersistenceDependencyProbe {
  readonly details: {
    readonly databaseRole: string;
    readonly shadowSchemaVersion: string;
    readonly allowedWriteScopes: readonly string[];
    readonly allowedReadScopes: readonly string[];
    readonly deniedWriteScopes: readonly string[];
  };
}

export interface PersistenceFinalShadowTransactionRunner {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  checkWriteScope(): PersistencePermissionProbe | Promise<PersistencePermissionProbe>;
  withTransaction<T>(operation: (transaction: PersistenceDatabaseTransaction) => Promise<T>): Promise<T>;
  recordFinalMaterialization(
    transaction: PersistenceDatabaseTransaction,
    record: PersistenceFinalMaterializationRecord
  ): Promise<PersistenceFinalMaterializationWriteResult>;
  recordQuarantine(transaction: PersistenceDatabaseTransaction, record: PersistenceQuarantineRecord): Promise<void>;
}

export interface PersistenceStageViewReader {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  checkReadScope(): PersistencePermissionProbe | Promise<PersistencePermissionProbe>;
  readFinalMaterializationInputs(
    request: PersistenceFinalMaterializationRequest
  ): PersistenceFinalMaterializationInputs | Promise<PersistenceFinalMaterializationInputs>;
}

export interface PersistenceBrokerOutbox {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  hasReceipt(command: BrokerPublishCommand): Promise<boolean>;
}

export interface PersistenceFeedHealthProjectionStore {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  project(event: FeedHealthProjectionEvent): Promise<FeedHealthProjectionWriteResult>;
  readLegacyFeedHealthRows(): Promise<readonly FeedHealthLegacyRow[]>;
  readLegacyFeedQualityRows(): Promise<readonly FeedQualityLegacyRow[]>;
}

export interface PersistenceBackendApiCompatibility {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
  readonly version: string;
  readonly requiredScopes: readonly string[];
  readonly productionDomainWritesEnabled: boolean;
}

export interface PersistenceBackendWorkerApiClient {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  checkCompatibility(expectedVersion: string): PersistenceBackendApiCompatibility | Promise<PersistenceBackendApiCompatibility>;
  recordShadowAggregate(
    command: PersistenceBackendShadowAggregateCommand
  ): PersistenceBackendShadowAggregateResult | Promise<PersistenceBackendShadowAggregateResult>;
  saveAcceptedArticle(
    command: PersistenceSaveAcceptedArticleCommand
  ): PersistenceBackendPrimaryWriteResult | Promise<PersistenceBackendPrimaryWriteResult>;
  saveArticleSummaries(
    command: PersistenceSaveArticleSummariesCommand
  ): PersistenceBackendPrimaryWriteResult | Promise<PersistenceBackendPrimaryWriteResult>;
}

export interface PersistenceWorkTools {
  readonly signal: AbortSignal;
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: PersistenceDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface PersistenceWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: PersistenceWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface PersistenceBrokerTransport extends RuntimeBrokerTransport {
  publishWithSignal?(
    command: BrokerPublishCommand,
    signal: AbortSignal
  ): Promise<BrokerPublishReceipt>;
}

export interface PersistenceDependencies {
  readonly adapterMode: "in_memory" | "production";
  readonly stateStoreMode: "in_memory" | "postgresql";
  readonly productionDomainWritesEnabled: boolean;
  readonly clock: RuntimeClock;
  readonly inboxStore: PersistenceInboxStore;
  readonly finalShadowTransactions: PersistenceFinalShadowTransactionRunner;
  readonly stageViewReader: PersistenceStageViewReader;
  readonly brokerOutbox: PersistenceBrokerOutbox;
  readonly feedHealthProjectionStore: PersistenceFeedHealthProjectionStore;
  readonly brokerTransport: PersistenceBrokerTransport;
  readonly backendApiClient: PersistenceBackendWorkerApiClient;
  readonly workHandler: PersistenceWorkHandler;
}
