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
  PersistenceBackendShadowAggregateCommand,
  PersistenceBackendShadowAggregateResult,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalMaterializationWriteResult,
  PersistenceQuarantineRecord
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

export interface PersistenceInboxStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  verifyPayloadFingerprint(idempotencyKey: string, fingerprint: string): Promise<PersistenceInboxFingerprintResult>;
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

export interface PersistenceBackendApiCompatibility {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
  readonly version: string;
  readonly requiredScopes: readonly string[];
  readonly productionDomainWritesEnabled: false;
}

export interface PersistenceBackendWorkerApiClient {
  readonly name: string;
  probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  checkCompatibility(expectedVersion: string): PersistenceBackendApiCompatibility | Promise<PersistenceBackendApiCompatibility>;
  recordShadowAggregate(
    command: PersistenceBackendShadowAggregateCommand
  ): PersistenceBackendShadowAggregateResult | Promise<PersistenceBackendShadowAggregateResult>;
}

export interface PersistenceWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: PersistenceDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface PersistenceWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: PersistenceWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface PersistenceDependencies {
  readonly clock: RuntimeClock;
  readonly inboxStore: PersistenceInboxStore;
  readonly finalShadowTransactions: PersistenceFinalShadowTransactionRunner;
  readonly stageViewReader: PersistenceStageViewReader;
  readonly brokerOutbox: PersistenceBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly backendApiClient: PersistenceBackendWorkerApiClient;
  readonly workHandler: PersistenceWorkHandler;
}
