import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadPersistenceConfig,
  type PersistenceConfig
} from "./config.js";
import type { PersistenceDependencies } from "./dependencies.js";
import { createPersistenceHttpServer } from "./http.js";
import { createProductionPersistenceDependencies } from "./production.js";
import type { PersistenceReconciler } from "./reconciliation.js";
import { createPersistenceService } from "./service.js";
import { createPersistencePrometheusTelemetrySink } from "./telemetry.js";
import { createLocalPersistenceDependencies } from "./test-doubles.js";

export {
  PERSISTENCE_CONFIG_SCHEMA,
  PERSISTENCE_SERVICE_NAME,
  PERSISTENCE_SERVICE_VERSION,
  PersistenceConfigError,
  loadPersistenceConfig,
  type PersistenceConfig
} from "./config.js";
export type {
  PersistenceBackendApiCompatibility,
  PersistenceBackendWorkerApiClient,
  PersistenceBrokerOutbox,
  PersistenceDatabaseTransaction,
  PersistenceDependencies,
  PersistenceDependencyProbe,
  PersistenceFeedHealthProjectionStore,
  PersistenceFinalShadowTransactionRunner,
  PersistenceInboxStore,
  PersistencePermissionProbe,
  PersistenceStageViewReader,
  PersistenceWorkHandler,
  PersistenceWorkTools
} from "./dependencies.js";
export {
  PersistencePermanentError,
  PersistenceProcessingError,
  PersistenceTransientError,
  classifyPersistenceError,
  type PersistenceErrorClassification
} from "./errors.js";
export {
  FeedHealthProjectionHandler,
  PersistenceRoutingWorkHandler,
  createFeedHealthProjectionHandler,
  createPersistenceRoutingWorkHandler,
  isFeedHealthProjectionPayload
} from "./feed-health-projection.js";
export type {
  FeedHealthLegacyRow,
  FeedHealthOutcomeKind,
  FeedHealthOutcomeStage,
  FeedHealthOutcomeStatus,
  FeedHealthProjectionCounts,
  FeedHealthProjectionErrorSample,
  FeedHealthProjectionEvent,
  FeedHealthProjectionState,
  FeedHealthProjectionWriteResult,
  FeedQualityLegacyRow
} from "./feed-health-types.js";
export {
  FinalShadowMaterializationHandler,
  createFinalShadowMaterializationHandler
} from "./materialization.js";
export type {
  PersistenceCanonicalStageResult,
  PersistenceEnrichmentStageResult,
  PersistenceApprovalStageResult,
  PersistenceBackendShadowAggregateCommand,
  PersistenceBackendShadowAggregateResult,
  PersistenceFinalMaterializationAudit,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalMaterializationWriteResult,
  PersistenceFinalShadowAggregate,
  PersistenceFinalStageResultReferences,
  PersistenceQuarantineRecord,
  PersistenceStageResultReference,
  PersistenceTranslationStageResult,
  PersistenceTranslationStageResultReference
} from "./materialization-types.js";
export {
  createPersistenceHttpServer,
  type PersistenceHttpServer
} from "./http.js";
export {
  PERSISTENCE_RECONCILIATION_CONFIRMATION,
  PERSISTENCE_RECONCILIATION_PATH,
  type PersistenceReconciliationCandidate,
  type PersistenceReconciliationReport,
  type PersistenceReconciliationRequest,
  type PersistenceReconciler
} from "./reconciliation.js";
export {
  HttpPersistenceBackendWorkerApiClient,
  PostgresPersistenceBrokerOutbox,
  PostgresPersistenceFeedHealthProjectionStore,
  PostgresPersistenceFinalShadowTransactionRunner,
  PostgresPersistenceInboxStore,
  PostgresPersistenceOutboxReconciler,
  PostgresPersistenceStageViewReader,
  createProductionPersistenceDependencies,
  type ProductionPersistenceDependencies
} from "./production.js";
export {
  PayloadRabbitMqTransport
} from "./rabbitmq-payload-transport.js";
export {
  createPersistenceService,
  type PersistenceService
} from "./service.js";
export {
  PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS,
  PERSISTENCE_STAGE_METRIC_OUTCOMES,
  PERSISTENCE_STAGE_METRIC_SERVICE,
  createPersistencePrometheusTelemetrySink,
  type PersistenceHealthOutcome,
  type PersistenceHealthProbe,
  type PersistencePrometheusTelemetrySink,
  type PersistenceRuntimeMetricsSink,
  type PersistenceStageMetricOutcome
} from "./telemetry.js";
export {
  InMemoryPersistenceInboxStore,
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFeedHealthProjectionStore,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceBrokerOutbox,
  LocalPersistenceWorkHandler,
  LocalStageViewReader,
  ManualPersistenceClock,
  createLocalFinalMaterializationInputs,
  createLocalPersistenceDependencies,
  createMinimalFeedHealthProjectionDelivery,
  createMinimalFeedHealthProjectionEnvelope,
  createMinimalFeedHealthProjectionPayload,
  createMinimalPersistenceDelivery,
  createMinimalPersistenceEnvelope,
  createMinimalPersistencePayload
} from "./test-doubles.js";

export interface PersistenceApplication {
  readonly config: PersistenceConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
  url(path?: string): string;
}

export interface PersistenceApplicationOptions {
  readonly dependencies?: PersistenceDependencies;
}

export function createPersistenceApplication(
  config = loadPersistenceConfig(),
  options: PersistenceApplicationOptions = {}
): PersistenceApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.dependencyMode === "production"
      ? "shadow"
      : config.environment === "test" ? "test" : "local",
    adapter: config.dependencyMode === "production" ? "production" : "in_memory"
  } as const;
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPersistencePrometheusTelemetrySink({
        identity,
        expectedActive: config.dependencyMode === "production"
          && config.security.productionWritesEnabled
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const dependencies = options.dependencies ?? (
    config.dependencyMode === "production"
      ? createProductionPersistenceDependencies({
          config,
          clock: SYSTEM_RUNTIME_CLOCK,
          ...(telemetry === undefined ? {} : {
            telemetry
          })
        })
      : createLocalPersistenceDependencies({
          clock: SYSTEM_RUNTIME_CLOCK
        })
  );
  const service = createPersistenceService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createPersistenceHttpServer({
    config,
    service,
    ...(hasReconciler(dependencies) ? {
      reconciler: dependencies.reconciler
    } : {}),
    ...(hasReconciliationToken(dependencies) ? {
      reconciliationToken: dependencies.reconciliationToken
    } : {}),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
      },
      async () => {
        if (hasClose(dependencies)) {
          await dependencies.close();
        }
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: {
        flush: async () => {
          try {
            await logSink.flush();
          } catch {
            // Telemetry flushing is best effort and must not block shutdown.
          }
        }
      }
    })
  });

  return {
    config,
    async start(): Promise<void> {
      let listenerBound = false;

      try {
        assertPackageCompatibility();
        await httpServer.listen();
        listenerBound = true;
        shutdown.start();
        await service.start();
      } catch (error: unknown) {
        shutdown.stop();
        await cleanupFailedStart(httpServer, service, dependencies, listenerBound);
        throw error;
      }
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    },
    url(path?: string): string {
      return httpServer.url(path);
    }
  };
}

async function cleanupFailedStart(
  httpServer: ReturnType<typeof createPersistenceHttpServer>,
  service: ReturnType<typeof createPersistenceService>,
  dependencies: PersistenceDependencies,
  listenerBound: boolean
): Promise<void> {
  if (listenerBound) {
    await runCleanupBestEffort(() => httpServer.close());
  }

  await runCleanupBestEffort(() => service.stop());

  if (hasClose(dependencies)) {
    await runCleanupBestEffort(() => dependencies.close());
  }
}

async function runCleanupBestEffort(operation: () => Promise<void>): Promise<void> {
  try {
    await operation();
  } catch {
    // Preserve the startup failure while still attempting all remaining cleanup.
  }
}

function hasClose(value: unknown): value is { close(): Promise<void> } {
  return typeof value === "object"
    && value !== null
    && "close" in value
    && typeof value.close === "function";
}

function hasReconciler(value: unknown): value is { readonly reconciler: PersistenceReconciler } {
  return typeof value === "object"
    && value !== null
    && "reconciler" in value
    && typeof value.reconciler === "object"
    && value.reconciler !== null;
}

function hasReconciliationToken(value: unknown): value is { readonly reconciliationToken: string } {
  return typeof value === "object"
    && value !== null
    && "reconciliationToken" in value
    && typeof value.reconciliationToken === "string"
    && value.reconciliationToken.length > 0;
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        try {
          await sink.emit(event);
        } catch {
          // Each sink is isolated so one telemetry outage cannot block the others.
        }
      }
    }
  };
}

export const SUPPORTED_CONTRACTS_PACKAGE_VERSION = "1.0.0";
export const SUPPORTED_RUNTIME_PACKAGE_VERSION = "1.0.0";

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;
  const runtimeContractsVersion: string = runtime.contractsPackageVersion;

  if (contractsVersion !== SUPPORTED_CONTRACTS_PACKAGE_VERSION) {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== SUPPORTED_RUNTIME_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }

  if (runtimeContractsVersion !== SUPPORTED_CONTRACTS_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime contracts package version ${runtimeContractsVersion}.`);
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createPersistenceApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start persistence");
    process.exitCode = 1;
  });
}
