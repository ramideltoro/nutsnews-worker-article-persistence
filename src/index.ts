import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadPersistenceConfig,
  type PersistenceConfig
} from "./config.js";
import { createPersistenceHttpServer } from "./http.js";
import { createPersistenceService } from "./service.js";
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
  PersistenceFinalShadowTransactionRunner,
  PersistenceInboxStore,
  PersistencePermissionProbe,
  PersistenceStageViewReader,
  PersistenceWorkHandler,
  PersistenceWorkTools
} from "./dependencies.js";
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
  createPersistenceService,
  type PersistenceService
} from "./service.js";
export {
  InMemoryPersistenceInboxStore,
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceBrokerOutbox,
  LocalPersistenceWorkHandler,
  LocalStageViewReader,
  ManualPersistenceClock,
  createLocalFinalMaterializationInputs,
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery,
  createMinimalPersistenceEnvelope,
  createMinimalPersistencePayload
} from "./test-doubles.js";

export interface PersistenceApplication {
  readonly config: PersistenceConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createPersistenceApplication(config = loadPersistenceConfig()): PersistenceApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const dependencies = createLocalPersistenceDependencies({
    clock: SYSTEM_RUNTIME_CLOCK
  });
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
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
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
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createPersistenceApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start persistence");
    process.exitCode = 1;
  });
}
