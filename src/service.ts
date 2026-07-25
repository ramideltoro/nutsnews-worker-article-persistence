import {
  getRetryDestination,
  getWorkerRoute,
  validateStagePayload,
  validateWorkerEnvelope,
  type StagePayloadValidationIssue,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyStore,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink,
  type RuntimeValidationIssue
} from "@ramideltoro/nutsnews-worker-runtime";

import type { PersistenceConfig } from "./config.js";
import type {
  PersistenceDependencies,
  PersistenceDependencyProbe
} from "./dependencies.js";

export interface PersistenceServiceOptions {
  readonly config: PersistenceConfig;
  readonly dependencies: PersistenceDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
}

export interface PersistenceService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createPersistenceService(options: PersistenceServiceOptions): PersistenceService {
  const persistenceRoute = getWorkerRoute("persistence");
  const publicationRoute = getWorkerRoute("publication");
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      persistenceRoute,
      publicationRoute
    ],
    clock: options.dependencies.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createPersistenceInputProcessor({
    dependencies: options.dependencies,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          options.metrics?.setInFlight(persistenceRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publish: (command) => broker.publish(command),
            recordOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.finalShadowTransactions.withTransaction(operation)
          });

          await emitRuntimeTelemetry(options.telemetry, {
            name: "runtime.dependency.observed",
            level: result.status === "ok" ? "info" : "warn",
            at: runtimeNow(options.dependencies.clock),
            stage: "persistence",
            queue: persistenceRoute.mainQueue.name,
            outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
            attributes: {
              event: "persistence.message.delegated",
              dependency: options.dependencies.workHandler.name,
              shadowMode: options.config.shadowMode,
              productionWritesEnabled: options.config.security.productionWritesEnabled
            }
          });

          return result;
        });
      } finally {
        options.metrics?.setInFlight(persistenceRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      return createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          dependencyReadinessCheck("persistence-inbox", options.dependencies.inboxStore),
          dependencyReadinessCheck("final-shadow-transactions", options.dependencies.finalShadowTransactions),
          dependencyReadinessCheck("stage-view-reader", options.dependencies.stageViewReader),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          dependencyReadinessCheck("backend-worker-api", options.dependencies.backendApiClient),
          finalShadowPermissionCheck(options),
          stageViewPermissionCheck(options),
          backendApiCompatibilityCheck(options),
          shadowModeCheck(options.config),
          productionWritesDisabledCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(options.telemetry === undefined ? {} : {
          telemetry: options.telemetry
        })
      });
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      await broker.start();
      consumer = await broker.consume("persistence", processor);
      started = true;
      options.metrics?.recordDependencyLatency(persistenceRoute.mainQueue.name, 0, "success");
      options.metrics?.setInFlight(persistenceRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.dependency.observed",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "persistence",
        queue: persistenceRoute.mainQueue.name,
        outcome: "success",
        attributes: {
          dependency: "persistence-shell",
          mode: options.config.dependencyMode,
          prefetch: options.config.prefetch,
          concurrency: options.config.concurrency,
          databaseRole: options.config.security.databaseRole,
          backendApiIdentity: options.config.security.backendApiIdentity,
          shadowMode: options.config.shadowMode
        }
      });
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      options.metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      options.metrics?.setShutdownDraining(false);
      options.metrics?.setInFlight(persistenceRoute.mainQueue.name, drain.inFlight);
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies PersistenceService;

  return service;
}

interface PersistenceInputProcessorOptions {
  readonly dependencies: PersistenceDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  handler(context: RuntimeMessageContext): Promise<{ readonly status: "ok" } | { readonly status: "retry"; readonly reason: string; readonly retryAfterMs?: number } | { readonly status: "terminal-failure"; readonly reason: string }>;
}

function createPersistenceInputProcessor(options: PersistenceInputProcessorOptions) {
  return async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const receivedAt = delivery.receivedAt ?? runtimeNow(options.dependencies.clock);
    const queue = getWorkerRoute("persistence").mainQueue.name;
    await emitRuntimeTelemetry(options.telemetry, {
      name: "runtime.message.started",
      level: "info",
      at: runtimeNow(options.dependencies.clock),
      stage: "persistence",
      queue,
      outcome: "started"
    });

    const envelopeResult = validateWorkerEnvelope(delivery.envelope);

    if (!envelopeResult.ok) {
      return {
        action: "dlq",
        reason: "invalid-envelope",
        issues: envelopeResult.issues.map(toRuntimeValidationIssue)
      };
    }

    const envelope = envelopeResult.value;

    if (envelope.route !== "persistence") {
      return terminalResult(envelope, "stage-mismatch", [
        {
          path: "$.route",
          code: "stage-mismatch",
          message: `Envelope route ${envelope.route} does not match processor stage persistence.`
        }
      ]);
    }

    const payloadResult = validateStagePayload(delivery.payload);

    if (!payloadResult.ok) {
      return terminalResult(envelope, "invalid-payload", payloadResult.issues.map(toRuntimeValidationIssue));
    }

    if (payloadResult.definition.consumer !== "persistence") {
      return terminalResult(envelope, "payload-consumer-mismatch", [
        {
          path: "$.schemaId",
          code: "payload-consumer-mismatch",
          message: `Payload schema consumer ${payloadResult.definition.consumer} does not match persistence.`
        }
      ]);
    }

    const claim = await options.dependencies.inboxStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "persistence",
      receivedAt
    });

    if (claim.status === "already-completed") {
      return {
        action: "ack",
        reason: "duplicate",
        envelope
      };
    }

    if (claim.status === "in-progress") {
      return retryOrDlq(envelope, "idempotency-in-progress", 1_000);
    }

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: "persistence",
      receivedAt
    };

    try {
      const result = await options.handler(context);

      if (result.status === "ok") {
        await markCompleted(options.dependencies.inboxStore, envelope, options.dependencies.clock);
        return {
          action: "ack",
          reason: "handled",
          envelope
        };
      }

      if (result.status === "retry") {
        await markFailed(options.dependencies.inboxStore, envelope, result.reason, true, options.dependencies.clock);
        return retryOrDlq(envelope, result.reason, result.retryAfterMs);
      }

      await markFailed(options.dependencies.inboxStore, envelope, result.reason, false, options.dependencies.clock);
      return terminalResult(envelope, result.reason);
    } catch (error: unknown) {
      await markFailed(options.dependencies.inboxStore, envelope, classifyHandlerError(error), true, options.dependencies.clock);
      return retryOrDlq(envelope, "handler-error");
    }
  };
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): PersistenceDependencyProbe | Promise<PersistenceDependencyProbe>;
  }
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await dependency.probe();

      return {
        status: probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

function finalShadowPermissionCheck(options: PersistenceServiceOptions): RuntimeHealthCheck {
  return {
    name: "final-shadow-permissions",
    critical: true,
    check: async () => {
      const probe = await options.dependencies.finalShadowTransactions.checkWriteScope();

      return {
        status: probe.status,
        details: {
          databaseRole: probe.details.databaseRole,
          shadowSchemaVersion: probe.details.shadowSchemaVersion,
          allowedWriteScopeCount: probe.details.allowedWriteScopes.length,
          allowedReadScopeCount: probe.details.allowedReadScopes.length,
          deniedWriteScopeCount: probe.details.deniedWriteScopes.length
        }
      };
    }
  };
}

function stageViewPermissionCheck(options: PersistenceServiceOptions): RuntimeHealthCheck {
  return {
    name: "stage-view-permissions",
    critical: true,
    check: async () => {
      const probe = await options.dependencies.stageViewReader.checkReadScope();

      return {
        status: probe.status,
        details: {
          databaseRole: probe.details.databaseRole,
          shadowSchemaVersion: probe.details.shadowSchemaVersion,
          allowedWriteScopeCount: probe.details.allowedWriteScopes.length,
          allowedReadScopeCount: probe.details.allowedReadScopes.length,
          deniedWriteScopeCount: probe.details.deniedWriteScopes.length
        }
      };
    }
  };
}

function backendApiCompatibilityCheck(options: PersistenceServiceOptions): RuntimeHealthCheck {
  return {
    name: "backend-api-compatibility",
    critical: true,
    check: async () => {
      const compatibility = await options.dependencies.backendApiClient.checkCompatibility(options.config.compatibility.backendApiVersion);

      return {
        status: compatibility.status,
        details: {
          summary: compatibility.summary,
          version: compatibility.version,
          requiredScopeCount: compatibility.requiredScopes.length,
          productionDomainWritesEnabled: compatibility.productionDomainWritesEnabled
        }
      };
    }
  };
}

function shadowModeCheck(config: PersistenceConfig): RuntimeHealthCheck {
  return {
    name: "shadow-mode",
    critical: true,
    check: () => config.shadowMode
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "shadow-mode-disabled"
          }
        }
  };
}

function productionWritesDisabledCheck(config: PersistenceConfig): RuntimeHealthCheck {
  return {
    name: "production-writes-disabled",
    critical: true,
    check: () => !config.security.productionWritesEnabled
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "production-writes-enabled"
          }
        }
  };
}

function retryOrDlq(envelope: WorkerMessageEnvelope, reason: string, retryAfterMs?: number): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  if ("ttlMs" in destination) {
    if (retryAfterMs === undefined) {
      return {
        action: "retry",
        reason,
        envelope,
        destination
      };
    }

    return {
      action: "retry",
      reason,
      envelope,
      destination,
      retryAfterMs
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination
  };
}

function terminalResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues?: readonly RuntimeValidationIssue[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  if (issues === undefined) {
    return {
      action: "dlq",
      reason,
      envelope,
      destination
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination,
    issues
  };
}

function toRuntimeValidationIssue(issue: StagePayloadValidationIssue | RuntimeValidationIssue): RuntimeValidationIssue {
  return {
    path: issue.path,
    code: issue.code,
    message: issue.message
  };
}

async function markCompleted(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  clock: PersistenceDependencies["clock"]
): Promise<void> {
  await store.markCompleted(envelope.idempotencyKey, {
    completedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    stage: "persistence"
  });
}

async function markFailed(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryable: boolean,
  clock: PersistenceDependencies["clock"]
): Promise<void> {
  await store.markFailed(envelope.idempotencyKey, {
    failedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    stage: "persistence",
    reason,
    retryable
  });
}

function classifyHandlerError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-handler-error";
}
