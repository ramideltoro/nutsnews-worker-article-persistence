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
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthReport,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyClaimReleaseResult,
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
  PersistenceDependencyProbe,
  PersistenceBrokerTransport
} from "./dependencies.js";
import { sha256Digest } from "./digest.js";
import { classifyPersistenceError } from "./errors.js";
import type {
  PersistenceQuarantineRecord
} from "./materialization-types.js";
import type {
  PersistenceHealthOutcome,
  PersistenceHealthProbe,
  PersistencePrometheusTelemetrySink,
  PersistenceRuntimeMetricsSink
} from "./telemetry.js";

export interface PersistenceServiceOptions {
  readonly config: PersistenceConfig;
  readonly dependencies: PersistenceDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PersistenceRuntimeMetricsSink;
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
  assertDependencyModeCompatibility(options.config, options.dependencies);
  const persistenceRoute = getWorkerRoute("persistence");
  const publicationRoute = getWorkerRoute("publication");
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      persistenceRoute,
      publicationRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createPersistenceInputProcessor({
    dependencies: options.dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context, signal) => {
      try {
        return await drain.track(async () => {
          signal.throwIfAborted();
          setInFlight(options.metrics, persistenceRoute.mainQueue.name, drain.inFlight);
          const dependencyStartedAtMs = options.dependencies.clock.now().getTime();
          const result = await options.dependencies.workHandler.handle(context, {
            signal,
            publish: (command) => publishWithOwnershipSignal(
              options.dependencies.brokerTransport,
              broker,
              command,
              signal
            ),
            recordOutbox: (command, receipt) => runAbortableOperation(
              signal,
              () => options.dependencies.brokerOutbox.record(command, receipt)
            ),
            withTransaction: (operation) => runAbortableOperation(
              signal,
              () => options.dependencies.finalShadowTransactions.withTransaction(async (transaction) => {
                signal.throwIfAborted();
                const value = await operation(transaction);
                signal.throwIfAborted();
                return value;
              })
            )
          });
          signal.throwIfAborted();

          await emitRuntimeTelemetry(telemetry, {
            name: "runtime.dependency.observed",
            level: result.status === "ok" ? "info" : "warn",
            at: runtimeNow(options.dependencies.clock),
            stage: "persistence",
            queue: persistenceRoute.mainQueue.name,
            durationMs: elapsedMs(options.dependencies.clock, dependencyStartedAtMs),
            outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
            attributes: {
              event: "persistence.message.delegated",
              dependency: options.dependencies.workHandler.name,
              ...(result.status === "ok" ? {} : {
                reason: result.reason
              }),
              shadowMode: options.config.shadowMode,
              productionWritesEnabled: options.config.security.productionWritesEnabled
            }
          });

          return result;
        });
      } finally {
        setInFlight(options.metrics, persistenceRoute.mainQueue.name, drain.inFlight);
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
      const probes = createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "persistence"),
          dependencyReadinessCheck("persistence-inbox", options.dependencies.inboxStore),
          dependencyReadinessCheck("final-shadow-transactions", options.dependencies.finalShadowTransactions),
          dependencyReadinessCheck("stage-view-reader", options.dependencies.stageViewReader),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          dependencyReadinessCheck("feed-health-projection-store", options.dependencies.feedHealthProjectionStore),
          dependencyReadinessCheck("backend-worker-api", options.dependencies.backendApiClient),
          finalShadowPermissionCheck(options),
          stageViewPermissionCheck(options),
          backendApiCompatibilityCheck(options),
          shadowModeCheck(options.config),
          productionWritePolicyCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      });

      return observeHealthProbes(probes, options.metrics);
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
      const brokerConsumer = await broker.consume("persistence", processor);
      consumer = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          await brokerConsumer.cancel();
          setHealthProbe(options.metrics, "readiness", "unhealthy", "rabbitmq-consumer");
        }
      };
      started = true;
      setHealthProbe(options.metrics, "startup", "ok");
      setInFlight(options.metrics, persistenceRoute.mainQueue.name, drain.inFlight);
      await refreshReadinessBestEffort(
        () => service.health.readiness(),
        options.metrics
      );
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      setShutdownDraining(options.metrics, false);
      setInFlight(options.metrics, persistenceRoute.mainQueue.name, drain.inFlight);
      setHealthProbe(options.metrics, "startup", "unhealthy");
      setHealthProbe(options.metrics, "readiness", "unhealthy");
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies PersistenceService;

  return service;
}

function assertDependencyModeCompatibility(
  config: PersistenceConfig,
  dependencies: PersistenceDependencies
): void {
  const expectedAdapterMode = config.dependencyMode === "production" ? "production" : "in_memory";
  const expectedStateStoreMode = config.dependencyMode === "production" ? "postgresql" : "in_memory";

  if (
    dependencies.adapterMode !== expectedAdapterMode
    || dependencies.stateStoreMode !== expectedStateStoreMode
  ) {
    throw new Error(
      `Persistence dependency mode mismatch: ${config.dependencyMode} requires adapter=${expectedAdapterMode} and stateStore=${expectedStateStoreMode}.`
    );
  }
}

function bestEffortTelemetrySink(sink: RuntimeTelemetrySink | undefined): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // Telemetry is non-semantic and must never change message disposition.
      }
    }
  };
}

function setInFlight(
  metrics: PersistenceRuntimeMetricsSink | undefined,
  queue: string,
  value: number
): void {
  runBestEffort(() => metrics?.setInFlight(queue, value));
}

function setShutdownDraining(
  metrics: PersistenceRuntimeMetricsSink | undefined,
  draining: boolean
): void {
  runBestEffort(() => metrics?.setShutdownDraining(draining));
}

function setHealthProbe(
  metrics: PersistenceRuntimeMetricsSink | undefined,
  probe: PersistenceHealthProbe,
  outcome: PersistenceHealthOutcome,
  check?: string
): void {
  if (isPersistenceMetrics(metrics)) {
    runBestEffort(() => metrics.setHealthProbe(probe, outcome, check));
  }
}

function observeHealthProbes(
  probes: RuntimeHealthProbeSet,
  metrics: PersistenceRuntimeMetricsSink | undefined
): RuntimeHealthProbeSet {
  const observe = async <T extends RuntimeHealthReport>(
    probe: PersistenceHealthProbe,
    operation: () => Promise<T>
  ): Promise<T> => {
    const report = await operation();
    setHealthProbe(metrics, probe, report.status);

    return report;
  };

  return {
    liveness: () => observe("liveness", () => probes.liveness()),
    startup: () => observe("startup", () => probes.startup()),
    readiness: () => observe("readiness", () => probes.readiness())
  };
}

async function refreshReadinessBestEffort(
  operation: () => Promise<RuntimeHealthReport>,
  metrics: PersistenceRuntimeMetricsSink | undefined
): Promise<void> {
  try {
    await operation();
  } catch {
    setHealthProbe(metrics, "readiness", "unhealthy");
  }
}

function isPersistenceMetrics(
  metrics: PersistenceRuntimeMetricsSink | undefined
): metrics is PersistencePrometheusTelemetrySink {
  return metrics !== undefined
    && "setHealthProbe" in metrics
    && typeof metrics.setHealthProbe === "function";
}

function runBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // Metrics are non-semantic and must never change message disposition.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

interface PersistenceInputProcessorOptions {
  readonly dependencies: PersistenceDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  handler(
    context: RuntimeMessageContext,
    signal: AbortSignal
  ): Promise<{ readonly status: "ok" } | { readonly status: "retry"; readonly reason: string; readonly retryAfterMs?: number } | { readonly status: "terminal-failure"; readonly reason: string }>;
}

// Persistence owns all schemas whose definition.consumer is persistence, including
// translationResult, and adds payload-fingerprint quarantine before Runtime 1-style
// token-aware idempotency transitions.
function createPersistenceInputProcessor(options: PersistenceInputProcessorOptions) {
  return async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const receivedAt = delivery.receivedAt ?? runtimeNow(options.dependencies.clock);
    const startedAtMs = options.dependencies.clock.now().getTime();
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
      const issues = envelopeResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(
        options.telemetry,
        undefined,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return {
        action: "dlq",
        reason: "invalid-envelope",
        issues
      };
    }

    const envelope = envelopeResult.value;

    if (envelope.route !== "persistence") {
      const issues = [
        {
          path: "$.route",
          code: "stage-mismatch",
          message: `Envelope route ${envelope.route} does not match processor stage persistence.`
        }
      ];
      await emitInvalid(
        options.telemetry,
        envelope,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return terminalResult(envelope, "stage-mismatch", issues);
    }

    const payloadResult = validateStagePayload(delivery.payload);

    if (!payloadResult.ok) {
      const issues = payloadResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(
        options.telemetry,
        envelope,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return terminalResult(envelope, "invalid-payload", issues);
    }

    if (payloadResult.definition.consumer !== "persistence") {
      const issues = [
        {
          path: "$.schemaId",
          code: "payload-consumer-mismatch",
          message: `Payload schema consumer ${payloadResult.definition.consumer} does not match persistence.`
        }
      ];
      await emitInvalid(
        options.telemetry,
        envelope,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return terminalResult(envelope, "payload-consumer-mismatch", issues);
    }

    const payloadFingerprint = sha256Digest({
      aggregate: envelope.aggregate,
      payload: payloadResult.value
    });
    let fingerprint: Awaited<ReturnType<PersistenceDependencies["inboxStore"]["verifyPayloadFingerprint"]>>;

    try {
      fingerprint = await options.dependencies.inboxStore.verifyPayloadFingerprint(envelope.idempotencyKey, payloadFingerprint);
    } catch {
      return completeProcessingFailure(
        options.telemetry,
        envelope,
        "payload-fingerprint-verification-error",
        true,
        options.dependencies.clock,
        queue,
        startedAtMs
      );
    }

    if (fingerprint.status === "conflict") {
      return quarantinePayloadFingerprintConflict(
        options,
        envelope,
        payloadResult.value,
        payloadFingerprint,
        fingerprint.existingFingerprint,
        queue,
        startedAtMs
      );
    }

    let claim: Awaited<ReturnType<RuntimeIdempotencyStore["claim"]>>;

    try {
      claim = await options.dependencies.inboxStore.claim(
        envelope.idempotencyKey,
        {
          envelope,
          stage: "persistence",
          receivedAt
        },
        payloadFingerprint
      );
    } catch {
      let racedFingerprint: Awaited<ReturnType<PersistenceDependencies["inboxStore"]["verifyPayloadFingerprint"]>>;

      try {
        racedFingerprint = await options.dependencies.inboxStore.verifyPayloadFingerprint(
          envelope.idempotencyKey,
          payloadFingerprint
        );
      } catch {
        racedFingerprint = {
          status: "accepted"
        };
      }

      if (racedFingerprint.status === "conflict") {
        return quarantinePayloadFingerprintConflict(
          options,
          envelope,
          payloadResult.value,
          payloadFingerprint,
          racedFingerprint.existingFingerprint,
          queue,
          startedAtMs
        );
      }

      return completeProcessingFailure(
        options.telemetry,
        envelope,
        "idempotency-claim-error",
        true,
        options.dependencies.clock,
        queue,
        startedAtMs
      );
    }

    if (claim.status === "already-completed") {
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.duplicate",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "persistence",
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(options.dependencies.clock, startedAtMs)),
        outcome: "duplicate",
        attributes: {
          firstSeenAt: claim.firstSeenAt,
          completedAt: claim.completedAt
        }
      });

      return {
        action: "ack",
        reason: "duplicate",
        envelope
      };
    }

    if (claim.status === "in-progress") {
      const result = retryOrDlq(envelope, "idempotency-in-progress", 1_000);
      await emitRetryOrDlq(
        options.telemetry,
        result,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return result;
    }

    const claimToken = claim.claimToken;

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: "persistence",
      receivedAt
    };

    const claimLease = startPersistenceClaimLeaseHeartbeat(
      options.dependencies.inboxStore,
      envelope.idempotencyKey,
      claimToken
    );
    let handlerOutcome:
      | {
          readonly status: "returned";
          readonly result: Awaited<ReturnType<PersistenceInputProcessorOptions["handler"]>>;
        }
      | {
          readonly status: "threw";
          readonly error: unknown;
        };

    try {
      handlerOutcome = {
        status: "returned",
        result: await options.handler(context, claimLease.signal)
      };
    } catch (error: unknown) {
      handlerOutcome = {
        status: "threw",
        error
      };
    }

    await claimLease.stop();

    if (claimLease.ownershipLost) {
      return completeProcessingFailure(
        options.telemetry,
        envelope,
        "idempotency-lease-lost",
        true,
        options.dependencies.clock,
        queue,
        startedAtMs
      );
    }

    if (handlerOutcome.status === "threw") {
      const classification = classifyPersistenceError(handlerOutcome.error);
      const failureRecorded = await tryMarkFailed(
        options.dependencies.inboxStore,
        envelope,
        claimToken,
        classification.reason,
        classification.retryable,
        options.dependencies.clock
      );

      if (!failureRecorded) {
        return completeProcessingFailure(
          options.telemetry,
          envelope,
          "idempotency-failure-record-error",
          true,
          options.dependencies.clock,
          queue,
          startedAtMs
        );
      }

      return completeProcessingFailure(
        options.telemetry,
        envelope,
        classification.reason,
        classification.retryable,
        options.dependencies.clock,
        queue,
        startedAtMs
      );
    }

    const result = handlerOutcome.result;

    if (result.status === "ok") {
      try {
        await markCompleted(options.dependencies.inboxStore, envelope, claimToken, options.dependencies.clock);
      } catch {
        const releaseResult = await tryReleaseClaim(
          options.dependencies.inboxStore,
          envelope,
          claimToken,
          "idempotency-completion-error",
          true,
          options.dependencies.clock
        );

        if (releaseResult === "release-error" || releaseResult.status !== "preserved-completed") {
          return completeProcessingFailure(
            options.telemetry,
            envelope,
            "idempotency-completion-error",
            true,
            options.dependencies.clock,
            queue,
            startedAtMs
          );
        }
      }

      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.accepted",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "persistence",
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(options.dependencies.clock, startedAtMs)),
        outcome: "success"
      });

      return {
        action: "ack",
        reason: "handled",
        envelope
      };
    }

    if (result.status === "retry") {
      const failureRecorded = await tryMarkFailed(
        options.dependencies.inboxStore,
        envelope,
        claimToken,
        result.reason,
        true,
        options.dependencies.clock
      );

      if (!failureRecorded) {
        return completeProcessingFailure(
          options.telemetry,
          envelope,
          "idempotency-failure-record-error",
          true,
          options.dependencies.clock,
          queue,
          startedAtMs
        );
      }

      return completeProcessingFailure(
        options.telemetry,
        envelope,
        result.reason,
        true,
        options.dependencies.clock,
        queue,
        startedAtMs,
        result.retryAfterMs
      );
    }

    const failureRecorded = await tryMarkFailed(
      options.dependencies.inboxStore,
      envelope,
      claimToken,
      result.reason,
      false,
      options.dependencies.clock
    );

    if (!failureRecorded) {
      return completeProcessingFailure(
        options.telemetry,
        envelope,
        "idempotency-failure-record-error",
        true,
        options.dependencies.clock,
        queue,
        startedAtMs
      );
    }

    return completeProcessingFailure(
      options.telemetry,
      envelope,
      result.reason,
      false,
      options.dependencies.clock,
      queue,
      startedAtMs
    );
  };
}

async function quarantinePayloadFingerprintConflict(
  options: PersistenceInputProcessorOptions,
  envelope: WorkerMessageEnvelope,
  payload: Readonly<Record<string, unknown>>,
  payloadFingerprint: string,
  existingFingerprint: string,
  queue: string,
  startedAtMs: number
): Promise<RuntimeMessageProcessingResult> {
  try {
    await options.dependencies.finalShadowTransactions.withTransaction(async (transaction) => {
      await options.dependencies.finalShadowTransactions.recordQuarantine(transaction, createPayloadConflictQuarantineRecord(
        envelope,
        payload,
        payloadFingerprint,
        existingFingerprint
      ));
    });
  } catch {
    const result = retryOrDlq(envelope, "quarantine-record-error");
    await emitRetryOrDlq(
      options.telemetry,
      result,
      options.dependencies.clock,
      queue,
      elapsedMs(options.dependencies.clock, startedAtMs)
    );

    return result;
  }

  const result = terminalResult(envelope, "idempotency-payload-conflict");
  await emitRetryOrDlq(
    options.telemetry,
    result,
    options.dependencies.clock,
    queue,
    elapsedMs(options.dependencies.clock, startedAtMs)
  );

  return result;
}

interface PersistenceClaimLeaseHeartbeat {
  readonly ownershipLost: boolean;
  readonly signal: AbortSignal;
  stop(): Promise<void>;
}

function startPersistenceClaimLeaseHeartbeat(
  store: PersistenceDependencies["inboxStore"],
  idempotencyKey: string,
  claimToken: string
): PersistenceClaimLeaseHeartbeat {
  const leaseMs = store.claimLeaseMs;
  const controller = new AbortController();

  if (leaseMs === undefined) {
    return {
      ownershipLost: false,
      signal: controller.signal,
      stop: () => Promise.resolve()
    };
  }

  const intervalMs = Math.max(1, Math.min(60_000, Math.floor(leaseMs / 3)));
  let ownershipLost = false;
  let stopped = false;
  let renewal: Promise<void> | undefined;
  const loseOwnership = (reason: unknown): void => {
    ownershipLost = true;

    if (!controller.signal.aborted) {
      controller.abort(reason);
    }
  };

  const renew = (): Promise<void> => {
    if (ownershipLost) {
      return Promise.resolve();
    }

    if (renewal !== undefined) {
      return renewal;
    }

    renewal = store.renewClaim(idempotencyKey, claimToken)
      .then((result) => {
        if (result.status === "not-owned") {
          loseOwnership(new Error("Persistence idempotency claim is no longer owned."));
        }
      })
      .catch((error: unknown) => {
        loseOwnership(error);
        throw error;
      })
      .finally(() => {
        renewal = undefined;
      });

    return renewal;
  };
  const timer = setInterval(() => {
    if (!stopped) {
      void renew().catch(() => undefined);
    }
  }, intervalMs);
  timer.unref();

  return {
    get ownershipLost(): boolean {
      return ownershipLost;
    },
    signal: controller.signal,
    async stop(): Promise<void> {
      stopped = true;
      clearInterval(timer);

      try {
        await renew();
      } catch (error: unknown) {
        loseOwnership(error);
      }
    }
  };
}

async function runAbortableOperation<T>(
  signal: AbortSignal,
  operation: () => Promise<T>
): Promise<T> {
  signal.throwIfAborted();
  const value = await operation();
  signal.throwIfAborted();
  return value;
}

async function publishWithOwnershipSignal(
  transport: PersistenceBrokerTransport,
  broker: BrokerLifecycle,
  command: BrokerPublishCommand,
  signal: AbortSignal
): Promise<BrokerPublishReceipt> {
  signal.throwIfAborted();

  if (transport.publishWithSignal === undefined) {
    return runAbortableOperation(signal, () => broker.publish(command));
  }

  if (broker.state !== "ready") {
    throw new Error(`Broker lifecycle must be ready before use; current state is ${broker.state}.`);
  }

  const receipt = await transport.publishWithSignal(command, signal);
  signal.throwIfAborted();
  const route = getWorkerRoute(command.envelope.route);

  if (receipt.exchange !== route.exchange || receipt.routingKey !== route.routingKey) {
    throw new Error("Broker transport returned a publish receipt for the wrong route.");
  }

  return receipt;
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

function productionWritePolicyCheck(config: PersistenceConfig): RuntimeHealthCheck {
  return {
    name: "production-write-policy",
    critical: true,
    check: () => !config.security.productionWritesEnabled || (
      config.dependencyMode === "production"
      && config.security.productionWriteConfirmationValid
    )
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "production-write-policy-invalid"
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

async function completeProcessingFailure(
  telemetry: RuntimeTelemetrySink | undefined,
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryable: boolean,
  clock: PersistenceDependencies["clock"],
  queue: string,
  startedAtMs: number,
  retryAfterMs?: number
): Promise<RuntimeMessageProcessingResult> {
  const result = retryable
    ? retryOrDlq(envelope, reason, retryAfterMs)
    : terminalResult(envelope, reason);

  await emitRetryOrDlq(
    telemetry,
    result,
    clock,
    queue,
    elapsedMs(clock, startedAtMs)
  );

  return result;
}

function toRuntimeValidationIssue(issue: StagePayloadValidationIssue | RuntimeValidationIssue): RuntimeValidationIssue {
  return {
    path: issue.path,
    code: issue.code,
    message: issue.message
  };
}

async function emitInvalid(
  telemetry: RuntimeTelemetrySink | undefined,
  envelope: WorkerMessageEnvelope | undefined,
  issues: readonly RuntimeValidationIssue[],
  clock: PersistenceDependencies["clock"],
  queue: string,
  durationMs: number
): Promise<void> {
  const firstIssue = issues[0];
  const attributes = firstIssue === undefined
    ? undefined
    : {
        issueCode: firstIssue.code,
        issuePath: firstIssue.path
      };

  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.message.invalid",
    level: "warn",
    at: runtimeNow(clock),
    stage: "persistence",
    queue,
    durationMs,
    outcome: "failure",
    ...(envelope === undefined
      ? {}
      : envelopeTelemetryFields(envelope, queue, durationMs)),
    ...(attributes === undefined
      ? {}
      : {
          attributes
        })
  });
}

async function emitRetryOrDlq(
  telemetry: RuntimeTelemetrySink | undefined,
  result: RuntimeMessageProcessingResult,
  clock: PersistenceDependencies["clock"],
  queue: string,
  durationMs: number
): Promise<void> {
  if (result.action === "retry") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.retry",
      level: "warn",
      at: runtimeNow(clock),
      stage: "persistence",
      ...envelopeTelemetryFields(result.envelope, queue, durationMs),
      outcome: "retry",
      attributes: {
        reason: result.reason,
        destination: result.destination.name
      }
    });

    return;
  }

  if (result.action === "dlq") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.dlq",
      level: "error",
      at: runtimeNow(clock),
      stage: "persistence",
      ...(result.envelope === undefined
        ? {}
        : envelopeTelemetryFields(result.envelope, queue, durationMs)),
      outcome: "dlq",
      attributes: {
        reason: result.reason,
        destination: result.destination?.name ?? "unroutable"
      }
    });
  }
}

function elapsedMs(clock: PersistenceDependencies["clock"], startedAtMs: number): number {
  return Math.max(0, clock.now().getTime() - startedAtMs);
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  if (envelope.tracestate === undefined) {
    return base;
  }

  return {
    ...base,
    tracestate: envelope.tracestate
  };
}

async function markCompleted(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  claimToken: string,
  clock: PersistenceDependencies["clock"]
): Promise<void> {
  await store.markCompleted(envelope.idempotencyKey, {
    completedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    claimToken,
    stage: "persistence"
  });
}

async function markFailed(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  claimToken: string,
  reason: string,
  retryable: boolean,
  clock: PersistenceDependencies["clock"]
): Promise<void> {
  await store.markFailed(envelope.idempotencyKey, {
    failedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    claimToken,
    stage: "persistence",
    reason,
    retryable
  });
}

async function tryMarkFailed(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  claimToken: string,
  reason: string,
  retryable: boolean,
  clock: PersistenceDependencies["clock"]
): Promise<boolean> {
  try {
    await markFailed(store, envelope, claimToken, reason, retryable, clock);
    return true;
  } catch {
    return false;
  }
}

async function tryReleaseClaim(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  claimToken: string,
  reason: string,
  retryable: boolean,
  clock: PersistenceDependencies["clock"]
): Promise<RuntimeIdempotencyClaimReleaseResult | "release-error"> {
  try {
    return await store.releaseClaim(envelope.idempotencyKey, {
      failedAt: runtimeNow(clock),
      messageId: envelope.messageId,
      claimToken,
      stage: "persistence",
      reason,
      retryable
    });
  } catch {
    return "release-error";
  }
}

function createPayloadConflictQuarantineRecord(
  envelope: WorkerMessageEnvelope,
  payload: Readonly<Record<string, unknown>>,
  payloadFingerprint: string,
  existingFingerprint: string
): PersistenceQuarantineRecord {
  const pipelineRunId = optionalString(payload.pipelineRunId);
  const stageExecutionId = optionalString(payload.stageExecutionId);
  const sourceMessageId = optionalString(payload.sourceMessageId);

  return {
    idempotencyKey: envelope.idempotencyKey,
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    articleId: envelope.aggregate.id,
    articleVersion: envelope.aggregate.version,
    reason: "idempotency-payload-conflict",
    payloadDigest: payloadFingerprint,
    diagnosticMetadata: {
      existingFingerprint,
      payloadFingerprint,
      safeMetadataOnly: true
    },
    ...(pipelineRunId === undefined ? {} : {
      pipelineRunId
    }),
    ...(stageExecutionId === undefined ? {} : {
      stageExecutionId
    }),
    ...(sourceMessageId === undefined ? {} : {
      sourceMessageId
    })
  };
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}
