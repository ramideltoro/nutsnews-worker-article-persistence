import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export const PERSISTENCE_STAGE_METRIC_SERVICE = "persistence" as const;
export const PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;
export const PERSISTENCE_STAGE_METRIC_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const;

export type PersistenceStageMetricOutcome = (typeof PERSISTENCE_STAGE_METRIC_OUTCOMES)[number];
export type PersistenceHealthProbe = "liveness" | "startup" | "readiness";
export type PersistenceHealthOutcome = "ok" | "degraded" | "unhealthy";

export interface PersistenceMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: "local" | "test" | "shadow" | "production" | "unknown";
  readonly adapter?: "in_memory" | "mixed" | "production" | "unknown";
}

export interface PersistencePrometheusTelemetrySinkOptions extends Omit<PrometheusRuntimeTelemetrySinkOptions, "identity"> {
  readonly identity: PersistenceMetricIdentity;
}

export interface PersistenceRuntimeMetricsSink extends RuntimeTelemetrySink {
  collect(): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export interface PersistencePrometheusTelemetrySink extends PersistenceRuntimeMetricsSink {
  readonly allowedLabels: PrometheusRuntimeTelemetrySink["allowedLabels"];
  setHealthProbe(probe: PersistenceHealthProbe, outcome: PersistenceHealthOutcome, check?: string): void;
}

interface PersistenceStageHistogram {
  readonly buckets: number[];
  count: number;
  sum: number;
}

const MAX_LABEL_LENGTH = 96;
const PERSISTENCE_MAIN_QUEUE = "nutsnews.worker.persistence.v1";
const HEALTH_PROBES = [
  "liveness",
  "startup",
  "readiness"
] as const satisfies readonly PersistenceHealthProbe[];
const HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const satisfies readonly PersistenceHealthOutcome[];
const PERSISTENCE_RUNTIME_DEPENDENCIES = [
  "persistence-routing-work-handler",
  "feed-health-projection-handler",
  "local-persistence-work-handler"
] as const;
const PERSISTENCE_RUNTIME_HEALTH_CHECKS = [
  "process",
  "service-started",
  "broker-lifecycle",
  "rabbitmq-consumer",
  "persistence-inbox",
  "final-shadow-transactions",
  "stage-view-reader",
  "broker-outbox",
  "feed-health-projection-store",
  "backend-worker-api",
  "final-shadow-permissions",
  "stage-view-permissions",
  "backend-api-compatibility",
  "shadow-mode",
  "production-writes-disabled"
] as const;

export function createPersistencePrometheusTelemetrySink(
  options: PersistencePrometheusTelemetrySinkOptions
): PersistencePrometheusTelemetrySink {
  const runtimeSink = createPrometheusRuntimeTelemetrySink({
    ...options,
    cardinality: {
      dependencies: options.cardinality?.dependencies ?? PERSISTENCE_RUNTIME_DEPENDENCIES,
      healthChecks: options.cardinality?.healthChecks ?? PERSISTENCE_RUNTIME_HEALTH_CHECKS
    }
  });
  const eventCounts = new Map<PersistenceStageMetricOutcome, number>();
  const health = new Map<PersistenceHealthProbe, PersistenceHealthOutcome>([
    [
      "liveness",
      "ok"
    ],
    [
      "startup",
      "unhealthy"
    ],
    [
      "readiness",
      "unhealthy"
    ]
  ]);
  const histogram: PersistenceStageHistogram = {
    buckets: PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
  let lastSuccessTimestampSeconds: number | undefined;

  for (const [probe, outcome] of health) {
    setRuntimeHealthProbe(runtimeSink, probe, outcome);
  }

  return {
    allowedLabels: runtimeSink.allowedLabels,
    async emit(event): Promise<void> {
      const outcome = stageMetricOutcome(event);

      if (outcome !== undefined) {
        eventCounts.set(outcome, (eventCounts.get(outcome) ?? 0) + 1);
        const durationSeconds = durationSecondsFrom(event.durationMs);

        if (durationSeconds !== undefined) {
          observeHistogram(histogram, durationSeconds);
        }
      }

      observeHealthEvent(health, event);
      observeConsumerReadinessEvent(health, event, runtimeSink);
      lastSuccessTimestampSeconds = updateRuntimeLastSuccess(
        runtimeSink,
        event,
        lastSuccessTimestampSeconds
      );

      if (shouldForwardToRuntime(event)) {
        try {
          await runtimeSink.emit(event);
        } catch {
          // Compatibility metrics are best effort and cannot alter delivery semantics.
        }
      }
    },
    collect(): string {
      const runtimeMetrics = collectRuntimeMetrics(runtimeSink);
      const identityMetrics = collectCompatibilityIdentityMetrics(options, runtimeMetrics);
      const stageMetrics = collectStageMetrics(options, eventCounts, histogram);
      const healthMetrics = collectHealthMetrics(options, health, runtimeMetrics);

      return [
        runtimeMetrics,
        identityMetrics,
        healthMetrics,
        stageMetrics
      ].filter((value) => value.length > 0).join("\n").concat("\n");
    },
    setInFlight(queue, value): void {
      runBestEffort(() => runtimeSink.setInFlight(queue, value));
    },
    setShutdownDraining(draining): void {
      runBestEffort(() => runtimeSink.setShutdownDraining(draining));
    },
    setHealthProbe(probe, outcome, check): void {
      health.set(probe, outcome);
      setRuntimeHealthProbe(runtimeSink, probe, outcome, check);
    }
  };
}

function collectCompatibilityIdentityMetrics(
  options: PersistencePrometheusTelemetrySinkOptions,
  runtimeOutput: string
): string {
  const identity = options.identity;
  const environment = metricLabelValue(identity.environment);
  const service = metricLabelValue(identity.service);
  const lines: string[] = [];

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_build_info")) {
    lines.push(
      "# HELP nutsnews_worker_build_info Immutable worker build identity.",
      "# TYPE nutsnews_worker_build_info gauge",
      `nutsnews_worker_build_info{environment="${escapeLabelValue(environment)}",service="${escapeLabelValue(service)}",version="${escapeLabelValue(metricLabelValue(identity.version))}",revision="${escapeLabelValue(metricLabelValue(identity.revision ?? "unknown"))}"} 1`
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_deployment_info")) {
    lines.push(
      "# HELP nutsnews_worker_deployment_info Worker deployment ownership and dependency adapter identity.",
      "# TYPE nutsnews_worker_deployment_info gauge",
      `nutsnews_worker_deployment_info{environment="${escapeLabelValue(environment)}",service="${escapeLabelValue(service)}",deployment="${escapeLabelValue(metricLabelValue(identity.deployment ?? "unknown"))}",adapter="${escapeLabelValue(metricLabelValue(identity.adapter ?? "unknown"))}"} 1`
    );
  }

  return lines.join("\n");
}

function hasMetricFamily(output: string, metric: string): boolean {
  return output.split("\n").some((line) => line.startsWith(`# HELP ${metric} `)
    || line.startsWith(`${metric}{`)
    || line.startsWith(`${metric} `));
}

function collectRuntimeMetrics(runtimeSink: PersistenceRuntimeMetricsSink): string {
  try {
    return runtimeSink.collect().trimEnd();
  } catch {
    return "";
  }
}

function shouldForwardToRuntime(event: RuntimeTelemetryEvent): boolean {
  if (event.name !== "runtime.dependency.observed") {
    return true;
  }

  const attributeDuration = event.attributes?.durationMs;

  return (event.durationMs !== undefined && Number.isFinite(event.durationMs))
    || (typeof attributeDuration === "number" && Number.isFinite(attributeDuration));
}

function runBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // Compatibility metrics are best effort and cannot alter delivery semantics.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

function observeHealthEvent(
  health: Map<PersistenceHealthProbe, PersistenceHealthOutcome>,
  event: RuntimeTelemetryEvent
): void {
  if (event.name !== "runtime.health.evaluated") {
    return;
  }

  const probe = event.attributes?.probe;
  const outcome = event.outcome;

  if (isHealthProbe(probe) && isHealthOutcome(outcome)) {
    health.set(probe, outcome);
  }
}

function observeConsumerReadinessEvent(
  health: Map<PersistenceHealthProbe, PersistenceHealthOutcome>,
  event: RuntimeTelemetryEvent,
  runtimeSink: PrometheusRuntimeTelemetrySink
): void {
  if (
    event.name !== "runtime.broker.consumer_state_changed"
    || event.stage !== PERSISTENCE_STAGE_METRIC_SERVICE
    || event.queue !== PERSISTENCE_MAIN_QUEUE
  ) {
    return;
  }

  const activeConsumers = event.attributes?.activeConsumers;
  const consumerCount = typeof activeConsumers === "number" && Number.isFinite(activeConsumers)
    ? Math.max(0, Math.floor(activeConsumers))
    : event.outcome === "active" ? 1 : 0;

  if (consumerCount === 0) {
    health.set("readiness", "unhealthy");
    setRuntimeHealthProbe(runtimeSink, "readiness", "unhealthy", "rabbitmq-consumer");
  }
}

function collectHealthMetrics(
  options: PrometheusRuntimeTelemetrySinkOptions,
  health: ReadonlyMap<PersistenceHealthProbe, PersistenceHealthOutcome>,
  runtimeOutput: string
): string {
  if (hasMetricFamily(runtimeOutput, "nutsnews_worker_health_probe")) {
    return "";
  }

  const environment = metricLabelValue(options.identity.environment);
  const lines = [
    "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by bounded probe and outcome.",
    "# TYPE nutsnews_worker_health_probe gauge"
  ];

  for (const probe of HEALTH_PROBES) {
    const observed = health.get(probe);

    if (observed === undefined) {
      continue;
    }

    for (const outcome of HEALTH_OUTCOMES) {
      lines.push(`nutsnews_worker_health_probe${healthLabels(environment, PERSISTENCE_STAGE_METRIC_SERVICE, probe, outcome)} ${outcome === observed ? "1" : "0"}`);
    }
  }

  return lines.join("\n");
}

function setRuntimeHealthProbe(
  runtimeSink: PrometheusRuntimeTelemetrySink,
  probe: PersistenceHealthProbe,
  outcome: PersistenceHealthOutcome,
  check?: string
): void {
  const checks = check === undefined
    ? []
    : [
        {
          name: check,
          status: outcome,
          critical: true,
          durationMs: 0
        }
      ];

  runBestEffort(() => runtimeSink.emit({
    name: "runtime.health.evaluated",
    level: outcome === "ok" ? "info" : "warn",
    at: new Date().toISOString(),
    outcome,
    attributes: {
      probe,
      status: outcome,
      checkCount: checks.length,
      checks
    }
  }));
}

function updateRuntimeLastSuccess(
  runtimeSink: PrometheusRuntimeTelemetrySink,
  event: RuntimeTelemetryEvent,
  current: number | undefined
): number | undefined {
  if (event.name !== "runtime.message.accepted" && event.name !== "runtime.message.duplicate") {
    return current;
  }

  const parsedTimestampSeconds = Math.floor(Date.parse(event.at) / 1_000);

  if (!Number.isFinite(parsedTimestampSeconds) || parsedTimestampSeconds < 0) {
    return current;
  }

  const next = Math.max(current ?? 0, parsedTimestampSeconds);
  runBestEffort(() => runtimeSink.setLastSuccessTimestamp(next));

  return next;
}

function healthLabels(
  environment: string,
  service: string,
  probe: PersistenceHealthProbe,
  outcome: PersistenceHealthOutcome
): string {
  return `{environment="${escapeLabelValue(environment)}",service="${escapeLabelValue(service)}",probe="${escapeLabelValue(probe)}",outcome="${escapeLabelValue(outcome)}"}`;
}

function isHealthProbe(value: unknown): value is PersistenceHealthProbe {
  return typeof value === "string" && (HEALTH_PROBES as readonly string[]).includes(value);
}

function isHealthOutcome(value: unknown): value is PersistenceHealthOutcome {
  return typeof value === "string" && (HEALTH_OUTCOMES as readonly string[]).includes(value);
}

function stageMetricOutcome(event: RuntimeTelemetryEvent): PersistenceStageMetricOutcome | undefined {
  if (event.stage !== PERSISTENCE_STAGE_METRIC_SERVICE || event.queue !== PERSISTENCE_MAIN_QUEUE) {
    return undefined;
  }

  switch (event.name) {
    case "runtime.message.accepted":
      return "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    case "runtime.broker.consumer_state_changed":
    case "runtime.broker.state_changed":
    case "runtime.broker.topology_asserted":
    case "runtime.dependency.observed":
    case "runtime.health.evaluated":
    case "runtime.message.started":
    case "runtime.shutdown.completed":
    case "runtime.shutdown.failed":
    case "runtime.shutdown.started":
      return undefined;
  }
}

function observeHistogram(histogram: PersistenceStageHistogram, value: number): void {
  histogram.count += 1;
  histogram.sum += value;

  for (const [index, boundary] of PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS.entries()) {
    if (value <= boundary) {
      histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    }
  }
}

function collectStageMetrics(
  options: PrometheusRuntimeTelemetrySinkOptions,
  eventCounts: ReadonlyMap<PersistenceStageMetricOutcome, number>,
  histogram: PersistenceStageHistogram
): string {
  const environment = metricLabelValue(options.identity.environment);
  const service = PERSISTENCE_STAGE_METRIC_SERVICE;
  const lines = [
    "# HELP nutsnews_worker_uplift_stage_events_total Completed worker-uplift stage delivery outcomes.",
    "# TYPE nutsnews_worker_uplift_stage_events_total counter"
  ];

  for (const outcome of PERSISTENCE_STAGE_METRIC_OUTCOMES) {
    const count = eventCounts.get(outcome) ?? 0;

    lines.push(`nutsnews_worker_uplift_stage_events_total${stageEventLabels(environment, service, outcome)} ${String(count)}`);
  }

  lines.push("# HELP nutsnews_worker_uplift_stage_latency_seconds Worker-uplift stage delivery completion latency in seconds.");
  lines.push("# TYPE nutsnews_worker_uplift_stage_latency_seconds histogram");

  for (const [index, boundary] of PERSISTENCE_STAGE_HISTOGRAM_BUCKETS_SECONDS.entries()) {
    lines.push(`nutsnews_worker_uplift_stage_latency_seconds_bucket${stageHistogramLabels(environment, service, String(boundary))} ${String(histogram.buckets[index] ?? 0)}`);
  }

  lines.push(`nutsnews_worker_uplift_stage_latency_seconds_bucket${stageHistogramLabels(environment, service, "+Inf")} ${String(histogram.count)}`);
  lines.push(`nutsnews_worker_uplift_stage_latency_seconds_sum${stageHistogramBaseLabels(environment, service)} ${formatMetricNumber(histogram.sum)}`);
  lines.push(`nutsnews_worker_uplift_stage_latency_seconds_count${stageHistogramBaseLabels(environment, service)} ${String(histogram.count)}`);

  return lines.join("\n");
}

function stageEventLabels(environment: string, service: string, outcome: PersistenceStageMetricOutcome): string {
  return `{environment="${escapeLabelValue(environment)}",service="${escapeLabelValue(service)}",outcome="${escapeLabelValue(outcome)}"}`;
}

function stageHistogramLabels(environment: string, service: string, boundary: string): string {
  return `{environment="${escapeLabelValue(environment)}",service="${escapeLabelValue(service)}",le="${escapeLabelValue(boundary)}"}`;
}

function stageHistogramBaseLabels(environment: string, service: string): string {
  return `{environment="${escapeLabelValue(environment)}",service="${escapeLabelValue(service)}"}`;
}

function metricLabelValue(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "_")
    .slice(0, MAX_LABEL_LENGTH);

  return cleaned.length > 0 ? cleaned : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}

function durationSecondsFrom(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) ? Math.max(0, value) / 1_000 : undefined;
}
