import {
  beforeEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

const runtime1Delegate = vi.hoisted(() => ({
  forwardedEventNames: [] as string[],
  health: new Map<string, string>()
}));

vi.mock("@ramideltoro/nutsnews-worker-runtime", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@ramideltoro/nutsnews-worker-runtime")>();

  return {
    ...actual,
    createPrometheusRuntimeTelemetrySink: () => ({
      allowedLabels: actual.RUNTIME_ALLOWED_METRIC_LABELS,
      emit(event: Parameters<typeof actual.emitRuntimeTelemetry>[1]): void {
        runtime1Delegate.forwardedEventNames.push(event.name);

        if (event.name === "runtime.health.evaluated") {
          const probe = typeof event.attributes?.probe === "string" ? event.attributes.probe : "unknown";
          runtime1Delegate.health.set(probe, event.outcome ?? "unhealthy");
        }
      },
      collect(): string {
        const lines = [
          "# HELP nutsnews_worker_inflight Worker in-flight deliveries by bounded queue and outcome.",
          "# TYPE nutsnews_worker_inflight gauge",
          'nutsnews_worker_inflight{environment="production",service="persistence",queue="nutsnews.worker.persistence.v1",outcome="in_flight"} 0'
        ];

        if (runtime1Delegate.health.size > 0) {
          lines.push(
            "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by probe and outcome.",
            "# TYPE nutsnews_worker_health_probe gauge"
          );

          for (const [probe, current] of runtime1Delegate.health) {
            for (const outcome of [
              "ok",
              "degraded",
              "unhealthy"
            ]) {
              lines.push(`nutsnews_worker_health_probe{environment="production",service="persistence",probe="${probe}",outcome="${outcome}"} ${outcome === current ? "1" : "0"}`);
            }
          }
        }

        return `${lines.join("\n")}\n`;
      },
      setInFlight(): void {
        // Runtime1-like delegate method used by the persistence service.
      },
      setShutdownDraining(): void {
        // Runtime1-like delegate method used by the persistence service.
      }
    })
  };
});

import { createPersistencePrometheusTelemetrySink } from "../src/telemetry.js";

describe("persistence Runtime1 health metric compatibility", () => {
  beforeEach(() => {
    runtime1Delegate.forwardedEventNames.length = 0;
    runtime1Delegate.health.clear();
  });

  it("keeps one Runtime-owned health-probe family and forwards health evaluations", async () => {
    const metrics = createPersistencePrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-persistence",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: "0123456789abcdef0123456789abcdef01234567",
        deployment: "shadow",
        adapter: "production"
      }
    });

    await metrics.emit({
      name: "runtime.message.started",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "persistence",
      queue: "nutsnews.worker.persistence.v1"
    });
    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness"
      }
    });

    const output = metrics.collect();
    const lines = output.split("\n");
    const healthSamples = lines.filter((line) => line.startsWith("nutsnews_worker_health_probe{"));
    const healthSeries = healthSamples.map((line) => line.slice(0, line.lastIndexOf(" ")));

    expect(runtime1Delegate.forwardedEventNames).toEqual([
      "runtime.health.evaluated",
      "runtime.health.evaluated",
      "runtime.health.evaluated",
      "runtime.message.started",
      "runtime.health.evaluated"
    ]);
    expect(lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_probe "))).toHaveLength(1);
    expect(lines.filter((line) => line === "# TYPE nutsnews_worker_health_probe gauge")).toHaveLength(1);
    expect(healthSamples).toHaveLength(9);
    expect(new Set(healthSeries).size).toBe(healthSamples.length);
    expect(lines).toContain('nutsnews_worker_health_probe{environment="production",service="persistence",probe="readiness",outcome="ok"} 1');
    expect(lines).toContain('nutsnews_worker_inflight{environment="production",service="persistence",queue="nutsnews.worker.persistence.v1",outcome="in_flight"} 0');
  });
});
