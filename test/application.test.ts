import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import { createPersistenceApplication } from "../src/index.js";
import {
  LocalBrokerTransport,
  createLocalPersistenceDependencies
} from "../src/test-doubles.js";

describe("createPersistenceApplication", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("serves health and metrics while broker initialization is pending, then becomes ready", async () => {
    const config = applicationConfig();
    const dependencies = createLocalPersistenceDependencies();
    const transport = dependencies.brokerTransport as LocalBrokerTransport;
    const originalConnect = transport.connect.bind(transport);
    const connectEntered = deferredSignal();
    const connectRelease = deferredSignal();
    vi.spyOn(transport, "connect").mockImplementation(async () => {
      connectEntered.resolve();
      await connectRelease.promise;
      await originalConnect();
    });
    const application = createPersistenceApplication(config, {
      dependencies
    });
    const starting = application.start();

    await connectEntered.promise;

    try {
      const live = await fetch(application.url("/live"));
      const startup = await fetch(application.url("/startup"));
      const ready = await fetch(application.url("/ready"));
      const metrics = await fetch(application.url("/metrics"));

      expect(live.status).toBe(200);
      expect(await live.json()).toMatchObject({
        status: "ok"
      });
      expect(startup.status).toBe(503);
      expect(await startup.json()).toMatchObject({
        status: "unhealthy"
      });
      expect(ready.status).toBe(503);
      expect(await ready.json()).toMatchObject({
        status: "unhealthy"
      });
      expect(metrics.status).toBe(200);
      expect(await metrics.text()).toContain("nutsnews_worker_health_probe");

      connectRelease.resolve();
      await starting;

      expect((await fetch(application.url("/startup"))).status).toBe(200);
      expect((await fetch(application.url("/ready"))).status).toBe(200);
    } finally {
      connectRelease.resolve();
      await starting.catch(() => undefined);
      await application.stop();
    }
  });

  it("closes the listener and removes shutdown handlers when initialization rejects", async () => {
    const config = applicationConfig();
    const dependencies = createLocalPersistenceDependencies();
    const transport = dependencies.brokerTransport as LocalBrokerTransport;
    const startupError = new Error("broker initialization failed");
    const signalListenerCounts = {
      SIGINT: process.listenerCount("SIGINT"),
      SIGTERM: process.listenerCount("SIGTERM")
    };
    vi.spyOn(transport, "connect").mockRejectedValue(startupError);
    const application = createPersistenceApplication(config, {
      dependencies
    });

    await expect(application.start()).rejects.toBe(startupError);

    expect(() => application.url("/live")).toThrow("not listening");
    expect(process.listenerCount("SIGINT")).toBe(signalListenerCounts.SIGINT);
    expect(process.listenerCount("SIGTERM")).toBe(signalListenerCounts.SIGTERM);
  });

  it("rejects injected in-memory adapters for a production configuration", () => {
    const config = loadPersistenceConfig({
      HOSTNAME: "persistence-production-adapter-test",
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE: "production",
      NUTSNEWS_PERSISTENCE_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_PERSISTENCE_DATABASE_URL: "postgres://example.invalid/persistence",
      NUTSNEWS_PERSISTENCE_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
      NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN: "test-placeholder",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });

    expect(() => createPersistenceApplication(config, {
      dependencies: createLocalPersistenceDependencies()
    })).toThrow(
      "production requires adapter=production and stateStore=postgresql"
    );
  });
});

function applicationConfig() {
  return loadPersistenceConfig({
    HOSTNAME: "persistence-application-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_PERSISTENCE_HTTP_HOST: "127.0.0.1",
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
}

function deferredSignal() {
  let resolvePromise!: () => void;
  let rejectPromise!: (reason?: unknown) => void;
  const promise = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });

  return {
    promise,
    resolve: resolvePromise,
    reject: rejectPromise
  };
}
