import {
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import {
  createPersistenceHttpServer,
  type PersistenceHttpServer
} from "../src/http.js";
import { createPersistenceService } from "../src/service.js";
import { createLocalPersistenceDependencies } from "../src/test-doubles.js";

describe("createPersistenceHttpServer", () => {
  let server: PersistenceHttpServer | undefined;
  let service: ReturnType<typeof createPersistenceService> | undefined;

  afterEach(async () => {
    await server?.close();
    await service?.stop();
  });

  it("serves health, metrics, and config schema without exposing values", async () => {
    const config = loadPersistenceConfig({
      NUTSNEWS_PERSISTENCE_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    service = createPersistenceService({
      config,
      dependencies: createLocalPersistenceDependencies(),
      metrics
    });
    server = createPersistenceHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await server.listen();

    const live = await fetch(server.url("/live"));
    const ready = await fetch(server.url("/ready"));
    const metricsResponse = await fetch(server.url("/metrics"));
    const schema = await fetch(server.url("/config-schema"));

    expect(live.status).toBe(200);
    expect(ready.status).toBe(200);
    expect(await metricsResponse.text()).toContain("nutsnews_worker_dependency_duration_ms");
    const schemaBody = await schema.json() as {
      readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[];
    };

    expect(schemaBody.variables.some((variable) => variable.name === "NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schemaBody)).not.toContain("postgres://");
    expect(JSON.stringify(schemaBody)).not.toContain("amqp://");
  });
});
