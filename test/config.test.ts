import {
  describe,
  expect,
  it
} from "vitest";

import {
  PersistenceConfigError,
  loadPersistenceConfig
} from "../src/config.js";

describe("loadPersistenceConfig", () => {
  it("loads value-free local defaults", () => {
    const config = loadPersistenceConfig({
      HOSTNAME: "persistence-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-article-persistence",
      dependencyMode: "test",
      buildRevision: "development",
      host: "persistence-host",
      concurrency: 2,
      prefetch: 4,
      compatibility: {
        backendApiVersion: "worker-api-v1",
        shadowSchemaVersion: "worker-uplift-shadow-v1"
      },
      security: {
        databaseRole: "nutsnews_worker_persistence",
        backendApiIdentity: "worker-uplift-persistence",
        productionWritesEnabled: false
      },
      shadowMode: true,
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false,
        backendApiConfigured: false,
        backendApiCredentialConfigured: false
      }
    });
  });

  it("fails production config by missing secret names only", () => {
    expect(() => loadPersistenceConfig({
      NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE: "production"
    })).toThrow(PersistenceConfigError);

    try {
      loadPersistenceConfig({
        NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE: "production"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(PersistenceConfigError);
      const configError = error as PersistenceConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_PERSISTENCE_DATABASE_URL is required when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PERSISTENCE_RABBITMQ_URL is required when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL is required when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN is required when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production.",
        "NUTSNEWS_PERSISTENCE_BUILD_REVISION must be a lowercase 40-character Git commit SHA when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
      expect(configError.message).not.toContain("https://");
      expect(configError.message).not.toContain("secret");
    }
  });

  it("rejects unsafe bounds and production domain writes", () => {
    expect(() => loadPersistenceConfig({
      NUTSNEWS_PERSISTENCE_CONCURRENCY: "8",
      NUTSNEWS_PERSISTENCE_PREFETCH: "2",
      NUTSNEWS_PERSISTENCE_SHADOW_MODE: "false",
      NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED: "true"
    })).toThrow(PersistenceConfigError);
  });

  it("accepts production dependency presence without retaining values", () => {
    const config = loadPersistenceConfig({
      NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE: "production",
      NUTSNEWS_PERSISTENCE_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_PERSISTENCE_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_PERSISTENCE_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL: "https://backend.example.invalid/worker",
      NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN: "secret-not-real",
      NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
    });

    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true,
      backendApiConfigured: true,
      backendApiCredentialConfigured: true
    });
    expect(config.buildRevision).toBe("0123456789abcdef0123456789abcdef01234567");
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
    expect(JSON.stringify(config)).not.toContain("backend.example.invalid");
    expect(JSON.stringify(config)).not.toContain("secret-not-real");
  });
});
