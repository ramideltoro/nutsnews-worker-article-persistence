import os from "node:os";

export const PERSISTENCE_SERVICE_NAME = "nutsnews-worker-article-persistence" as const;
export const PERSISTENCE_SERVICE_VERSION = "0.1.0" as const;
export const PERSISTENCE_PRODUCTION_WRITE_CONFIRMATION = "backend-protected-persistence-cutover-approved" as const;

export type PersistenceDependencyMode = "test" | "production";
export type PersistenceTelemetryLogMode = "stdout" | "silent";

export interface PersistenceConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const PERSISTENCE_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_PERSISTENCE_BUILD_REVISION", "Immutable lowercase 40-character Git commit revision baked into the production image.", true, false, "development"),
  variable("NUTSNEWS_PERSISTENCE_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_PERSISTENCE_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_PERSISTENCE_DATABASE_URL", "Final worker-uplift shadow database connection string.", true, true),
  variable("NUTSNEWS_PERSISTENCE_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL", "Scoped backend Worker API base URL.", true, true),
  variable("NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN", "Credential for the scoped backend Worker API identity.", true, true),
  variable("NUTSNEWS_PERSISTENCE_BACKEND_API_COMPATIBILITY_VERSION", "Expected backend Worker API compatibility version.", false, false, "worker-api-v1"),
  variable("NUTSNEWS_PERSISTENCE_SHADOW_SCHEMA_VERSION", "Expected final shadow data model compatibility version.", false, false, "worker-uplift-shadow-v1"),
  variable("NUTSNEWS_PERSISTENCE_DATABASE_ROLE", "Dedicated database role used by the persistence service.", false, false, "nutsnews_worker_persistence"),
  variable("NUTSNEWS_PERSISTENCE_BACKEND_API_IDENTITY", "Scoped backend API identity used for future domain commands.", false, false, "worker-uplift-persistence"),
  variable("NUTSNEWS_PERSISTENCE_CONCURRENCY", "Maximum concurrent persistence message handlers.", false, false, "2"),
  variable("NUTSNEWS_PERSISTENCE_PREFETCH", "Broker prefetch bound for persistence deliveries.", false, false, "4"),
  variable("NUTSNEWS_PERSISTENCE_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_PERSISTENCE_SHADOW_MODE", "Keep persistence output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED", "Production domain writes must remain disabled until cutover.", false, false, "false"),
  variable("NUTSNEWS_PERSISTENCE_PRODUCTION_WRITE_CONFIRMATION", "Fixed confirmation supplied only by the protected backend cutover controller.", false, false),
  variable("NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_PERSISTENCE_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly PersistenceConfigVariable[];

export interface PersistenceConfig {
  readonly serviceName: typeof PERSISTENCE_SERVICE_NAME;
  readonly serviceVersion: typeof PERSISTENCE_SERVICE_VERSION;
  readonly environment: string;
  readonly buildRevision: string;
  readonly host: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: PersistenceDependencyMode;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
    readonly backendApiConfigured: boolean;
    readonly backendApiCredentialConfigured: boolean;
  };
  readonly compatibility: {
    readonly backendApiVersion: string;
    readonly shadowSchemaVersion: string;
  };
  readonly security: {
    readonly databaseRole: string;
    readonly backendApiIdentity: string;
    readonly productionWritesEnabled: boolean;
    readonly productionWriteConfirmationValid: boolean;
  };
  readonly concurrency: number;
  readonly prefetch: number;
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: PersistenceTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class PersistenceConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid persistence configuration: ${issues.join("; ")}`);
    this.name = "PersistenceConfigError";
    this.issues = issues;
  }
}

export function loadPersistenceConfig(env: NodeJS.ProcessEnv = process.env): PersistenceConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE, issues);
  const environment = nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local");
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_PERSISTENCE_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_PERSISTENCE_RABBITMQ_URL),
    backendApiConfigured: hasValue(env.NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL),
    backendApiCredentialConfigured: hasValue(env.NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_PERSISTENCE_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_PERSISTENCE_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
    requireConfigured("NUTSNEWS_PERSISTENCE_BACKEND_API_BASE_URL", dependencies.backendApiConfigured, issues);
    requireConfigured("NUTSNEWS_PERSISTENCE_BACKEND_API_TOKEN", dependencies.backendApiCredentialConfigured, issues);
  }

  if (environment.toLowerCase() === "production" && dependencyMode !== "production") {
    issues.push("NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE must be production when NUTSNEWS_ENVIRONMENT=production so the PostgreSQL state store and external adapters cannot run in test mode.");
  }

  const buildRevision = parseBuildRevision(env.NUTSNEWS_PERSISTENCE_BUILD_REVISION, dependencyMode, issues);

  const concurrency = parseInteger(env.NUTSNEWS_PERSISTENCE_CONCURRENCY, "NUTSNEWS_PERSISTENCE_CONCURRENCY", 2, 1, 16, issues);
  const prefetch = parseInteger(env.NUTSNEWS_PERSISTENCE_PREFETCH, "NUTSNEWS_PERSISTENCE_PREFETCH", 4, 1, 64, issues);
  const config: PersistenceConfig = {
    serviceName: PERSISTENCE_SERVICE_NAME,
    serviceVersion: PERSISTENCE_SERVICE_VERSION,
    environment,
    buildRevision,
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_PERSISTENCE_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_PERSISTENCE_HTTP_PORT, "NUTSNEWS_PERSISTENCE_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    compatibility: {
      backendApiVersion: nonEmpty(env.NUTSNEWS_PERSISTENCE_BACKEND_API_COMPATIBILITY_VERSION, "worker-api-v1"),
      shadowSchemaVersion: nonEmpty(env.NUTSNEWS_PERSISTENCE_SHADOW_SCHEMA_VERSION, "worker-uplift-shadow-v1")
    },
    security: {
      databaseRole: nonEmpty(env.NUTSNEWS_PERSISTENCE_DATABASE_ROLE, "nutsnews_worker_persistence"),
      backendApiIdentity: nonEmpty(env.NUTSNEWS_PERSISTENCE_BACKEND_API_IDENTITY, "worker-uplift-persistence"),
      productionWritesEnabled: parseBoolean(env.NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED, "NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED", false, issues),
      productionWriteConfirmationValid: env.NUTSNEWS_PERSISTENCE_PRODUCTION_WRITE_CONFIRMATION === PERSISTENCE_PRODUCTION_WRITE_CONFIRMATION
    },
    concurrency,
    prefetch,
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_PERSISTENCE_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_PERSISTENCE_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode: parseBoolean(env.NUTSNEWS_PERSISTENCE_SHADOW_MODE, "NUTSNEWS_PERSISTENCE_SHADOW_MODE", true, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_PERSISTENCE_METRICS_ENABLED, "NUTSNEWS_PERSISTENCE_METRICS_ENABLED", true, issues)
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_PERSISTENCE_PREFETCH must be greater than or equal to NUTSNEWS_PERSISTENCE_CONCURRENCY.");
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_PERSISTENCE_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (config.security.productionWritesEnabled) {
    if (config.dependencyMode !== "production") {
      issues.push("NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED requires production dependencies.");
    }
    if (!config.security.productionWriteConfirmationValid) {
      issues.push("NUTSNEWS_PERSISTENCE_PRODUCTION_WRITES_ENABLED requires the fixed protected confirmation.");
    }
  } else if (env.NUTSNEWS_PERSISTENCE_PRODUCTION_WRITE_CONFIRMATION !== undefined) {
    issues.push("NUTSNEWS_PERSISTENCE_PRODUCTION_WRITE_CONFIRMATION must be absent while production writes are disabled.");
  }

  if (issues.length > 0) {
    throw new PersistenceConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): PersistenceConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseDependencyMode(value: string | undefined, issues: string[]): PersistenceDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseBuildRevision(
  value: string | undefined,
  dependencyMode: PersistenceDependencyMode,
  issues: string[]
): string {
  const revision = nonEmpty(value, "development");

  if (dependencyMode === "production" && !/^[0-9a-f]{40}$/u.test(revision)) {
    issues.push("NUTSNEWS_PERSISTENCE_BUILD_REVISION must be a lowercase 40-character Git commit SHA when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production.");
  }

  return revision;
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): PersistenceTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBoolean(value: string | undefined, key: string, fallback: boolean, issues: string[]): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(value: string | undefined, key: string, fallback: number, min: number, max: number, issues: string[]): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_PERSISTENCE_DEPENDENCY_MODE=production.`);
  }
}
