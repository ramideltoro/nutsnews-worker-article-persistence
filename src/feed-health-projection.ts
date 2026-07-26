import {
  type RuntimeHandlerResult,
  type RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  PersistenceDependencies,
  PersistenceWorkHandler,
  PersistenceWorkTools
} from "./dependencies.js";
import { sha256Digest } from "./digest.js";
import type {
  FeedHealthOutcomeKind,
  FeedHealthOutcomeStage,
  FeedHealthOutcomeStatus,
  FeedHealthProjectionCounts,
  FeedHealthProjectionEvent
} from "./feed-health-types.js";
import {
  createFinalShadowMaterializationHandler,
  isTranslationSummaryPersistenceCommandPayload
} from "./materialization.js";

type JsonRecord = Readonly<Record<string, unknown>>;

type ProjectionParseResult =
  | {
      readonly ok: true;
      readonly event: FeedHealthProjectionEvent;
    }
  | {
      readonly ok: false;
      readonly reason: string;
    };

const OUTCOME_STAGES = new Set<FeedHealthOutcomeStage>([
  "fetcher",
  "canonicalizer",
  "enrichment",
  "approval",
  "translation",
  "persistence",
  "terminal"
]);
const OUTCOME_KINDS = new Set<FeedHealthOutcomeKind>([
  "fetch_attempt",
  "candidate_batch",
  "duplicate_batch",
  "image_enrichment",
  "approval_batch",
  "translation_batch",
  "persistence_write",
  "terminal_failure"
]);
const OUTCOME_STATUSES = new Set<FeedHealthOutcomeStatus>([
  "success",
  "failure",
  "partial",
  "duplicate"
]);

export function createPersistenceRoutingWorkHandler(dependencies: PersistenceDependencies): PersistenceWorkHandler {
  return new PersistenceRoutingWorkHandler(
    createFinalShadowMaterializationHandler(dependencies),
    createFeedHealthProjectionHandler(dependencies)
  );
}

export function createFeedHealthProjectionHandler(dependencies: PersistenceDependencies): PersistenceWorkHandler {
  return new FeedHealthProjectionHandler(dependencies);
}

export class PersistenceRoutingWorkHandler implements PersistenceWorkHandler {
  readonly name = "persistence-routing-work-handler";

  constructor(
    private readonly finalMaterializationHandler: PersistenceWorkHandler,
    private readonly feedHealthProjectionHandler: PersistenceWorkHandler
  ) {}

  handle(context: RuntimeMessageContext, tools: PersistenceWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult> {
    if (isFeedHealthProjectionPayload(context.payload)) {
      return this.feedHealthProjectionHandler.handle(context, tools);
    }

    if (isTranslationSummaryPersistenceCommandPayload(context.payload)) {
      return {
        status: "ok"
      };
    }

    return this.finalMaterializationHandler.handle(context, tools);
  }
}

export class FeedHealthProjectionHandler implements PersistenceWorkHandler {
  readonly name = "feed-health-projection-handler";

  constructor(private readonly dependencies: PersistenceDependencies) {}

  async handle(context: RuntimeMessageContext): Promise<RuntimeHandlerResult> {
    const parsed = parseFeedHealthProjectionEvent(context);

    if (!parsed.ok) {
      return {
        status: "terminal-failure",
        reason: parsed.reason
      };
    }

    const result = await this.dependencies.feedHealthProjectionStore.project(parsed.event);

    if (result.status === "conflict") {
      return {
        status: "terminal-failure",
        reason: result.reason
      };
    }

    return {
      status: "ok"
    };
  }
}

export function isFeedHealthProjectionPayload(payload: Readonly<Record<string, unknown>>): boolean {
  const entityRef = objectArray(payload.entityRefs)[0];
  return entityRef?.projectionKind === "feed_health";
}

function parseFeedHealthProjectionEvent(context: RuntimeMessageContext): ProjectionParseResult {
  const payload = context.payload;
  const entityRef = objectArray(payload.entityRefs)[0];
  const payloadIdempotencyKey = requiredString(payload.idempotencyKey);
  const payloadDigest = sha256Digest({
    aggregate: context.envelope.aggregate,
    payload
  });

  if (entityRef?.projectionKind !== "feed_health") {
    return {
      ok: false,
      reason: "invalid-feed-health-projection-ref"
    };
  }

  if (payloadIdempotencyKey !== context.envelope.idempotencyKey) {
    return {
      ok: false,
      reason: "idempotency-key-mismatch"
    };
  }

  if (hasForbiddenRawFields(entityRef)) {
    return {
      ok: false,
      reason: "projection-payload-carries-raw-content"
    };
  }

  const projectionId = requiredString(entityRef.projectionId);
  const feedKey = requiredString(entityRef.feedKey);
  const feedUrl = requiredString(entityRef.feedUrl);
  const source = requiredString(entityRef.source);
  const outcomeStage = enumValue(entityRef.outcomeStage, OUTCOME_STAGES);
  const outcomeKind = enumValue(entityRef.outcomeKind, OUTCOME_KINDS);
  const outcomeStatus = enumValue(entityRef.outcomeStatus, OUTCOME_STATUSES);
  const eventVersion = positiveInteger(entityRef.eventVersion);
  const occurredAt = requiredString(entityRef.occurredAt);
  const latencyMs = nonNegativeInteger(entityRef.latencyMs);
  const counts = projectionCounts(entityRef.counts);
  const backoffRecommendation = enumValue(entityRef.backoffRecommendation, new Set([
    "none",
    "retry",
    "slow_down",
    "disable_candidate"
  ] as const));

  if (
    projectionId === undefined ||
    feedKey === undefined ||
    feedUrl === undefined ||
    source === undefined ||
    outcomeStage === undefined ||
    outcomeKind === undefined ||
    outcomeStatus === undefined ||
    eventVersion === undefined ||
    occurredAt === undefined ||
    latencyMs === undefined ||
    counts === undefined ||
    backoffRecommendation === undefined
  ) {
    return {
      ok: false,
      reason: "invalid-feed-health-projection-event"
    };
  }

  return {
    ok: true,
    event: {
      projectionId,
      idempotencyKey: context.envelope.idempotencyKey,
      pipelineRunId: stringField(payload.pipelineRunId),
      stageExecutionId: stringField(payload.stageExecutionId),
      sourceMessageId: stringField(payload.sourceMessageId),
      messageId: context.envelope.messageId,
      correlationId: context.envelope.correlationId,
      feedKey,
      feedUrl,
      source,
      outcomeStage,
      outcomeKind,
      outcomeStatus,
      eventVersion,
      occurredAt,
      latencyMs,
      counts,
      ...(typeof entityRef.errorClass === "string" && entityRef.errorClass.length > 0 ? {
        errorClass: sanitizeErrorClass(entityRef.errorClass)
      } : {}),
      backoffRecommendation,
      payloadDigest
    }
  };
}

function objectArray(value: unknown): readonly JsonRecord[] {
  return Array.isArray(value)
    ? value.filter((item): item is JsonRecord => item !== null && typeof item === "object" && !Array.isArray(item))
    : [];
}

function requiredString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function stringField(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function positiveInteger(value: unknown): number | undefined {
  return Number.isInteger(value) && typeof value === "number" && value > 0 ? value : undefined;
}

function nonNegativeInteger(value: unknown): number | undefined {
  return Number.isInteger(value) && typeof value === "number" && value >= 0 ? value : undefined;
}

function enumValue<T extends string>(value: unknown, allowed: ReadonlySet<T>): T | undefined {
  return typeof value === "string" && allowed.has(value as T) ? value as T : undefined;
}

function projectionCounts(value: unknown): FeedHealthProjectionCounts | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  const source = value as JsonRecord;
  const counts = {
    itemCount: nonNegativeInteger(source.itemCount),
    duplicateCount: nonNegativeInteger(source.duplicateCount),
    imageCount: nonNegativeInteger(source.imageCount),
    eligibleCount: nonNegativeInteger(source.eligibleCount),
    rejectedCount: nonNegativeInteger(source.rejectedCount),
    acceptedCount: nonNegativeInteger(source.acceptedCount)
  };

  if (Object.values(counts).some((count) => count === undefined)) {
    return undefined;
  }

  return counts as FeedHealthProjectionCounts;
}

function hasForbiddenRawFields(value: JsonRecord): boolean {
  return [
    "rawFeedBody",
    "articleBody",
    "modelOutput",
    "credential",
    "secret"
  ].some((field) => Object.hasOwn(value, field));
}

function sanitizeErrorClass(value: string): string {
  return value.replace(/[^A-Za-z0-9_.:-]/g, "_").slice(0, 96);
}
