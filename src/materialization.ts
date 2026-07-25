import { createHash } from "node:crypto";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  runtimeNow,
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
  PersistenceBackendShadowAggregateCommand,
  PersistenceFinalMaterializationAudit,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalShadowAggregate,
  PersistenceQuarantineRecord,
  PersistenceStageResultReference,
  PersistenceTranslationStageResultReference
} from "./materialization-types.js";

const DEFAULT_REQUIRED_LANGUAGE_CODES = [
  "fr",
  "ja",
  "de-CH",
  "de",
  "el"
] as const;

type JsonRecord = Readonly<Record<string, unknown>>;

type ParseResult =
  | {
      readonly ok: true;
      readonly request: PersistenceFinalMaterializationRequest;
    }
  | {
      readonly ok: false;
      readonly reason: string;
      readonly diagnosticMetadata: Readonly<Record<string, string | number | boolean>>;
    };

export function createFinalShadowMaterializationHandler(dependencies: PersistenceDependencies): PersistenceWorkHandler {
  return new FinalShadowMaterializationHandler(dependencies);
}

export class FinalShadowMaterializationHandler implements PersistenceWorkHandler {
  readonly name = "final-shadow-materialization-handler";

  constructor(private readonly dependencies: PersistenceDependencies) {}

  async handle(context: RuntimeMessageContext, tools: PersistenceWorkTools): Promise<RuntimeHandlerResult> {
    const parsed = parseFinalMaterializationRequest(context);

    if (!parsed.ok) {
      await this.recordQuarantine(context, tools, parsed.reason, parsed.diagnosticMetadata);
      return {
        status: "terminal-failure",
        reason: parsed.reason
      };
    }

    const request = parsed.request;
    const inputs = await this.dependencies.stageViewReader.readFinalMaterializationInputs(request);
    const inputValidation = validateStageInputs(request, inputs);

    if (inputValidation !== undefined) {
      await this.recordQuarantine(context, tools, inputValidation.reason, inputValidation.diagnosticMetadata, request);
      return {
        status: "terminal-failure",
        reason: inputValidation.reason
      };
    }

    const aggregate = buildFinalShadowAggregate(request, inputs);
    const backendCommand = buildBackendShadowAggregateCommand(request, aggregate);
    const publicationReadinessCommand = buildPublicationReadinessCommand(context, request, inputs, aggregate);
    const audit = buildAuditRecord(request, inputs, aggregate, "recorded_success");
    const record: PersistenceFinalMaterializationRecord = {
      request,
      inputs,
      aggregate,
      audit,
      publicationReadinessCommand
    };

    const writeResult = await tools.withTransaction(async (transaction) => {
      const backendResult = await this.dependencies.backendApiClient.recordShadowAggregate(backendCommand);

      if (backendResult.status === "conflict" || backendResult.status === "stale") {
        await this.dependencies.finalShadowTransactions.recordQuarantine(transaction, createQuarantineRecord(
          context,
          backendResult.reason,
          request,
          {
            backendResult: backendResult.status
          }
        ));
        return {
          status: backendResult.status,
          reason: backendResult.reason
        } as const;
      }

      const result = await this.dependencies.finalShadowTransactions.recordFinalMaterialization(transaction, record);

      if (result.status === "conflict" || result.status === "stale") {
        await this.dependencies.finalShadowTransactions.recordQuarantine(transaction, createQuarantineRecord(
          context,
          result.reason,
          request,
          {
            finalShadowResult: result.status
          }
        ));
      }

      return result;
    });

    if (writeResult.status === "recorded") {
      await this.publishUnconfirmedReadiness(tools, writeResult.publicationReadinessCommand);
      return {
        status: "ok"
      };
    }

    if (writeResult.status === "duplicate") {
      await this.publishUnconfirmedReadiness(tools, writeResult.publicationReadinessCommand);
      return {
        status: "ok"
      };
    }

    return {
      status: "terminal-failure",
      reason: writeResult.reason
    };
  }

  private async recordQuarantine(
    context: RuntimeMessageContext,
    tools: PersistenceWorkTools,
    reason: string,
    diagnosticMetadata: Readonly<Record<string, string | number | boolean>>,
    request?: PersistenceFinalMaterializationRequest
  ): Promise<void> {
    await tools.withTransaction(async (transaction) => {
      await this.dependencies.finalShadowTransactions.recordQuarantine(transaction, createQuarantineRecord(
        context,
        reason,
        request,
        diagnosticMetadata
      ));
    });
  }

  private async publishUnconfirmedReadiness(
    tools: PersistenceWorkTools,
    command: PersistenceFinalMaterializationRecord["publicationReadinessCommand"]
  ): Promise<void> {
    if (await this.dependencies.brokerOutbox.hasReceipt(command)) {
      return;
    }

    const receipt = await tools.publish(command);
    await tools.recordOutbox(command, receipt);
  }
}

function parseFinalMaterializationRequest(context: RuntimeMessageContext): ParseResult {
  const payload = context.payload;
  const payloadDigest = sha256Digest({
    aggregate: context.envelope.aggregate,
    payload
  });
  const commandId = requiredString(payload.commandId);
  const pipelineRunId = requiredString(payload.pipelineRunId);
  const stageExecutionId = requiredString(payload.stageExecutionId);
  const sourceMessageId = requiredString(payload.sourceMessageId);
  const idempotencyKey = requiredString(payload.idempotencyKey);
  const traceparent = requiredString(payload.traceparent);
  const producedAt = requiredString(payload.producedAt);
  const commandKind = requiredString(payload.commandKind);
  const payloadBackendOperation = requiredString(payload.backendOperation);
  const writeMode = requiredString(payload.writeMode);
  const payloadProviderMode = requiredString(payload.providerMode);

  if (
    commandId === undefined ||
    pipelineRunId === undefined ||
    stageExecutionId === undefined ||
    sourceMessageId === undefined ||
    idempotencyKey === undefined ||
    traceparent === undefined ||
    producedAt === undefined
  ) {
    return parseFailure("invalid-final-materialization-command", payloadDigest, {
      missingRequiredCommandField: true
    });
  }

  if (idempotencyKey !== context.envelope.idempotencyKey) {
    return parseFailure("idempotency-key-mismatch", payloadDigest, {
      safeMetadataOnly: true
    });
  }

  if (commandKind !== "save_article" || payloadBackendOperation !== "save-accepted-articles-batch") {
    return parseFailure("unsupported-final-materialization-command", payloadDigest, {
      commandKind: commandKind ?? "missing",
      backendOperation: payloadBackendOperation ?? "missing"
    });
  }

  if (writeMode !== "upsert" || payloadProviderMode !== "backend_postgres_primary") {
    return parseFailure("unsupported-final-materialization-write-mode", payloadDigest, {
      writeMode: writeMode ?? "missing",
      providerMode: payloadProviderMode ?? "missing"
    });
  }

  const entityRefs = objectArray(payload.entityRefs);
  const entityRef = entityRefs[0];

  if (entityRefs.length !== 1 || entityRef === undefined) {
    return parseFailure("invalid-final-materialization-entity-refs", payloadDigest, {
      entityRefCount: entityRefs.length
    });
  }

  if (hasForbiddenStageBodyFields(entityRef)) {
    return parseFailure("payload-carries-stage-owned-body", payloadDigest, {
      safeMetadataOnly: true
    });
  }

  const articleId = requiredString(entityRef.articleId);
  const articleVersion = positiveInteger(entityRef.articleVersion);
  const materializationKind = requiredString(entityRef.materializationKind);
  const stageRefs = record(entityRef.stageResultRefs);

  if (articleId === undefined || articleVersion === undefined || materializationKind !== "final_shadow_article" || stageRefs === undefined) {
    return parseFailure("invalid-final-materialization-entity-ref", payloadDigest, {
      safeMetadataOnly: true
    });
  }

  if (context.envelope.aggregate.id !== articleId || context.envelope.aggregate.version !== articleVersion) {
    return parseFailure("envelope-aggregate-mismatch", payloadDigest, {
      envelopeVersion: context.envelope.aggregate.version,
      articleVersion
    });
  }

  const canonical = stageResultReference(stageRefs.canonical);
  const enrichment = stageResultReference(stageRefs.enrichment);
  const approval = stageResultReference(stageRefs.approval);
  const translations = objectArray(stageRefs.translations).map(translationStageResultReference);

  if (
    canonical === undefined ||
    enrichment === undefined ||
    approval === undefined ||
    translations.length === 0 ||
    translations.some((translation) => translation === undefined)
  ) {
    return parseFailure("invalid-final-materialization-stage-refs", payloadDigest, {
      translationRefCount: translations.length
    });
  }

  const requiredLanguageCodes = stringArray(entityRef.requiredLanguageCodes);
  const request = {
    commandId,
    idempotencyKey,
    messageId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    pipelineRunId,
    stageExecutionId,
    sourceMessageId,
    traceparent,
    producedAt,
    articleId,
    articleVersion,
    writeMode: "upsert",
    payloadProviderMode: "backend_postgres_primary",
    backendProviderMode: "backend_postgres_shadow",
    payloadBackendOperation: "save-accepted-articles-batch",
    backendOperation: "uplift-record-shadow-aggregate",
    stageRefs: {
      canonical,
      enrichment,
      approval,
      translations: translations.filter((translation): translation is PersistenceTranslationStageResultReference => translation !== undefined)
    },
    requiredLanguageCodes: requiredLanguageCodes.length > 0 ? requiredLanguageCodes : DEFAULT_REQUIRED_LANGUAGE_CODES,
    payloadDigest
  } satisfies PersistenceFinalMaterializationRequest;

  return {
    ok: true,
    request
  };
}

function validateStageInputs(
  request: PersistenceFinalMaterializationRequest,
  inputs: PersistenceFinalMaterializationInputs
): { readonly reason: string; readonly diagnosticMetadata: Readonly<Record<string, string | number | boolean>> } | undefined {
  const stageResults = [
    inputs.canonical,
    inputs.enrichment,
    inputs.approval,
    ...inputs.translations
  ];
  const mismatchedArticle = stageResults.some((result) => result.articleId !== request.articleId || result.articleVersion !== request.articleVersion);

  if (mismatchedArticle) {
    return {
      reason: "inconsistent-stage-references",
      diagnosticMetadata: {
        safeMetadataOnly: true,
        articleVersion: request.articleVersion
      }
    };
  }

  if (
    inputs.canonical.resultVersion !== request.stageRefs.canonical.version ||
    inputs.enrichment.resultVersion !== request.stageRefs.enrichment.version ||
    inputs.approval.approvalVersion !== request.stageRefs.approval.version
  ) {
    return {
      reason: "stale-stage-version",
      diagnosticMetadata: {
        safeMetadataOnly: true,
        canonicalVersion: inputs.canonical.resultVersion,
        enrichmentVersion: inputs.enrichment.resultVersion,
        approvalVersion: inputs.approval.approvalVersion
      }
    };
  }

  const seenLanguages = new Set<string>();

  for (const translationRef of request.stageRefs.translations) {
    const result = inputs.translations.find((candidate) => candidate.languageCode === translationRef.languageCode);

    if (result?.translationVersion !== translationRef.version || seenLanguages.has(translationRef.languageCode)) {
      return {
        reason: "stale-stage-version",
        diagnosticMetadata: {
          safeMetadataOnly: true,
          translationLanguage: translationRef.languageCode,
          translationVersion: translationRef.version
        }
      };
    }

    seenLanguages.add(translationRef.languageCode);
  }

  return undefined;
}

function buildFinalShadowAggregate(
  request: PersistenceFinalMaterializationRequest,
  inputs: PersistenceFinalMaterializationInputs
): PersistenceFinalShadowAggregate {
  const acceptedLanguages = uniqueStrings(inputs.translations
    .filter((translation) => translation.qualityStatus === "accepted")
    .map((translation) => translation.languageCode));
  const missingLanguages = request.requiredLanguageCodes.filter((languageCode) => !acceptedLanguages.includes(languageCode));
  const approved = inputs.approval.decision === "approved";
  const publicationStatus: PersistenceFinalShadowAggregate["publicationStatus"] = approved && missingLanguages.length === 0 ? "ready" : "blocked";
  const payloadRef = `backend://worker-uplift/final-shadow/${inputs.canonical.articleIdentityHash}/v${String(request.articleVersion)}`;
  const diagnosticMetadata = {
    safeMetadataOnly: true,
    approved,
    requiredLanguageCount: request.requiredLanguageCodes.length,
    acceptedLanguageCount: acceptedLanguages.length,
    missingLanguageCount: missingLanguages.length,
    translationResultCount: inputs.translations.length
  };
  const baseAggregate = {
    articleIdentityHash: inputs.canonical.articleIdentityHash,
    canonicalUrlHash: inputs.canonical.canonicalUrlHash,
    originalUrlHash: inputs.canonical.originalUrlHash,
    aggregateVersion: request.articleVersion,
    titleRef: inputs.enrichment.titleRef,
    imageUrlRef: inputs.enrichment.imageUrlRef,
    approvalVersion: inputs.approval.approvalVersion,
    translationLanguages: acceptedLanguages,
    publicationStatus,
    payloadRef,
    diagnosticMetadata,
    ...(inputs.canonical.sourceFeedUrl === undefined ? {} : {
      sourceFeedUrl: inputs.canonical.sourceFeedUrl
    }),
    ...(inputs.enrichment.category === undefined ? {} : {
      category: inputs.enrichment.category
    }),
    ...(inputs.approval.positivityScore === undefined ? {} : {
      positivityScore: inputs.approval.positivityScore
    })
  };

  return {
    ...baseAggregate,
    payloadDigest: sha256Digest(baseAggregate)
  };
}

function buildBackendShadowAggregateCommand(
  request: PersistenceFinalMaterializationRequest,
  aggregate: PersistenceFinalShadowAggregate
): PersistenceBackendShadowAggregateCommand {
  return {
    operation: "uplift-record-shadow-aggregate",
    providerMode: "backend_postgres_shadow",
    idempotencyKey: request.idempotencyKey,
    messageId: request.messageId,
    correlationId: request.correlationId,
    pipelineRunId: request.pipelineRunId,
    stageExecutionId: request.stageExecutionId,
    sourceMessageId: request.sourceMessageId,
    actorService: "worker-uplift-persistence",
    schemaVersion: 1,
    operationVersion: aggregate.aggregateVersion,
    expectedArticleVersion: request.articleVersion,
    shadowAggregate: aggregate
  };
}

function buildAuditRecord(
  request: PersistenceFinalMaterializationRequest,
  inputs: PersistenceFinalMaterializationInputs,
  aggregate: PersistenceFinalShadowAggregate,
  status: PersistenceFinalMaterializationAudit["status"]
): PersistenceFinalMaterializationAudit {
  return {
    idempotencyKey: request.idempotencyKey,
    commandId: request.commandId,
    messageId: request.messageId,
    correlationId: request.correlationId,
    pipelineRunId: request.pipelineRunId,
    stageExecutionId: request.stageExecutionId,
    sourceMessageId: request.sourceMessageId,
    traceparent: request.traceparent,
    articleId: request.articleId,
    articleVersion: request.articleVersion,
    aggregateVersion: aggregate.aggregateVersion,
    canonicalVersion: inputs.canonical.resultVersion,
    enrichmentVersion: inputs.enrichment.resultVersion,
    approvalVersion: inputs.approval.approvalVersion,
    translationVersions: Object.fromEntries(inputs.translations.map((translation) => [
      translation.languageCode,
      translation.translationVersion
    ])),
    status,
    payloadDigest: aggregate.payloadDigest
  };
}

function buildPublicationReadinessCommand(
  context: RuntimeMessageContext,
  request: PersistenceFinalMaterializationRequest,
  inputs: PersistenceFinalMaterializationInputs,
  aggregate: PersistenceFinalShadowAggregate
) {
  const route = getWorkerRoute("publication");
  const occurredAt = runtimeNow({ now: () => new Date(request.producedAt) });
  const availableLanguageCodes = uniqueStrings(inputs.translations
    .filter((translation) => translation.qualityStatus === "accepted")
    .map((translation) => translation.languageCode));
  const missingLanguageCodes = request.requiredLanguageCodes.filter((languageCode) => !availableLanguageCodes.includes(languageCode));
  const readinessStatus = aggregate.publicationStatus === "ready" ? "ready" : "blocked_missing_translations";
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.publicationReadiness,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: request.pipelineRunId,
    stageExecutionId: deterministicUuid(`${request.commandId}:publication-readiness-stage`),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey: `persistence:publication:${request.articleId}:v${String(request.articleVersion)}:${request.commandId}`,
    traceparent: request.traceparent,
    producedAt: occurredAt,
    articleId: request.articleId,
    readinessStatus,
    requiredLanguageCodes: request.requiredLanguageCodes,
    availableLanguageCodes,
    missingLanguageCodes,
    snapshotRefreshRequired: aggregate.publicationStatus === "ready",
    publicationRef: {
      kind: "backend-record",
      uri: aggregate.payloadRef,
      mediaType: "application/json",
      aggregateVersion: aggregate.aggregateVersion,
      payloadDigest: aggregate.payloadDigest
    }
  };
  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "publication",
    messageId: deterministicUuid(`${request.commandId}:publication-readiness-message`),
    causationId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    idempotencyKey: payload.idempotencyKey,
    aggregate: {
      type: "article",
      id: request.articleId,
      version: request.articleVersion
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: "persistence",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: `${aggregate.payloadRef}/publication-readiness`,
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function createQuarantineRecord(
  context: RuntimeMessageContext,
  reason: string,
  request: PersistenceFinalMaterializationRequest | undefined,
  diagnosticMetadata: Readonly<Record<string, string | number | boolean>>
): PersistenceQuarantineRecord {
  const pipelineRunId = request?.pipelineRunId ?? optionalString(context.payload.pipelineRunId);
  const stageExecutionId = request?.stageExecutionId ?? optionalString(context.payload.stageExecutionId);
  const sourceMessageId = request?.sourceMessageId ?? optionalString(context.payload.sourceMessageId);
  return {
    idempotencyKey: context.envelope.idempotencyKey,
    messageId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    reason,
    diagnosticMetadata,
    ...(pipelineRunId === undefined ? {} : {
      pipelineRunId
    }),
    ...(stageExecutionId === undefined ? {} : {
      stageExecutionId
    }),
    ...(sourceMessageId === undefined ? {} : {
      sourceMessageId
    }),
    articleId: request?.articleId ?? context.envelope.aggregate.id,
    articleVersion: request?.articleVersion ?? context.envelope.aggregate.version,
    ...(request?.payloadDigest === undefined ? {} : {
      payloadDigest: request.payloadDigest
    })
  };
}

function parseFailure(
  reason: string,
  payloadDigest: string,
  diagnosticMetadata: Readonly<Record<string, string | number | boolean>>
): ParseResult {
  return {
    ok: false,
    reason,
    diagnosticMetadata: {
      ...diagnosticMetadata,
      payloadDigest,
      safeMetadataOnly: true
    }
  };
}

function requiredString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function positiveInteger(value: unknown): number | undefined {
  return Number.isInteger(value) && typeof value === "number" && value > 0 ? value : undefined;
}

function record(value: unknown): JsonRecord | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord
    : undefined;
}

function objectArray(value: unknown): readonly JsonRecord[] {
  return Array.isArray(value)
    ? value.filter((item): item is JsonRecord => record(item) !== undefined)
    : [];
}

function stringArray(value: unknown): readonly string[] {
  return Array.isArray(value)
    ? uniqueStrings(value.filter((item): item is string => typeof item === "string" && item.length > 0))
    : [];
}

function stageResultReference(value: unknown): PersistenceStageResultReference | undefined {
  const source = record(value);

  if (source === undefined) {
    return undefined;
  }

  const uri = requiredString(source.uri);
  const version = positiveInteger(source.version);

  if (uri === undefined || version === undefined) {
    return undefined;
  }

  return {
    uri,
    version,
    ...(typeof source.digest === "string" && source.digest.length > 0 ? {
      digest: source.digest
    } : {})
  };
}

function translationStageResultReference(value: unknown): PersistenceTranslationStageResultReference | undefined {
  const source = record(value);
  const reference = stageResultReference(value);
  const languageCode = source === undefined ? undefined : requiredString(source.languageCode);

  if (reference === undefined || languageCode === undefined) {
    return undefined;
  }

  return {
    ...reference,
    languageCode
  };
}

function hasForbiddenStageBodyFields(value: JsonRecord): boolean {
  const forbiddenFields = new Set([
    "articleBody",
    "canonicalBody",
    "enrichmentBody",
    "approvalBody",
    "translationBody",
    "summaryText",
    "modelResponse"
  ]);

  return Object.keys(value).some((key) => forbiddenFields.has(key));
}

function uniqueStrings(values: readonly string[]): readonly string[] {
  return Array.from(new Set(values));
}

function deterministicUuid(seed: string): string {
  const hash = createHash("sha256").update(seed).digest("hex");
  return [
    hash.slice(0, 8),
    hash.slice(8, 12),
    `7${hash.slice(13, 16)}`,
    `8${hash.slice(17, 20)}`,
    hash.slice(20, 32)
  ].join("-");
}
