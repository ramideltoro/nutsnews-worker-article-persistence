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
  PersistenceProductionMaterialization,
  PersistenceFinalMaterializationAudit,
  PersistenceFinalMaterializationInputs,
  PersistenceFinalMaterializationRecord,
  PersistenceFinalMaterializationRequest,
  PersistenceFinalShadowAggregate,
  PersistenceQuarantineRecord,
  PersistenceStageResultReference,
  PersistenceSaveAcceptedArticleCommand,
  PersistenceSaveArticleSummariesCommand,
  PersistenceTranslationStageResultReference
} from "./materialization-types.js";

const DEFAULT_REQUIRED_LANGUAGE_CODES = [
  "fr",
  "ja",
  "de-CH",
  "de",
  "el"
] as const;
const BACKEND_CAPTURED_PUBLICATION_POLICY_VERSION = "2026-07-23.worker-uplift-api-admin-compatibility-contract.v1";

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
    tools.signal.throwIfAborted();
    const parsed = parseFinalMaterializationRequest(context);

    if (!parsed.ok) {
      await this.recordQuarantine(context, tools, parsed.reason, parsed.diagnosticMetadata);
      return {
        status: "terminal-failure",
        reason: parsed.reason
      };
    }

    const request = parsed.request;
    const inputs = await runPersistenceWorkOperation(
      tools,
      () => this.dependencies.stageViewReader.readFinalMaterializationInputs(request)
    );
    const inputValidation = validateStageInputs(request, inputs);

    if (inputValidation !== undefined) {
      await this.recordQuarantine(context, tools, inputValidation.reason, inputValidation.diagnosticMetadata, request);
      return {
        status: "terminal-failure",
        reason: inputValidation.reason
      };
    }

    const aggregate = buildFinalShadowAggregate(request, inputs);
    const productionMaterialization = this.dependencies.productionDomainWritesEnabled && aggregate.publicationStatus === "ready"
      ? buildProductionMaterialization(request, inputs)
      : undefined;
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
      const backendResult = await runPersistenceWorkOperation(
        tools,
        () => this.dependencies.backendApiClient.recordShadowAggregate(backendCommand)
      );

      if (backendResult.status === "conflict" || backendResult.status === "stale") {
        await runPersistenceWorkOperation(
          tools,
          () => this.dependencies.finalShadowTransactions.recordQuarantine(transaction, createQuarantineRecord(
            context,
            backendResult.reason,
            request,
            {
              backendResult: backendResult.status
            }
          ))
        );
        return {
          status: backendResult.status,
          reason: backendResult.reason
        } as const;
      }

      const result = await runPersistenceWorkOperation(
        tools,
        () => this.dependencies.finalShadowTransactions.recordFinalMaterialization(transaction, record)
      );

      if (result.status === "conflict" || result.status === "stale") {
        await runPersistenceWorkOperation(
          tools,
          () => this.dependencies.finalShadowTransactions.recordQuarantine(transaction, createQuarantineRecord(
            context,
            result.reason,
            request,
            {
              finalShadowResult: result.status
            }
          ))
        );
      }

      return result;
    });

    if (writeResult.status === "recorded") {
      await this.applyProductionMaterializationIfEnabled(tools, productionMaterialization);
      await this.publishUnconfirmedReadiness(tools, writeResult.publicationReadinessCommand);
      return {
        status: "ok"
      };
    }

    if (writeResult.status === "duplicate") {
      await this.applyProductionMaterializationIfEnabled(tools, productionMaterialization);
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

  private async applyProductionMaterializationIfEnabled(
    tools: PersistenceWorkTools,
    materialization: PersistenceProductionMaterialization | undefined
  ): Promise<void> {
    if (!this.dependencies.productionDomainWritesEnabled || materialization === undefined) {
      return;
    }
    await runPersistenceWorkOperation(
      tools,
      () => this.dependencies.backendApiClient.saveAcceptedArticle(materialization.saveArticleCommand)
    );
    await runPersistenceWorkOperation(
      tools,
      () => this.dependencies.backendApiClient.saveArticleSummaries(materialization.saveSummariesCommand)
    );
  }

  private async recordQuarantine(
    context: RuntimeMessageContext,
    tools: PersistenceWorkTools,
    reason: string,
    diagnosticMetadata: Readonly<Record<string, string | number | boolean>>,
    request?: PersistenceFinalMaterializationRequest
  ): Promise<void> {
    await tools.withTransaction(async (transaction) => {
      await runPersistenceWorkOperation(
        tools,
        () => this.dependencies.finalShadowTransactions.recordQuarantine(transaction, createQuarantineRecord(
          context,
          reason,
          request,
          diagnosticMetadata
        ))
      );
    });
  }

  private async publishUnconfirmedReadiness(
    tools: PersistenceWorkTools,
    command: PersistenceFinalMaterializationRecord["publicationReadinessCommand"]
  ): Promise<void> {
    if (await runPersistenceWorkOperation(tools, () => this.dependencies.brokerOutbox.hasReceipt(command))) {
      return;
    }

    const receipt = await tools.publish(command);
    await tools.recordOutbox(command, receipt);
  }
}

async function runPersistenceWorkOperation<T>(
  tools: PersistenceWorkTools,
  operation: () => T | Promise<T>
): Promise<T> {
  tools.signal.throwIfAborted();
  const value = await operation();
  tools.signal.throwIfAborted();
  return value;
}

function parseFinalMaterializationRequest(context: RuntimeMessageContext): ParseResult {
  const payload = context.payload;
  const payloadDigest = sha256Digest({
    aggregate: context.envelope.aggregate,
    payload
  });

  if (isTranslationResultPayload(payload)) {
    return parseTranslationResultMaterializationRequest(context, payload, payloadDigest);
  }

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

function parseTranslationResultMaterializationRequest(
  context: RuntimeMessageContext,
  payload: Readonly<Record<string, unknown>>,
  payloadDigest: string
): ParseResult {
  const pipelineRunId = requiredString(payload.pipelineRunId);
  const stageExecutionId = requiredString(payload.stageExecutionId);
  const sourceMessageId = requiredString(payload.sourceMessageId);
  const idempotencyKey = requiredString(payload.idempotencyKey);
  const traceparent = requiredString(payload.traceparent);
  const producedAt = requiredString(payload.producedAt);
  const articleId = requiredString(payload.articleId);
  const articleVersion = positiveInteger(context.envelope.aggregate.version);
  const completedLanguageCodes = stringArray(payload.completedLanguageCodes);
  const missingLanguageCodes = stringArray(payload.missingLanguageCodes);

  if (
    pipelineRunId === undefined ||
    stageExecutionId === undefined ||
    sourceMessageId === undefined ||
    idempotencyKey === undefined ||
    traceparent === undefined ||
    producedAt === undefined ||
    articleId === undefined ||
    articleVersion === undefined
  ) {
    return parseFailure("invalid-translation-result-materialization-command", payloadDigest, {
      missingRequiredCommandField: true
    });
  }

  if (idempotencyKey !== context.envelope.idempotencyKey) {
    return parseFailure("idempotency-key-mismatch", payloadDigest, {
      safeMetadataOnly: true
    });
  }

  if (context.envelope.aggregate.id !== articleId) {
    return parseFailure("envelope-aggregate-mismatch", payloadDigest, {
      envelopeVersion: context.envelope.aggregate.version,
      articleVersion
    });
  }

  if (completedLanguageCodes.length === 0) {
    return parseFailure("translation-result-has-no-completed-languages", payloadDigest, {
      safeMetadataOnly: true
    });
  }

  const summaryRefs = objectArray(payload.summaryRefs);
  const translations = completedLanguageCodes.map((languageCode) => {
    const summaryRef = summaryRefs.find((candidate) => requiredString(candidate.targetLanguage) === languageCode || requiredString(candidate.languageCode) === languageCode);
    const uri = requiredString(summaryRef?.uri) ?? `backend://worker-uplift/translation/${encodeURIComponent(articleId)}/v${String(articleVersion)}/${encodeURIComponent(languageCode)}/summary`;

    return {
      languageCode,
      uri,
      version: articleVersion,
      ...(typeof summaryRef?.digest === "string" && summaryRef.digest.length > 0 ? {
        digest: summaryRef.digest
      } : {})
    } satisfies PersistenceTranslationStageResultReference;
  });
  const requiredLanguageCodes = uniqueStrings([
    ...completedLanguageCodes,
    ...missingLanguageCodes
  ]);

  return {
    ok: true,
    request: {
      commandId: `translation-result:${articleId}:v${String(articleVersion)}`,
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
        canonical: stageReferenceFor(articleId, articleVersion, "canonical"),
        enrichment: stageReferenceFor(articleId, articleVersion, "enrichment"),
        approval: stageReferenceFor(articleId, articleVersion, "approval"),
        translations
      },
      requiredLanguageCodes: requiredLanguageCodes.length > 0 ? requiredLanguageCodes : DEFAULT_REQUIRED_LANGUAGE_CODES,
      payloadDigest
    }
  };
}

function stageReferenceFor(articleId: string, articleVersion: number, stage: "canonical" | "enrichment" | "approval"): PersistenceStageResultReference {
  return {
    uri: `backend://worker-uplift/${stage}/${encodeURIComponent(articleId)}/v${String(articleVersion)}`,
    version: articleVersion
  };
}

function isTranslationResultPayload(payload: Readonly<Record<string, unknown>>): boolean {
  return payload.schemaId === STAGE_PAYLOAD_SCHEMA_IDS.translationResult
    && typeof payload.articleId === "string"
    && Array.isArray(payload.completedLanguageCodes);
}

export function isTranslationSummaryPersistenceCommandPayload(payload: Readonly<Record<string, unknown>>): boolean {
  return payload.schemaId === STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand
    && payload.commandKind === "save_summaries"
    && payload.backendOperation === "save-article-summaries-batch";
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

  if (
    inputs.approval.decision === "approved"
    && (!isHttpUrl(inputs.approval.canonicalUrl)
      || inputs.approval.title.trim().length === 0
      || !isHttpUrl(inputs.approval.imageUrl ?? "")
      || inputs.approval.category.trim().length === 0
      || inputs.approval.sourceSummary.trim().length === 0
      || inputs.approval.sourceLanguage.trim().length === 0
      || inputs.approval.model.trim().length === 0)
  ) {
    return {
      reason: "incomplete-approved-publication-material",
      diagnosticMetadata: {
        safeMetadataOnly: true,
        articleVersion: request.articleVersion
      }
    };
  }

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

  if (inputs.translations.some((translation) => (
    translation.qualityStatus === "accepted"
    && (translation.title.trim().length === 0
      || translation.summary.trim().length === 0
      || translation.sourceLanguage.trim().length === 0
      || translation.model.trim().length === 0)
  ))) {
    return {
      reason: "incomplete-translated-publication-material",
      diagnosticMetadata: {
        safeMetadataOnly: true,
        translationResultCount: inputs.translations.length
      }
    };
  }

  return undefined;
}

function buildProductionMaterialization(
  request: PersistenceFinalMaterializationRequest,
  inputs: PersistenceFinalMaterializationInputs
): PersistenceProductionMaterialization {
  if (!sameStringSet(request.requiredLanguageCodes, DEFAULT_REQUIRED_LANGUAGE_CODES)) {
    throw new Error("production materialization requires the exact five-language policy");
  }
  const occurredAt = new Date(request.producedAt).toISOString();
  const article = {
    source: sourceName(inputs.approval.canonicalUrl),
    title: inputs.approval.title,
    original_url: inputs.approval.canonicalUrl,
    ...(inputs.approval.imageUrl === undefined ? {} : {
      image_url: inputs.approval.imageUrl
    }),
    ...(inputs.approval.publishedAt === undefined ? {} : {
      published_at: inputs.approval.publishedAt
    }),
    published_on_site_at: occurredAt,
    ...(inputs.approval.description === undefined ? {} : {
      original_excerpt: inputs.approval.description
    }),
    ai_summary: inputs.approval.sourceSummary,
    category: inputs.approval.category,
    ...(inputs.approval.positivityScore === undefined ? {} : {
      positivity_score: inputs.approval.positivityScore
    }),
    ai_provider: "local_ai",
    ai_model: inputs.approval.model,
    status: "translation_pending"
  } as const;
  const summaries = request.requiredLanguageCodes.map((languageCode) => {
    const result = inputs.translations.find((translation) => (
      translation.languageCode === languageCode && translation.qualityStatus === "accepted"
    ));

    if (result === undefined) {
      throw new Error(`missing accepted translation material for ${languageCode}`);
    }
    return {
      original_url: inputs.approval.canonicalUrl,
      language_code: result.languageCode,
      source_language_code: result.sourceLanguage,
      title: result.title,
      summary: result.summary,
      generated_by: "local_ai",
      model: result.model,
      updated_at: occurredAt
    } as const;
  });
  const baseCommand = {
    providerMode: "backend_postgres_primary",
    messageId: request.messageId,
    correlationId: request.correlationId,
    pipelineRunId: request.pipelineRunId,
    stageExecutionId: request.stageExecutionId,
    sourceMessageId: request.sourceMessageId,
    actorService: "worker-uplift-persistence",
    schemaVersion: 1,
    operationVersion: request.articleVersion,
    expectedArticleVersion: request.articleVersion
  } as const;
  const saveArticleCommand: PersistenceSaveAcceptedArticleCommand = {
    ...baseCommand,
    operation: "uplift-save-accepted-articles-batch",
    idempotencyKey: `${request.idempotencyKey}:accepted-article`,
    articles: [article]
  };
  const saveSummariesCommand: PersistenceSaveArticleSummariesCommand = {
    ...baseCommand,
    operation: "uplift-save-article-summaries-batch",
    idempotencyKey: `${request.idempotencyKey}:article-summaries`,
    summaries
  };

  return {
    article,
    summaries,
    saveArticleCommand,
    saveSummariesCommand
  };
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
  const sourceSummary = inputs.approval.sourceSummary.trim().length > 0 ? inputs.approval.sourceSummary : undefined;
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
      policyVersion: BACKEND_CAPTURED_PUBLICATION_POLICY_VERSION,
      articleVersion: request.articleVersion,
      currentArticleVersion: request.articleVersion,
      aggregateVersion: aggregate.aggregateVersion,
      finalAggregateVersion: aggregate.aggregateVersion,
      payloadDigest: aggregate.payloadDigest,
      canonicalIdentityHash: aggregate.articleIdentityHash,
      canonicalIdentityValid: true,
      enrichmentPolicyValid: aggregate.titleRef.trim().length > 0 && aggregate.imageUrlRef.trim().length > 0,
      approvalStatus: inputs.approval.decision === "approved" ? "accepted" : inputs.approval.decision,
      sourceSummaryPersisted: sourceSummary !== undefined,
      ...(sourceSummary === undefined ? {} : {
        persistedSourceSummaryRef: {
          kind: "backend-record",
          uri: request.stageRefs.approval.uri,
          mediaType: "application/json"
        }
      }),
      processingState: "clear",
      originalUrl: inputs.approval.canonicalUrl,
      operationVersion: "public-feed-snapshot-compat-v1",
      publicFeedSnapshotRequest: {
        limit: 6,
        offset: 0,
        category: "all",
        languageCode: "en"
      },
      publicFeedSnapshot: {
        id: request.articleId,
        source: sourceName(inputs.approval.canonicalUrl),
        title: inputs.approval.title,
        originalUrl: inputs.approval.canonicalUrl,
        imageUrl: inputs.approval.imageUrl ?? "",
        publishedAt: inputs.approval.publishedAt ?? occurredAt,
        publishedOnSiteAt: occurredAt,
        aiSummary: inputs.approval.sourceSummary,
        category: inputs.approval.category,
        positivityScore: aggregate.positivityScore ?? 0,
        status: "published",
        snapshotRank: 1
      },
      localizedSummaries: inputs.translations
        .filter((translation) => translation.qualityStatus === "accepted")
        .map((translation) => ({
          languageCode: translation.languageCode,
          title: translation.title,
          summary: translation.summary
        }))
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

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value) => right.includes(value));
}

function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);

    return (parsed.protocol === "http:" || parsed.protocol === "https:") && parsed.hostname.length > 0;
  } catch {
    return false;
  }
}

function sourceName(value: string): string {
  try {
    const hostname = new URL(value).hostname.replace(/^www\./u, "");

    return hostname.length > 0 ? hostname : "NutsNews";
  } catch {
    return "NutsNews";
  }
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
