import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { PersistencePermanentError } from "../src/errors.js";
import { HttpPersistenceBackendWorkerApiClient } from "../src/production.js";
import type {
  PersistenceSaveAcceptedArticleCommand,
  PersistenceSaveArticleSummariesCommand
} from "../src/materialization-types.js";

describe("protected persistence backend production commands", () => {
  it("accepts only exact machine-confirmed article and summary counts", async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ ok: true, articleCount: 1 }))
      .mockResolvedValueOnce(jsonResponse({ ok: true, summaryCount: 5 }));
    const client = new HttpPersistenceBackendWorkerApiClient({
      baseUrl: "https://backend.example.test/api/worker/db",
      token: "test-only",
      expectedVersion: "worker-api-v1",
      productionDomainWritesEnabled: true,
      fetcher
    });

    await expect(client.saveAcceptedArticle(articleCommand())).resolves.toMatchObject({
      status: "recorded",
      affectedCount: 1
    });
    await expect(client.saveArticleSummaries(summaryCommand())).resolves.toMatchObject({
      status: "recorded",
      affectedCount: 5
    });
    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "https://backend.example.test/api/worker/db/uplift-save-accepted-articles-batch",
      expect.any(Object)
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "https://backend.example.test/api/worker/db/uplift-save-article-summaries-batch",
      expect.any(Object)
    );
  });

  it("fails closed when writes are disabled or the backend does not prove the exact count", async () => {
    const disabled = new HttpPersistenceBackendWorkerApiClient({
      baseUrl: "https://backend.example.test/api/worker/db",
      token: "test-only",
      expectedVersion: "worker-api-v1",
      fetcher: vi.fn()
    });
    await expect(disabled.saveAcceptedArticle(articleCommand())).rejects.toBeInstanceOf(PersistencePermanentError);

    const unconfirmed = new HttpPersistenceBackendWorkerApiClient({
      baseUrl: "https://backend.example.test/api/worker/db",
      token: "test-only",
      expectedVersion: "worker-api-v1",
      productionDomainWritesEnabled: true,
      fetcher: vi.fn().mockResolvedValue(jsonResponse({ ok: true }))
    });
    await expect(unconfirmed.saveAcceptedArticle(articleCommand())).rejects.toThrow("backend-api-production-write-unconfirmed");
  });
});

function articleCommand(): PersistenceSaveAcceptedArticleCommand {
  return {
    ...baseCommand(),
    operation: "uplift-save-accepted-articles-batch",
    idempotencyKey: "persistence:article-001:accepted-article",
    articles: [{
      source: "publisher.example.test",
      title: "Community library opens",
      original_url: "https://publisher.example.test/article-001",
      image_url: "https://publisher.example.test/article-001.jpg",
      published_on_site_at: "2026-08-01T20:00:00.000Z",
      ai_summary: "Neighbors opened a free community library.",
      category: "Community | Uplifting",
      ai_provider: "local_ai",
      ai_model: "qwen2.5:3b",
      status: "translation_pending"
    }]
  };
}

function summaryCommand(): PersistenceSaveArticleSummariesCommand {
  return {
    ...baseCommand(),
    operation: "uplift-save-article-summaries-batch",
    idempotencyKey: "persistence:article-001:article-summaries",
    summaries: ["fr", "ja", "de-CH", "de", "el"].map((languageCode) => ({
      original_url: "https://publisher.example.test/article-001",
      language_code: languageCode,
      source_language_code: "en",
      title: `Localized title ${languageCode}`,
      summary: `Localized summary ${languageCode}`,
      generated_by: "local_ai" as const,
      model: "qwen2.5:3b",
      updated_at: "2026-08-01T20:00:00.000Z"
    }))
  };
}

function baseCommand() {
  return {
    providerMode: "backend_postgres_primary" as const,
    messageId: "message-001",
    correlationId: "correlation-001",
    pipelineRunId: "pipeline-001",
    stageExecutionId: "stage-001",
    sourceMessageId: "source-001",
    actorService: "worker-uplift-persistence" as const,
    schemaVersion: 1 as const,
    operationVersion: 1,
    expectedArticleVersion: 1
  };
}

function jsonResponse(body: Readonly<Record<string, unknown>>): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: {
      "content-type": "application/json"
    }
  });
}
