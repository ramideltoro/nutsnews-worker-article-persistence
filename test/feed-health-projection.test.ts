import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import { createPersistenceService } from "../src/service.js";
import {
  LocalBackendWorkerApiClient,
  LocalBrokerTransport,
  LocalFeedHealthProjectionStore,
  LocalFinalShadowTransactionRunner,
  createLocalPersistenceDependencies,
  createMinimalFeedHealthProjectionDelivery,
  createMinimalFeedHealthProjectionEnvelope,
  createMinimalFeedHealthProjectionPayload,
  createMinimalPersistenceDelivery
} from "../src/test-doubles.js";

describe("feed-health projection", () => {
  it("projects feed health and quality rows without backend or publication side effects", async () => {
    const context = createProjectionContext();

    await context.service.start();

    await expect(context.broker.deliverPersistence(createMinimalFeedHealthProjectionDelivery())).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.feedProjection.projectedEvents).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);
    await expect(context.feedProjection.readLegacyFeedHealthRows()).resolves.toEqual([
      expect.objectContaining({
        source: "Example",
        feed_url: "https://example.com/feed.xml",
        last_status: "success",
        last_article_count: 10,
        last_image_count: 8,
        last_accepted_count: 5,
        last_rejected_count: 1,
        consecutive_failure_count: 0,
        total_fetch_count: 1,
        total_success_count: 1,
        total_article_count: 10,
        total_image_count: 8,
        total_accepted_count: 5,
        total_rejected_count: 1
      })
    ]);
    await expect(context.feedProjection.readLegacyFeedQualityRows()).resolves.toEqual([
      expect.objectContaining({
        source: "Example",
        feed_url: "https://example.com/feed.xml",
        total_fetch_count: 1,
        success_rate: 100,
        thumbnail_rate: 80,
        accepted_rate: 83.33,
        duplicate_rate: 16.67,
        unique_reviewed_url_count: 6,
        unique_published_url_count: 5
      })
    ]);

    await context.service.stop();
  });

  it("handles duplicate, late, and out-of-order projection events deterministically", async () => {
    const context = createProjectionContext();
    const version2 = createProjectionDelivery({
      idempotencyKey: "feed-health:example-feed:v2",
      eventVersion: 2,
      itemCount: 20,
      acceptedCount: 12
    });
    const lateVersion1 = createProjectionDelivery({
      idempotencyKey: "feed-health:example-feed:v1-late",
      eventVersion: 1,
      itemCount: 99,
      acceptedCount: 99
    });

    await context.service.start();

    await expect(context.broker.deliverPersistence(version2)).resolves.toMatchObject({
      action: "ack"
    });
    await expect(context.broker.deliverPersistence(lateVersion1)).resolves.toMatchObject({
      action: "ack"
    });
    await expect(context.broker.deliverPersistence(version2)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.feedProjection.projectedEvents).toHaveLength(1);
    expect(context.feedProjection.staleEvents).toHaveLength(1);
    await expect(context.feedProjection.readLegacyFeedHealthRows()).resolves.toEqual([
      expect.objectContaining({
        total_article_count: 20,
        total_accepted_count: 12
      })
    ]);

    await context.service.stop();
  });

  it("rejects raw content and keeps projection failures isolated from final materialization", async () => {
    const context = createProjectionContext();
    const rawProjection = createProjectionDelivery({
      idempotencyKey: "feed-health:example-feed:raw",
      eventVersion: 3,
      rawFeedBody: "<rss>raw body</rss>"
    });

    await context.service.start();

    await expect(context.broker.deliverPersistence(rawProjection)).resolves.toMatchObject({
      action: "dlq",
      reason: "projection-payload-carries-raw-content"
    });
    await expect(context.broker.deliverPersistence(createMinimalPersistenceDelivery())).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.feedProjection.projectedEvents).toHaveLength(0);
    expect(context.finalShadow.materializations).toHaveLength(1);
    expect(context.backendApi.shadowAggregateCommands).toHaveLength(1);

    await context.service.stop();
  });
});

function createProjectionContext() {
  const config = loadPersistenceConfig({
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalPersistenceDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPrometheusRuntimeTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const service = createPersistenceService({
    config,
    dependencies,
    telemetry,
    metrics
  });

  return {
    backendApi: dependencies.backendApiClient as LocalBackendWorkerApiClient,
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    feedProjection: dependencies.feedHealthProjectionStore as LocalFeedHealthProjectionStore,
    finalShadow: dependencies.finalShadowTransactions as LocalFinalShadowTransactionRunner,
    service
  };
}

function createProjectionDelivery(options: {
  readonly idempotencyKey: string;
  readonly eventVersion: number;
  readonly itemCount?: number;
  readonly acceptedCount?: number;
  readonly rawFeedBody?: string;
}) {
  const entityRef = {
    projectionKind: "feed_health",
    projectionId: `example-feed:v${String(options.eventVersion)}`,
    feedKey: "example-feed",
    feedUrl: "https://example.com/feed.xml",
    source: "Example",
    outcomeStage: "fetcher",
    outcomeKind: "fetch_attempt",
    outcomeStatus: "success",
    eventVersion: options.eventVersion,
    occurredAt: `2026-07-23T00:00:0${String(options.eventVersion)}.000Z`,
    latencyMs: 250,
    counts: {
      itemCount: options.itemCount ?? 10,
      duplicateCount: 2,
      imageCount: 8,
      eligibleCount: 6,
      rejectedCount: 1,
      acceptedCount: options.acceptedCount ?? 5
    },
    backoffRecommendation: "none",
    ...(options.rawFeedBody === undefined ? {} : {
      rawFeedBody: options.rawFeedBody
    })
  };

  return {
    envelope: createMinimalFeedHealthProjectionEnvelope({
      idempotencyKey: options.idempotencyKey,
      aggregate: {
        type: "feed",
        id: "example-feed",
        version: options.eventVersion
      }
    }),
    payload: createMinimalFeedHealthProjectionPayload({
      idempotencyKey: options.idempotencyKey,
      entityRefs: [
        entityRef
      ]
    }),
    receivedAt: "2026-07-23T00:00:10.000Z"
  };
}
