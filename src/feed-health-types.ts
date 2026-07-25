export type FeedHealthOutcomeStage =
  | "fetcher"
  | "canonicalizer"
  | "enrichment"
  | "approval"
  | "translation"
  | "persistence"
  | "terminal";

export type FeedHealthOutcomeKind =
  | "fetch_attempt"
  | "candidate_batch"
  | "duplicate_batch"
  | "image_enrichment"
  | "approval_batch"
  | "translation_batch"
  | "persistence_write"
  | "terminal_failure";

export type FeedHealthOutcomeStatus = "success" | "failure" | "partial" | "duplicate";

export interface FeedHealthProjectionCounts {
  readonly itemCount: number;
  readonly duplicateCount: number;
  readonly imageCount: number;
  readonly eligibleCount: number;
  readonly rejectedCount: number;
  readonly acceptedCount: number;
}

export interface FeedHealthProjectionEvent {
  readonly projectionId: string;
  readonly idempotencyKey: string;
  readonly pipelineRunId: string;
  readonly stageExecutionId: string;
  readonly sourceMessageId: string;
  readonly messageId: string;
  readonly correlationId: string;
  readonly feedKey: string;
  readonly feedUrl: string;
  readonly source: string;
  readonly outcomeStage: FeedHealthOutcomeStage;
  readonly outcomeKind: FeedHealthOutcomeKind;
  readonly outcomeStatus: FeedHealthOutcomeStatus;
  readonly eventVersion: number;
  readonly occurredAt: string;
  readonly latencyMs: number;
  readonly counts: FeedHealthProjectionCounts;
  readonly errorClass?: string;
  readonly backoffRecommendation: "none" | "retry" | "slow_down" | "disable_candidate";
  readonly payloadDigest: string;
}

export interface FeedHealthProjectionState {
  readonly feedKey: string;
  readonly source: string;
  readonly feedUrl: string;
  readonly lastEventVersion: number;
  readonly lastAttemptAt?: string;
  readonly lastSuccessAt?: string;
  readonly lastFailureAt?: string;
  readonly lastStatus: "success" | "failure" | "partial" | "duplicate";
  readonly lastErrorClass?: string;
  readonly lastItemCount: number;
  readonly lastDuplicateCount: number;
  readonly lastImageCount: number;
  readonly lastEligibleCount: number;
  readonly lastRejectedCount: number;
  readonly lastAcceptedCount: number;
  readonly consecutiveFailureCount: number;
  readonly totalFetchCount: number;
  readonly totalSuccessCount: number;
  readonly totalFailureCount: number;
  readonly totalItemCount: number;
  readonly totalDuplicateCount: number;
  readonly totalImageCount: number;
  readonly totalEligibleCount: number;
  readonly totalRejectedCount: number;
  readonly totalAcceptedCount: number;
  readonly totalLatencyMs: number;
  readonly updatedAt: string;
  readonly backoffRecommendation: FeedHealthProjectionEvent["backoffRecommendation"];
  readonly errorSamples: readonly FeedHealthProjectionErrorSample[];
}

export interface FeedHealthProjectionErrorSample {
  readonly occurredAt: string;
  readonly outcomeStage: FeedHealthOutcomeStage;
  readonly outcomeKind: FeedHealthOutcomeKind;
  readonly errorClass: string;
}

export interface FeedHealthLegacyRow {
  readonly source: string;
  readonly feed_url: string;
  readonly last_checked_at: string;
  readonly last_success_at: string | null;
  readonly last_failure_at: string | null;
  readonly last_status: string;
  readonly last_error_message: string | null;
  readonly last_article_count: number;
  readonly last_image_count: number;
  readonly last_accepted_count: number;
  readonly last_rejected_count: number;
  readonly consecutive_failure_count: number;
  readonly total_fetch_count: number;
  readonly total_success_count: number;
  readonly total_failure_count: number;
  readonly total_article_count: number;
  readonly total_image_count: number;
  readonly total_accepted_count: number;
  readonly total_rejected_count: number;
  readonly updated_at: string;
}

export interface FeedQualityLegacyRow {
  readonly source: string;
  readonly feed_url: string;
  readonly quality_score: number;
  readonly success_rate: number;
  readonly thumbnail_rate: number;
  readonly accepted_rate: number;
  readonly failure_rate: number;
  readonly duplicate_rate: number;
  readonly total_fetch_count: number;
  readonly total_success_count: number;
  readonly total_failure_count: number;
  readonly total_article_count: number;
  readonly total_image_count: number;
  readonly total_accepted_count: number;
  readonly total_rejected_count: number;
  readonly unique_reviewed_url_count: number;
  readonly unique_published_url_count: number;
}

export type FeedHealthProjectionWriteResult =
  | {
      readonly status: "projected";
      readonly state: FeedHealthProjectionState;
      readonly feedHealthRow: FeedHealthLegacyRow;
      readonly feedQualityRow: FeedQualityLegacyRow;
    }
  | {
      readonly status: "duplicate" | "stale";
      readonly state: FeedHealthProjectionState;
    }
  | {
      readonly status: "conflict";
      readonly reason: string;
    };
