import type {
  QueryResult,
  QueryResultRow
} from "pg";
import { type Pool } from "pg";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  PERSISTENCE_IDEMPOTENCY_LEASE_MS,
  PostgresPersistenceInboxStore
} from "../src/production.js";
import {
  createMinimalPersistenceEnvelope
} from "../src/test-doubles.js";

const IDEMPOTENCY_KEY = "persistence:final-shadow:article-001:v1";
const PAYLOAD_FINGERPRINT = "sha256:runtime1-persistence-payload";
const FIRST_SEEN_AT = "2026-08-01T00:00:00.000Z";

describe("Postgres persistence Runtime 1 idempotency adapter", () => {
  it("issues a fresh bounded lease and opaque token for an atomic new claim", async () => {
    const pool = new ScriptedPool([
      {
        assert(text, values) {
          expect(text).toContain("ON CONFLICT (idempotency_key) DO NOTHING");
          expect(text).toContain("RETURNING received_at");
          expect(text).toContain("clock_timestamp()");
          expect(text).toContain("claimLeaseExpiresAtEpochMs");
          const metadata = JSON.parse(String(values?.[13])) as Readonly<Record<string, unknown>>;

          expect(values?.[11]).toBe(PAYLOAD_FINGERPRINT);
          expect(metadata).toMatchObject({
            payloadFingerprint: PAYLOAD_FINGERPRINT
          });
          expect(metadata).not.toHaveProperty("claimToken");
          expect(metadata).not.toHaveProperty("claimLeaseExpiresAtEpochMs");
          expect(values?.[14]).toBe("claim-token-1");
          expect(values?.[15]).toBe(PERSISTENCE_IDEMPOTENCY_LEASE_MS);
        },
        result: result([
          {
            received_at: new Date(FIRST_SEEN_AT)
          }
        ], 1)
      }
    ]);
    const store = createStore(pool, [
      "claim-token-1"
    ]);

    await expect(store.claim(IDEMPOTENCY_KEY, claimContext(), PAYLOAD_FINGERPRINT)).resolves.toEqual({
      status: "claimed",
      firstSeenAt: FIRST_SEEN_AT,
      replay: false,
      claimToken: "claim-token-1"
    });
    expect(PERSISTENCE_IDEMPOTENCY_LEASE_MS).toBe(300_000);
    pool.assertExhausted();
  });

  it("reclaims an expired lease at the five-minute bound with a token-aware CAS", async () => {
    const previousLeaseEpochMs = Date.parse("2026-08-01T00:05:00.000Z");
    const pool = new ScriptedPool([
      {
        result: result([], 0)
      },
      {
        assert(text) {
          expect(text).toContain("payload_digest");
          expect(text).toContain("diagnostic_metadata");
        },
        result: result([
          {
            status: "processing",
            payload_digest: PAYLOAD_FINGERPRINT,
            received_at: new Date(FIRST_SEEN_AT),
            processed_at: null,
            diagnostic_metadata: {
              claimToken: "claim-token-1",
              claimLeaseExpiresAtEpochMs: previousLeaseEpochMs
            },
            claim_token: "claim-token-1",
            claim_lease_expires_at_epoch_ms: String(previousLeaseEpochMs),
            claim_control_invalid: false
          }
        ], 1)
      },
      {
        assert(text, values) {
          expect(text).toContain("AND status = $2");
          expect(text).toContain("(diagnostic_metadata->>'claimToken') IS NOT DISTINCT FROM $3");
          expect(text).toContain("(diagnostic_metadata->>'claimLeaseExpiresAtEpochMs') IS NOT DISTINCT FROM $5");
          expect(text).toContain("jsonb_typeof(diagnostic_metadata->'claimLeaseExpiresAtEpochMs') = 'number'");
          expect(text).toContain("clock_timestamp()");
          expect(text).not.toContain("received_at +");
          expect(values?.[1]).toBe("processing");
          expect(values?.[2]).toBe("claim-token-1");
          expect(values?.[4]).toBe(String(previousLeaseEpochMs));
          expect(JSON.parse(String(values?.[3]))).toMatchObject({
            replayMessageId: createMinimalPersistenceEnvelope().messageId
          });
          expect(values?.[5]).toBe("claim-token-2");
          expect(values?.[6]).toBe(PERSISTENCE_IDEMPOTENCY_LEASE_MS);
        },
        result: result([
          {
            received_at: new Date(FIRST_SEEN_AT)
          }
        ], 1)
      }
    ]);
    const store = createStore(pool, [
      "claim-token-2"
    ]);

    await expect(store.claim(IDEMPOTENCY_KEY, claimContext(), PAYLOAD_FINGERPRINT)).resolves.toEqual({
      status: "claimed",
      firstSeenAt: FIRST_SEEN_AT,
      replay: true,
      claimToken: "claim-token-2"
    });
    pool.assertExhausted();
  });

  it("normalizes missing or malformed processing controls with a database-timed grace lease", async () => {
    const metadataCases = [
      {
        claimToken: "legacy-claim-token"
      },
      {
        claimToken: {
          malformed: true
        },
        claimLeaseExpiresAtEpochMs: "not-a-number"
      },
      [],
      "high-precision-fractional-control"
    ];

    for (const diagnosticMetadata of metadataCases) {
      const pool = new ScriptedPool([
        {
          result: result([], 0)
        },
        {
          result: result([
            {
              status: "processing",
              payload_digest: PAYLOAD_FINGERPRINT,
              received_at: new Date(FIRST_SEEN_AT),
              processed_at: null,
              diagnostic_metadata: diagnosticMetadata,
              claim_token: null,
              claim_lease_expires_at_epoch_ms: null,
              claim_control_invalid: true
            }
          ], 1)
        },
        {
          assert(text, values) {
            expect(text).toContain("clock_timestamp()");
            expect(text).toContain("claimControlNormalizedAt");
            expect(text).toContain("jsonb_typeof(diagnostic_metadata) IS DISTINCT FROM 'object'");
            expect(text).toContain("jsonb_typeof(diagnostic_metadata->'claimToken') IS DISTINCT FROM 'string'");
            expect(text).toContain("trunc((diagnostic_metadata->>'claimLeaseExpiresAtEpochMs')::numeric)");
            expect(text).not.toContain("received_at +");
            expect(values).toEqual([
              IDEMPOTENCY_KEY,
              "normalization-token",
              PERSISTENCE_IDEMPOTENCY_LEASE_MS
            ]);
          },
          result: result([], 1)
        }
      ]);
      const store = createStore(pool, [
        "normalization-token"
      ]);

      await expect(store.claim(IDEMPOTENCY_KEY, claimContext(), PAYLOAD_FINGERPRINT)).resolves.toEqual({
        status: "in-progress",
        firstSeenAt: FIRST_SEEN_AT
      });
      pool.assertExhausted();
    }
  });

  it("atomically acquires a schema-default received row", async () => {
    const pool = new ScriptedPool([
      {
        result: result([], 0)
      },
      {
        result: result([
          {
            status: "received",
            payload_digest: PAYLOAD_FINGERPRINT,
            received_at: new Date(FIRST_SEEN_AT),
            processed_at: null,
            diagnostic_metadata: {}
          }
        ], 1)
      },
      {
        assert(text, values) {
          expect(text).toContain("AND status = 'received'");
          expect(text).toContain("AND payload_digest = $2");
          expect(text).toContain("clock_timestamp()");
          expect(values?.[1]).toBe(PAYLOAD_FINGERPRINT);
          expect(values?.[3]).toBe("received-claim-token");
          expect(values?.[4]).toBe(PERSISTENCE_IDEMPOTENCY_LEASE_MS);
        },
        result: result([], 1)
      }
    ]);
    const store = createStore(pool, [
      "received-claim-token"
    ]);

    await expect(store.claim(IDEMPOTENCY_KEY, claimContext(), PAYLOAD_FINGERPRINT)).resolves.toEqual({
      status: "claimed",
      firstSeenAt: FIRST_SEEN_AT,
      replay: true,
      claimToken: "received-claim-token"
    });
    pool.assertExhausted();
  });

  it("keeps a live lease in progress and rejects a racing payload fingerprint", async () => {
    const liveLeasePool = new ScriptedPool([
      {
        result: result([], 0)
      },
      {
        result: result([
          {
            status: "processing",
            payload_digest: PAYLOAD_FINGERPRINT,
            received_at: new Date(FIRST_SEEN_AT),
            processed_at: null,
            diagnostic_metadata: {
              claimToken: "claim-token-1",
              claimLeaseExpiresAtEpochMs: Date.parse("2026-08-01T00:05:00.001Z")
            },
            claim_token: "claim-token-1",
            claim_lease_expires_at_epoch_ms: String(Date.parse("2026-08-01T00:05:00.001Z")),
            claim_control_invalid: false
          }
        ], 1)
      },
      {
        assert(text) {
          expect(text).toContain("clock_timestamp()");
        },
        result: result([], 0)
      }
    ]);
    const liveStore = createStore(liveLeasePool, [
      "claim-token-2"
    ]);

    await expect(liveStore.claim(IDEMPOTENCY_KEY, claimContext(), PAYLOAD_FINGERPRINT)).resolves.toEqual({
      status: "in-progress",
      firstSeenAt: FIRST_SEEN_AT
    });
    liveLeasePool.assertExhausted();

    const conflictPool = new ScriptedPool([
      {
        result: result([], 0)
      },
      {
        result: result([
          {
            status: "processed",
            payload_digest: "sha256:different-payload",
            received_at: new Date(FIRST_SEEN_AT),
            processed_at: new Date("2026-08-01T00:00:02.000Z"),
            diagnostic_metadata: {}
          }
        ], 1)
      }
    ]);
    const conflictStore = createStore(conflictPool, [
      "claim-token-3"
    ]);

    await expect(conflictStore.claim(IDEMPOTENCY_KEY, claimContext(), PAYLOAD_FINGERPRINT)).rejects.toThrow(
      "Conflicting payload fingerprint"
    );
    conflictPool.assertExhausted();
  });

  it("requires claim-token CAS for completion and failure and preserves committed completion on release", async () => {
    const completionPool = new ScriptedPool([
      {
        assert(text, values) {
          expect(text).toContain("AND status = 'processing'");
          expect(text).toContain("diagnostic_metadata->>'claimToken' = $4");
          expect(values?.[3]).toBe("claim-token-1");
        },
        result: result([], 1)
      },
      {
        assert(text, values) {
          expect(text).toContain("diagnostic_metadata->>'claimToken' = $5");
          expect(values?.[4]).toBe("claim-token-1");
        },
        result: result([], 0)
      },
      {
        result: result([
          {
            status: "processed"
          }
        ], 1)
      }
    ]);
    const store = createStore(completionPool, []);
    const completion = {
      completedAt: "2026-08-01T00:00:02.000Z",
      messageId: createMinimalPersistenceEnvelope().messageId,
      claimToken: "claim-token-1",
      stage: "persistence" as const
    };

    await expect(store.markCompleted(IDEMPOTENCY_KEY, completion)).resolves.toBeUndefined();
    await expect(store.releaseClaim(IDEMPOTENCY_KEY, failure("claim-token-1"))).resolves.toEqual({
      status: "preserved-completed"
    });
    completionPool.assertExhausted();

    const stalePool = new ScriptedPool([
      {
        result: result([], 0)
      },
      {
        result: result([], 0)
      },
      {
        result: result([
          {
            status: "processing"
          }
        ], 1)
      }
    ]);
    const staleStore = createStore(stalePool, []);

    await expect(staleStore.markFailed(IDEMPOTENCY_KEY, failure("stale-token"))).rejects.toThrow("another delivery");
    await expect(staleStore.releaseClaim(IDEMPOTENCY_KEY, failure("stale-token"))).resolves.toEqual({
      status: "not-owned"
    });
    stalePool.assertExhausted();
  });

  it("renews only a live token-owned processing lease using the database clock", async () => {
    const pool = new ScriptedPool([
      {
        assert(text, values) {
          expect(text).toContain("SET diagnostic_metadata");
          expect(text).toContain("clock_timestamp()");
          expect(text).toContain("AND status = 'processing'");
          expect(text).toContain("diagnostic_metadata->>'claimToken' = $2");
          expect(text).toContain("jsonb_typeof(diagnostic_metadata->'claimLeaseExpiresAtEpochMs') = 'number'");
          expect(values).toEqual([
            IDEMPOTENCY_KEY,
            "claim-token-1",
            PERSISTENCE_IDEMPOTENCY_LEASE_MS
          ]);
        },
        result: result([], 1)
      },
      {
        result: result([], 0)
      }
    ]);
    const store = createStore(pool, []);

    await expect(store.renewClaim(IDEMPOTENCY_KEY, "claim-token-1")).resolves.toEqual({
      status: "renewed"
    });
    await expect(store.renewClaim(IDEMPOTENCY_KEY, "stale-token")).resolves.toEqual({
      status: "not-owned"
    });
    pool.assertExhausted();
  });

  it("rejects production leases longer than the Runtime 1 contract", () => {
    const pool = new ScriptedPool([]);

    expect(() => new PostgresPersistenceInboxStore(pool.asPool(), {
      leaseMs: PERSISTENCE_IDEMPOTENCY_LEASE_MS + 1
    })).toThrow("no longer than five minutes");
  });
});

interface ScriptedQuery {
  readonly assert?: (text: string, values: readonly unknown[] | undefined) => void;
  readonly result: QueryResult<QueryResultRow>;
}

class ScriptedPool {
  private readonly scripts: ScriptedQuery[];

  constructor(scripts: readonly ScriptedQuery[]) {
    this.scripts = [...scripts];
  }

  asPool(): Pool {
    return this as unknown as Pool;
  }

  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: readonly unknown[]): Promise<QueryResult<T>> {
    const script = this.scripts.shift();

    if (script === undefined) {
      return Promise.reject(new Error(`Unexpected query: ${text}`));
    }

    script.assert?.(text, values);
    return Promise.resolve(script.result as QueryResult<T>);
  }

  assertExhausted(): void {
    expect(this.scripts).toHaveLength(0);
  }
}

function result(rows: readonly QueryResultRow[], rowCount: number): QueryResult<QueryResultRow> {
  return {
    command: "TEST",
    fields: [],
    oid: 0,
    rowCount,
    rows: [...rows]
  };
}

function createStore(pool: ScriptedPool, claimTokens: readonly string[]) {
  const tokens = [...claimTokens];

  return new PostgresPersistenceInboxStore(pool.asPool(), {
    claimTokenFactory: () => {
      const token = tokens.shift();

      if (token === undefined) {
        throw new Error("No scripted claim token remains.");
      }

      return token;
    }
  });
}

function claimContext() {
  return {
    envelope: createMinimalPersistenceEnvelope(),
    stage: "persistence" as const,
    receivedAt: FIRST_SEEN_AT
  };
}

function failure(claimToken: string) {
  return {
    failedAt: "2026-08-01T00:00:03.000Z",
    messageId: createMinimalPersistenceEnvelope().messageId,
    claimToken,
    stage: "persistence" as const,
    reason: "idempotency-completion-error",
    retryable: true
  };
}
