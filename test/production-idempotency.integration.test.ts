import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it
} from "vitest";
import { Pool } from "pg";

import { PostgresPersistenceInboxStore } from "../src/production.js";
import { createMinimalPersistenceEnvelope } from "../src/test-doubles.js";

const TEST_DATABASE_URL = process.env.NUTSNEWS_PERSISTENCE_TEST_DATABASE_URL?.trim();
const describePostgres = TEST_DATABASE_URL === undefined || TEST_DATABASE_URL.length === 0
  ? describe.skip
  : describe;
const PAYLOAD_FINGERPRINT = "sha256:postgres-runtime1-payload";

describePostgres("Postgres persistence Runtime 1 MVCC conformance", () => {
  let ownerPool: Pool | undefined;
  let contenderPool: Pool | undefined;

  beforeAll(async () => {
    ownerPool = new Pool({
      connectionString: requiredDatabaseUrl(),
      max: 2
    });
    contenderPool = new Pool({
      connectionString: requiredDatabaseUrl(),
      max: 2
    });
    await requiredPool(ownerPool).query(`
      CREATE SCHEMA IF NOT EXISTS worker_uplift_persistence;
      CREATE TABLE IF NOT EXISTS worker_uplift_persistence.inbox (
        message_id text,
        pipeline_run_id text,
        stage_execution_id text,
        source_stage text,
        source_message_id text,
        entity_kind text,
        entity_id text,
        schema_version integer,
        operation_version integer,
        idempotency_key text PRIMARY KEY,
        payload_ref text,
        payload_digest text NOT NULL,
        received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        processed_at timestamptz,
        status text NOT NULL DEFAULT 'received',
        diagnostic_metadata jsonb,
        sanitized_error_code text,
        sanitized_error_message text
      )
    `);
  });

  beforeEach(async () => {
    await requiredPool(ownerPool).query("TRUNCATE worker_uplift_persistence.inbox");
  });

  afterAll(async () => {
    await Promise.all([
      ownerPool?.end(),
      contenderPool?.end()
    ]);
  });

  it("serializes live renewal, expiry reclaim, stale completion, and committed completion", async () => {
    const idempotencyKey = "persistence:postgres-mvcc:article-001:v1";
    const owner = storeWithTokens(requiredPool(ownerPool), [
      "owner-token"
    ], 5_000);
    const contender = storeWithTokens(requiredPool(contenderPool), [
      "live-contender-token",
      "expired-contender-token",
      "post-completion-unused-token"
    ], 5_000);
    const initial = await owner.claim(idempotencyKey, claimContext(idempotencyKey), PAYLOAD_FINGERPRINT);

    expect(initial).toMatchObject({
      status: "claimed",
      claimToken: "owner-token"
    });

    const [liveRenewal, liveContender] = await Promise.all([
      owner.renewClaim(idempotencyKey, "owner-token"),
      contender.claim(idempotencyKey, claimContext(idempotencyKey), PAYLOAD_FINGERPRINT)
    ]);

    expect(liveRenewal).toEqual({
      status: "renewed"
    });
    expect(liveContender).toMatchObject({
      status: "in-progress"
    });

    await requiredPool(ownerPool).query(
      `UPDATE worker_uplift_persistence.inbox
       SET diagnostic_metadata = diagnostic_metadata || jsonb_build_object(
         'claimLeaseExpiresAtEpochMs', floor(extract(epoch from clock_timestamp()) * 1000)::bigint
       )
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    const [expiredRenewal, reclaimed] = await Promise.all([
      owner.renewClaim(idempotencyKey, "owner-token"),
      contender.claim(idempotencyKey, claimContext(idempotencyKey), PAYLOAD_FINGERPRINT)
    ]);

    expect(expiredRenewal).toEqual({
      status: "not-owned"
    });
    expect(reclaimed).toMatchObject({
      status: "claimed",
      replay: true,
      claimToken: "expired-contender-token"
    });

    await expect(owner.markCompleted(idempotencyKey, completion("owner-token"))).rejects.toThrow("another delivery");
    await expect(contender.markCompleted(idempotencyKey, completion("expired-contender-token"))).resolves.toBeUndefined();
    await expect(contender.claim(idempotencyKey, claimContext(idempotencyKey), PAYLOAD_FINGERPRINT)).resolves.toMatchObject({
      status: "already-completed"
    });
  });

  it("normalizes malformed controls with server time before allowing reclaim", async () => {
    const malformedCases = [
      {
        suffix: "object",
        metadataJson: JSON.stringify({
          claimToken: {
            malformed: true
          },
          claimLeaseExpiresAtEpochMs: "caller-controlled"
        })
      },
      {
        suffix: "array",
        metadataJson: "[]"
      },
      {
        suffix: "fraction",
        metadataJson: "{\"claimToken\":\"legacy-token\",\"claimLeaseExpiresAtEpochMs\":1754000000000.0000000001}"
      }
    ] as const;

    for (const malformedCase of malformedCases) {
      const idempotencyKey = `persistence:postgres-malformed-${malformedCase.suffix}:article-001:v1`;
      const normalizedToken = `normalized-${malformedCase.suffix}-token`;
      await requiredPool(ownerPool).query(
        `INSERT INTO worker_uplift_persistence.inbox (
           idempotency_key, payload_digest, received_at, status, diagnostic_metadata
         ) VALUES ($1, $2, clock_timestamp() - interval '1 day', 'processing', $3::jsonb)`,
        [
          idempotencyKey,
          PAYLOAD_FINGERPRINT,
          malformedCase.metadataJson
        ]
      );
      const store = storeWithTokens(requiredPool(contenderPool), [
        normalizedToken
      ], 5_000);

      await expect(store.claim(idempotencyKey, claimContext(idempotencyKey), PAYLOAD_FINGERPRINT)).resolves.toMatchObject({
        status: "in-progress"
      });

      const controls = await requiredPool(ownerPool).query<{
        readonly lease_type: string;
        readonly lease_is_future: boolean;
        readonly metadata_type: string;
        readonly token: string;
        readonly token_type: string;
      }>(
        `SELECT
           jsonb_typeof(diagnostic_metadata) AS metadata_type,
           jsonb_typeof(diagnostic_metadata->'claimToken') AS token_type,
           diagnostic_metadata->>'claimToken' AS token,
           jsonb_typeof(diagnostic_metadata->'claimLeaseExpiresAtEpochMs') AS lease_type,
           (diagnostic_metadata->>'claimLeaseExpiresAtEpochMs')::numeric
             > floor(extract(epoch from clock_timestamp()) * 1000)::bigint AS lease_is_future
         FROM worker_uplift_persistence.inbox
         WHERE idempotency_key = $1`,
        [idempotencyKey]
      );

      expect(controls.rows[0]).toEqual({
        metadata_type: "object",
        token_type: "string",
        token: normalizedToken,
        lease_type: "number",
        lease_is_future: true
      });
    }
  });

  it("atomically acquires the schema-default received state", async () => {
    const idempotencyKey = "persistence:postgres-received:article-001:v1";
    await requiredPool(ownerPool).query(
      `INSERT INTO worker_uplift_persistence.inbox (idempotency_key, payload_digest)
       VALUES ($1, $2)`,
      [
        idempotencyKey,
        PAYLOAD_FINGERPRINT
      ]
    );
    const store = storeWithTokens(requiredPool(contenderPool), [
      "received-owner-token"
    ], 5_000);

    await expect(store.claim(idempotencyKey, claimContext(idempotencyKey), PAYLOAD_FINGERPRINT)).resolves.toMatchObject({
      status: "claimed",
      replay: true,
      claimToken: "received-owner-token"
    });
  });
});

function requiredDatabaseUrl(): string {
  if (TEST_DATABASE_URL === undefined || TEST_DATABASE_URL.length === 0) {
    throw new Error("NUTSNEWS_PERSISTENCE_TEST_DATABASE_URL is required for PostgreSQL integration tests.");
  }

  return TEST_DATABASE_URL;
}

function requiredPool(pool: Pool | undefined): Pool {
  if (pool === undefined) {
    throw new Error("PostgreSQL integration pool has not been initialized.");
  }

  return pool;
}

function storeWithTokens(pool: Pool, claimTokens: readonly string[], leaseMs: number): PostgresPersistenceInboxStore {
  const tokens = [...claimTokens];

  return new PostgresPersistenceInboxStore(pool, {
    leaseMs,
    claimTokenFactory: () => {
      const token = tokens.shift();

      if (token === undefined) {
        throw new Error("No PostgreSQL integration claim token remains.");
      }

      return token;
    }
  });
}

function claimContext(idempotencyKey: string) {
  return {
    envelope: createMinimalPersistenceEnvelope({
      idempotencyKey
    }),
    stage: "persistence" as const,
    receivedAt: "2026-08-01T00:00:00.000Z"
  };
}

function completion(claimToken: string) {
  return {
    completedAt: "2026-08-01T00:00:01.000Z",
    messageId: createMinimalPersistenceEnvelope().messageId,
    claimToken,
    stage: "persistence" as const
  };
}
