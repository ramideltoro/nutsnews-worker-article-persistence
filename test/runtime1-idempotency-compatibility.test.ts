import {
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadPersistenceConfig } from "../src/config.js";
import { createPersistenceService } from "../src/service.js";
import {
  InMemoryPersistenceInboxStore,
  LocalBrokerTransport,
  LocalFinalShadowTransactionRunner,
  LocalPersistenceWorkHandler,
  createLocalPersistenceDependencies,
  createMinimalPersistenceDelivery,
  createMinimalPersistenceEnvelope
} from "../src/test-doubles.js";

describe("persistence Runtime 1 idempotency compatibility", () => {
  it("does not release ownership when a committed claim response is lost", async () => {
    const context = createContext();
    const originalClaim = context.inbox.claim.bind(context.inbox);
    const release = vi.spyOn(context.inbox, "releaseClaim");

    vi.spyOn(context.inbox, "claim").mockImplementationOnce(async (idempotencyKey, claimContext, fingerprint) => {
      await originalClaim(idempotencyKey, claimContext, fingerprint);
      throw new Error("claim response lost");
    });

    await context.service.start();

    try {
      await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-claim-error"
      });
      await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-in-progress"
      });

      expect(release).not.toHaveBeenCalled();
      expect(context.workHandler.handled).toHaveLength(0);
    } finally {
      await context.service.stop();
    }
  });

  it("releases an owned claim when completion rejects before commit", async () => {
    const context = createContext();
    const release = vi.spyOn(context.inbox, "releaseClaim");

    vi.spyOn(context.inbox, "markCompleted").mockRejectedValueOnce(new Error("completion unavailable"));
    await context.service.start();

    try {
      await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-completion-error"
      });
      await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });

      expect(release).toHaveBeenCalledTimes(1);
      await expect(release.mock.results[0]?.value).resolves.toEqual({
        status: "released"
      });
      expect(context.workHandler.handled).toHaveLength(2);
    } finally {
      await context.service.stop();
    }
  });

  it("acknowledges final-attempt work when completion commits before its response is lost", async () => {
    const context = createContext();
    const originalMarkCompleted = context.inbox.markCompleted.bind(context.inbox);
    const release = vi.spyOn(context.inbox, "releaseClaim");
    const delivery = finalAttemptDelivery();

    vi.spyOn(context.inbox, "markCompleted").mockImplementationOnce(async (idempotencyKey, completion) => {
      await originalMarkCompleted(idempotencyKey, completion);
      throw new Error("completion response lost");
    });
    await context.service.start();

    try {
      await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      await expect(context.broker.deliverPersistence(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "duplicate"
      });

      expect(release).toHaveBeenCalledTimes(1);
      await expect(release.mock.results[0]?.value).resolves.toEqual({
        status: "preserved-completed"
      });
      expect(context.workHandler.handled).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("quarantines a conflicting fingerprint that races the initial verification", async () => {
    const context = createContext();

    vi.spyOn(context.inbox, "verifyPayloadFingerprint")
      .mockResolvedValueOnce({
        status: "accepted"
      })
      .mockResolvedValueOnce({
        status: "conflict",
        existingFingerprint: "sha256:racing-payload"
      });
    vi.spyOn(context.inbox, "claim").mockRejectedValueOnce(new Error("racing conflicting insert"));
    await context.service.start();

    try {
      await expect(context.broker.deliverPersistence(finalAttemptDelivery())).resolves.toMatchObject({
        action: "dlq",
        reason: "idempotency-payload-conflict"
      });
      expect(context.shadowTransactions.quarantines).toHaveLength(1);
      expect(context.shadowTransactions.quarantines[0]).toMatchObject({
        reason: "idempotency-payload-conflict",
        diagnosticMetadata: {
          existingFingerprint: "sha256:racing-payload"
        }
      });
      expect(context.workHandler.handled).toHaveLength(0);
    } finally {
      await context.service.stop();
    }
  });

  it("rejects stale-token transitions and preserves completed records", async () => {
    const inbox = new InMemoryPersistenceInboxStore();
    const envelope = createMinimalPersistenceEnvelope();
    const claim = await inbox.claim(envelope.idempotencyKey, claimContext(envelope));

    if (claim.status !== "claimed") {
      throw new Error("Expected an owned idempotency claim.");
    }

    const failure = {
      failedAt: "2026-08-01T00:00:02.000Z",
      messageId: envelope.messageId,
      claimToken: `${claim.claimToken}:stale`,
      stage: "persistence" as const,
      reason: "stale-owner",
      retryable: true
    };

    await expect(inbox.releaseClaim(envelope.idempotencyKey, failure)).resolves.toEqual({
      status: "not-owned"
    });
    await expect(inbox.markFailed(envelope.idempotencyKey, failure)).rejects.toThrow("another delivery");

    await inbox.markCompleted(envelope.idempotencyKey, {
      completedAt: "2026-08-01T00:00:03.000Z",
      messageId: envelope.messageId,
      claimToken: claim.claimToken,
      stage: "persistence"
    });

    await expect(inbox.releaseClaim(envelope.idempotencyKey, {
      ...failure,
      claimToken: claim.claimToken
    })).resolves.toEqual({
      status: "preserved-completed"
    });
    await expect(inbox.markFailed(envelope.idempotencyKey, {
      ...failure,
      claimToken: claim.claimToken
    })).rejects.toThrow("another delivery");
    await expect(inbox.claim(envelope.idempotencyKey, claimContext(envelope))).resolves.toMatchObject({
      status: "already-completed"
    });
  });

  it("synchronously validates the live lease before completing short work", async () => {
    const context = createContext(300_000);
    const renew = vi.spyOn(context.inbox, "renewClaim");
    const complete = vi.spyOn(context.inbox, "markCompleted");
    await context.service.start();

    try {
      await expect(context.broker.deliverPersistence()).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(renew).toHaveBeenCalledTimes(1);
      expect(complete).toHaveBeenCalledTimes(1);
      expect(renew.mock.invocationCallOrder[0] ?? 0).toBeLessThan(complete.mock.invocationCallOrder[0] ?? 0);
    } finally {
      await context.service.stop();
    }
  });

  it("renews long-running claims and fails closed when database ownership is lost", async () => {
    vi.useFakeTimers();
    const context = createContext(3_000);
    const handlerStarted = deferred<undefined>();
    const handlerGate = deferred<undefined>();
    const complete = vi.spyOn(context.inbox, "markCompleted");
    const fail = vi.spyOn(context.inbox, "markFailed");
    const release = vi.spyOn(context.inbox, "releaseClaim");
    const renew = vi.spyOn(context.inbox, "renewClaim").mockResolvedValue({
      status: "not-owned"
    });

    context.workHandler.onHandleStart = () => {
      handlerStarted.resolve(undefined);
    };
    context.workHandler.handleGate = handlerGate.promise;

    try {
      await context.service.start();
      const delivery = context.broker.deliverPersistence();

      await handlerStarted.promise;
      await vi.advanceTimersByTimeAsync(1_000);
      handlerGate.resolve(undefined);

      await expect(delivery).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-lease-lost"
      });
      expect(renew).toHaveBeenCalledWith(
        createMinimalPersistenceEnvelope().idempotencyKey,
        expect.any(String)
      );
      expect(complete).not.toHaveBeenCalled();
      expect(fail).not.toHaveBeenCalled();
      expect(release).not.toHaveBeenCalled();
    } finally {
      handlerGate.resolve(undefined);
      await context.service.stop();
      vi.useRealTimers();
    }
  });

  it("cooperatively aborts long-running work when renewal is uncertain", async () => {
    vi.useFakeTimers();
    const context = createContext(3_000);
    const handlerStarted = deferred<undefined>();
    const handlerGate = deferred<undefined>();
    const complete = vi.spyOn(context.inbox, "markCompleted");
    const fail = vi.spyOn(context.inbox, "markFailed");

    vi.spyOn(context.inbox, "renewClaim").mockRejectedValue(new Error("renewal database unavailable"));
    context.workHandler.onHandleStart = () => {
      handlerStarted.resolve(undefined);
    };
    context.workHandler.handleGate = handlerGate.promise;

    try {
      await context.service.start();
      const delivery = context.broker.deliverPersistence();

      await handlerStarted.promise;
      await vi.advanceTimersByTimeAsync(1_000);
      handlerGate.resolve(undefined);

      await expect(delivery).resolves.toMatchObject({
        action: "retry",
        reason: "idempotency-lease-lost"
      });
      expect(context.workHandler.handled).toHaveLength(0);
      expect(complete).not.toHaveBeenCalled();
      expect(fail).not.toHaveBeenCalled();
    } finally {
      handlerGate.resolve(undefined);
      await context.service.stop();
      vi.useRealTimers();
    }
  });
});

function createContext(claimLeaseMs?: number) {
  const config = loadPersistenceConfig({
    HOSTNAME: "persistence-runtime1-idempotency-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_PERSISTENCE_HTTP_PORT: "0",
    NUTSNEWS_PERSISTENCE_TELEMETRY_LOGS: "silent"
  });
  const workHandler = new LocalPersistenceWorkHandler();
  const dependencies = createLocalPersistenceDependencies({
    workHandler
  });

  if (claimLeaseMs !== undefined) {
    Object.defineProperty(dependencies.inboxStore, "claimLeaseMs", {
      configurable: true,
      value: claimLeaseMs
    });
  }

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    inbox: dependencies.inboxStore as InMemoryPersistenceInboxStore,
    shadowTransactions: dependencies.finalShadowTransactions as LocalFinalShadowTransactionRunner,
    service: createPersistenceService({
      config,
      dependencies
    }),
    workHandler
  };
}

function finalAttemptDelivery() {
  const delivery = createMinimalPersistenceDelivery();

  return {
    ...delivery,
    envelope: createMinimalPersistenceEnvelope({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-08-01T00:00:00.000Z",
        lastAttemptAt: "2026-08-01T00:05:00.000Z"
      }
    })
  };
}

function claimContext(envelope: WorkerMessageEnvelope) {
  return {
    envelope,
    stage: "persistence" as const,
    receivedAt: "2026-08-01T00:00:01.000Z"
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolver) => {
    resolve = resolver;
  });

  return {
    promise,
    resolve
  };
}
