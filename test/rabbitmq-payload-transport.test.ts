import type {
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage
} from "amqplib";
import {
  describe,
  expect,
  it
} from "vitest";
import { createBufferedRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";

import { PayloadRabbitMqTransport } from "../src/rabbitmq-payload-transport.js";
import { createMinimalPersistenceEnvelope } from "../src/test-doubles.js";

type CloseHandler = () => void;

interface FakeBroker {
  readonly connections: FakeConnection[];
  readonly connect: (url: string) => Promise<ChannelModel>;
}

interface FakeConnection {
  readonly channel: FakeChannel;
  emitClose(): void;
  toChannelModel(): ChannelModel;
}

interface FakeChannel {
  readonly cancelTags: string[];
  readonly consumeQueues: string[];
  readonly prefetchCalls: number[];
  readonly publishCalls: number;
  emitConsumerCancel(index?: number): void;
  emitClose(): void;
  toConfirmChannel(): ConfirmChannel;
}

const clock = {
  now: () => new Date("2026-07-26T00:00:00.000Z")
};

describe("RabbitMQ payload transport", () => {
  it("restores registered persistence consumers after reconnecting", async () => {
    const broker = createFakeBroker();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://persistence:test@example.invalid:5672",
      prefetch: 2,
      clock,
      connect: broker.connect
    });

    await transport.consume("persistence", () => Promise.resolve({
      action: "dlq",
      reason: "not-used"
    }));

    expect(broker.connections).toHaveLength(1);
    expect(broker.connections[0]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.persistence.v1"
    ]);
    expect(broker.connections[0]?.channel.prefetchCalls).toEqual([
      2
    ]);

    broker.connections[0]?.emitClose();
    await waitForCondition(() => broker.connections[1]?.channel.consumeQueues.length === 1);

    expect(broker.connections).toHaveLength(2);
    expect(broker.connections[1]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.persistence.v1"
    ]);
    expect(broker.connections[1]?.channel.prefetchCalls).toEqual([
      2
    ]);
  });

  it("reinstalls consumers cancelled by RabbitMQ without reconnecting", async () => {
    const broker = createFakeBroker();
    const telemetry = createBufferedRuntimeTelemetrySink();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://persistence:test@example.invalid:5672",
      prefetch: 2,
      clock,
      connect: broker.connect,
      telemetry
    });

    await transport.consume("persistence", () => Promise.resolve({
      action: "dlq",
      reason: "not-used"
    }));

    broker.connections[0]?.channel.emitConsumerCancel();
    expect(telemetry.events).toContainEqual(expect.objectContaining({
      name: "runtime.broker.consumer_state_changed",
      outcome: "cancelled",
      stage: "persistence"
    }));
    await waitForCondition(() => broker.connections[0]?.channel.consumeQueues.length === 2);

    expect(broker.connections).toHaveLength(1);
    expect(broker.connections[0]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.persistence.v1",
      "nutsnews.worker.persistence.v1"
    ]);
    expect(broker.connections[0]?.channel.prefetchCalls).toEqual([
      2,
      2
    ]);
    expect(transport.consumerStatus("persistence")).toMatchObject({
      state: "active",
      activeConsumers: 1
    });
  });

  it("cancels a publish waiting on reconnect and never resumes it on a late connection", async () => {
    const connection = createFakeConnection();
    const deferredConnection = deferred<ChannelModel>();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://persistence:test@example.invalid:5672",
      prefetch: 2,
      clock,
      connect: () => deferredConnection.promise
    });
    const controller = new AbortController();
    const pendingPublish = transport.publishWithSignal({
      envelope: createMinimalPersistenceEnvelope(),
      payload: {}
    }, controller.signal);

    controller.abort(new Error("idempotency lease renewal failed"));

    await expect(pendingPublish).rejects.toThrow("idempotency lease renewal failed");
    deferredConnection.resolve(connection.toChannelModel());
    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });

    expect(connection.channel.publishCalls).toBe(0);
    await transport.close();
  });
});

function createFakeBroker(): FakeBroker {
  const connections: FakeConnection[] = [];

  return {
    connections,
    connect: (url: string): Promise<ChannelModel> => {
      expect(url).toBe("amqp://persistence:test@example.invalid:5672");
      const connection = createFakeConnection();
      connections.push(connection);
      return Promise.resolve(connection.toChannelModel());
    }
  };
}

function createFakeConnection(): FakeConnection {
  const channel = createFakeChannel();
  const closeHandlers: CloseHandler[] = [];
  const connection = {
    createConfirmChannel(): Promise<ConfirmChannel> {
      return Promise.resolve(channel.toConfirmChannel());
    },
    close(): Promise<void> {
      for (const handler of closeHandlers) {
        handler();
      }

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (event === "close" && isCloseHandler(handler)) {
        closeHandlers.push(handler);
      }

      return connection;
    }
  };

  return {
    channel,
    emitClose(): void {
      for (const handler of closeHandlers) {
        handler();
      }
    },
    toChannelModel(): ChannelModel {
      return connection as unknown as ChannelModel;
    }
  };
}

function createFakeChannel(): FakeChannel {
  const cancelTags: string[] = [];
  const consumeQueues: string[] = [];
  const consumers: ((message: ConsumeMessage | null) => void)[] = [];
  const prefetchCalls: number[] = [];
  let publishCalls = 0;
  const closeHandlers: CloseHandler[] = [];
  const channel = {
    prefetch(count: number): Promise<void> {
      prefetchCalls.push(count);
      return Promise.resolve();
    },
    consume(queue: string, onMessage: (message: ConsumeMessage | null) => void): Promise<{ readonly consumerTag: string }> {
      consumeQueues.push(queue);
      consumers.push(onMessage);
      return Promise.resolve({
        consumerTag: `consumer-${String(consumeQueues.length)}`
      });
    },
    cancel(consumerTag: string): Promise<void> {
      cancelTags.push(consumerTag);
      return Promise.resolve();
    },
    close(): Promise<void> {
      for (const handler of closeHandlers) {
        handler();
      }

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (event === "close" && isCloseHandler(handler)) {
        closeHandlers.push(handler);
      }

      return channel;
    },
    off(): unknown {
      return channel;
    },
    publish(
      _exchange: string,
      _routingKey: string,
      _content: Buffer,
      _options: unknown,
      callback: (error: unknown) => void
    ): boolean {
      publishCalls += 1;
      callback(null);
      return true;
    }
  };

  return {
    cancelTags,
    consumeQueues,
    prefetchCalls,
    get publishCalls(): number {
      return publishCalls;
    },
    emitConsumerCancel(index = consumers.length - 1): void {
      consumers[index]?.(null);
    },
    emitClose(): void {
      for (const handler of closeHandlers) {
        handler();
      }
    },
    toConfirmChannel(): ConfirmChannel {
      return channel as unknown as ConfirmChannel;
    }
  };
}

function deferred<T>(): {
  readonly promise: Promise<T>;
  resolve(value: T): void;
} {
  let resolvePromise: ((value: T) => void) | undefined;
  const promise = new Promise<T>((resolve) => {
    resolvePromise = resolve;
  });

  return {
    promise,
    resolve(value): void {
      resolvePromise?.(value);
    }
  };
}

function isCloseHandler(handler: unknown): handler is CloseHandler {
  return typeof handler === "function";
}

async function waitForCondition(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (condition()) {
      return;
    }

    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
  }

  throw new Error("Timed out waiting for condition.");
}
