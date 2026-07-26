import {
  WORKER_DELIVERY_BEHAVIOR,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  computeRetryJitterMs,
  createRetryEnvelope,
  randomUuidMessageIdFactory,
  runtimeNow,
  runtimeTraceHeadersFromEnvelope,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  connect as amqpConnect,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options
} from "amqplib";

const DEFAULT_CONFIRM_TIMEOUT_MS = WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

interface PayloadCarrier {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
}

interface PayloadConsumerRegistration {
  readonly handler: BrokerDeliveryHandler;
  consumerTag: string | undefined;
}

type RabbitMqConnect = (url: string) => Promise<ChannelModel>;

export class PayloadRabbitMqTransport implements RuntimeBrokerTransport {
  readonly name = "rabbitmq-payload-transport";

  private readonly url: string;
  private readonly prefetchCount: number;
  private readonly clock: RuntimeClock;
  private readonly connectToBroker: RabbitMqConnect;
  private readonly consumers = new Map<WorkerStage, PayloadConsumerRegistration>();
  private readonly inFlight = new Set<Promise<void>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private routes: readonly WorkerRoute[] = [];
  private closing = false;

  constructor(options: {
    readonly url: string;
    readonly prefetch: number;
    readonly clock: RuntimeClock;
    readonly connect?: RabbitMqConnect;
  }) {
    this.url = options.url;
    this.prefetchCount = options.prefetch;
    this.clock = options.clock;
    this.connectToBroker = options.connect ?? amqpConnect;
  }

  get inFlightDeliveryCount(): number {
    return this.inFlight.size;
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.ensureChannel();
  }

  async assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.routes = routes;
    await this.ensureChannel();
  }

  async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const route = getWorkerRoute(command.envelope.route);
    const channel = await this.ensureChannel();

    await publishCarrierWithConfirm(channel, {
      carrier: {
        envelope: command.envelope,
        payload: command.payload
      },
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
    });

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: runtimeNow(this.clock)
    };
  }

  async consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    const channel = await this.ensureChannel();
    const existing = this.consumers.get(stage);

    if (existing?.consumerTag !== undefined) {
      await channel.cancel(existing.consumerTag).catch(() => undefined);
    }

    const registration: PayloadConsumerRegistration = {
      handler,
      consumerTag: undefined
    };
    this.consumers.set(stage, registration);
    await this.activateConsumer(stage, registration, channel);

    return {
      stage,
      cancel: async (): Promise<void> => {
        const registered = this.consumers.get(stage);
        this.consumers.delete(stage);

        if (registered?.consumerTag !== undefined && this.channel !== undefined) {
          await this.channel.cancel(registered.consumerTag).catch(() => undefined);
        }
      }
    };
  }

  async drain(timeoutMs = DEFAULT_DRAIN_TIMEOUT_MS): Promise<void> {
    if (this.inFlight.size === 0) {
      return;
    }

    await Promise.race([
      Promise.all([...this.inFlight]).then(() => undefined),
      new Promise<void>((_, reject) => {
        setTimeout(() => {
          reject(new Error("Timed out waiting for RabbitMQ payload deliveries to drain."));
        }, timeoutMs);
      })
    ]);
  }

  async close(): Promise<void> {
    this.closing = true;
    const channel = this.channel;

    if (channel !== undefined) {
      for (const registration of this.consumers.values()) {
        if (registration.consumerTag !== undefined) {
          await channel.cancel(registration.consumerTag).catch(() => undefined);
        }
      }
    }

    this.consumers.clear();
    await this.drain().catch(() => undefined);

    if (this.channel !== undefined) {
      await this.channel.close().catch(() => undefined);
      this.channel = undefined;
    }

    if (this.connection !== undefined) {
      await this.connection.close().catch(() => undefined);
      this.connection = undefined;
    }
  }

  private async ensureChannel(): Promise<ConfirmChannel> {
    if (this.channel !== undefined) {
      return this.channel;
    }

    if (this.closing) {
      throw new Error("RabbitMQ payload transport is closing.");
    }

    const connection = await this.connectToBroker(this.url);
    const channel = await connection.createConfirmChannel();
    this.connection = connection;
    this.channel = channel;

    connection.on("close", () => {
      if (this.connection === connection) {
        this.connection = undefined;
      }

      this.markChannelClosed(channel);
    });
    channel.on("close", () => {
      this.markChannelClosed(channel);
    });

    await this.restoreConsumers(channel);

    return channel;
  }

  private async restoreConsumers(channel: ConfirmChannel): Promise<void> {
    for (const [stage, registration] of this.consumers) {
      await this.activateConsumer(stage, registration, channel);
    }
  }

  private async activateConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): Promise<void> {
    if (registration.consumerTag !== undefined) {
      return;
    }

    const route = getWorkerRoute(stage);
    await channel.prefetch(this.prefetchCount);
    const reply = await channel.consume(route.mainQueue.name, (message) => {
      if (message === null) {
        registration.consumerTag = undefined;
        return;
      }

      const tracked = this.handleDelivery(registration.handler, message);
      this.inFlight.add(tracked);
      void tracked.finally(() => {
        this.inFlight.delete(tracked);
      });
    }, {
      noAck: false
    });

    registration.consumerTag = reply.consumerTag;
  }

  private markChannelClosed(channel: ConfirmChannel): void {
    if (this.channel === channel) {
      this.channel = undefined;
    }

    for (const registration of this.consumers.values()) {
      registration.consumerTag = undefined;
    }
  }

  private async handleDelivery(
    handler: BrokerDeliveryHandler,
    message: ConsumeMessage
  ): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      return;
    }

    try {
      const carrier = decodeCarrier(message);
      const result = await handler({
        envelope: carrier.envelope,
        payload: carrier.payload,
        receivedAt: runtimeNow(this.clock)
      });
      await this.settleDelivery(channel, message, carrier, result);
    } catch {
      channel.nack(message, false, false);
    }
  }

  private async settleDelivery(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    carrier: PayloadCarrier,
    result: RuntimeMessageProcessingResult
  ): Promise<void> {
    if (result.action === "ack") {
      channel.ack(message);
      return;
    }

    if (result.action === "retry") {
      const retryEnvelope = createRetryEnvelope(result.envelope, {
        now: runtimeNow(this.clock),
        messageIdFactory: randomUuidMessageIdFactory
      });
      const retryJitterMs = computeRetryJitterMs(result.destination.ttlMs, 0.1);
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: retryEnvelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).retryExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS,
        retryJitterMs
      });
      channel.ack(message);
      return;
    }

    if (result.envelope !== undefined && result.destination !== undefined) {
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: result.envelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).dlqExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
      });
      channel.ack(message);
      return;
    }

    channel.nack(message, false, false);
  }
}

async function publishCarrierWithConfirm(
  channel: ConfirmChannel,
  options: {
    readonly carrier: PayloadCarrier;
    readonly exchange: string;
    readonly routingKey: string;
    readonly confirmTimeoutMs: number;
    readonly retryJitterMs?: number;
  }
): Promise<void> {
  const content = Buffer.from(JSON.stringify(options.carrier), "utf8");
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let settled = false;

  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }

      channel.off("return", onReturn);
      channel.off("close", onClose);
      channel.off("error", onChannelError);
      settled = true;
    };
    const fail = (error: Error): void => {
      if (settled) {
        return;
      }

      cleanup();
      reject(error);
    };
    const onReturn = (returned: unknown): void => {
      if (returnedMessageId(returned) === options.carrier.envelope.messageId) {
        fail(new Error(`RabbitMQ publish was returned for ${options.exchange}:${options.routingKey}.`));
      }
    };
    const onClose = (): void => {
      fail(new Error("RabbitMQ channel closed during publish."));
    };
    const onChannelError = (): void => {
      fail(new Error("RabbitMQ channel errored during publish."));
    };

    timeout = setTimeout(() => {
      fail(new Error("RabbitMQ publish confirm timed out."));
    }, options.confirmTimeoutMs);

    channel.on("return", onReturn);
    channel.on("close", onClose);
    channel.on("error", onChannelError);
    channel.publish(
      options.exchange,
      options.routingKey,
      content,
      publishOptions(options.carrier.envelope, options.retryJitterMs),
      (error: unknown) => {
        if (error !== null && error !== undefined) {
          fail(error instanceof Error ? error : new Error("RabbitMQ publish confirm failed."));
          return;
        }

        if (!settled) {
          cleanup();
          resolve();
        }
      }
    );
  });
}

function decodeCarrier(message: ConsumeMessage): PayloadCarrier {
  const parsed = JSON.parse(message.content.toString("utf8")) as unknown;

  if (isRecord(parsed) && isRecord(parsed.envelope)) {
    return {
      envelope: parsed.envelope as unknown as WorkerMessageEnvelope,
      payload: isRecord(parsed.payload) ? parsed.payload : {}
    };
  }

  if (!isRecord(parsed)) {
    throw new Error("RabbitMQ message body must be a JSON object.");
  }

  return {
    envelope: parsed as unknown as WorkerMessageEnvelope,
    payload: {}
  };
}

function publishOptions(envelope: WorkerMessageEnvelope, retryJitterMs: number | undefined): Options.Publish {
  return {
    persistent: true,
    mandatory: true,
    contentType: WORKER_DELIVERY_BEHAVIOR.contentType,
    contentEncoding: WORKER_DELIVERY_BEHAVIOR.contentEncoding,
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    timestamp: Math.floor(Date.parse(envelope.occurredAt) / 1_000),
    headers: {
      schemaId: envelope.schemaId,
      schemaVersion: envelope.schemaVersion,
      route: envelope.route,
      attemptCount: envelope.attempt.count,
      idempotencyKey: envelope.idempotencyKey,
      payloadCarrier: "envelope-plus-payload",
      ...runtimeTraceHeadersFromEnvelope(envelope),
      ...(retryJitterMs === undefined ? {} : {
        retryJitterMs
      })
    }
  };
}

function returnedMessageId(returned: unknown): string | undefined {
  if (!isRecord(returned) || !isRecord(returned.properties)) {
    return undefined;
  }

  const messageId = returned.properties.messageId;

  return typeof messageId === "string" ? messageId : undefined;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
