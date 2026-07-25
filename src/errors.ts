export class PersistenceProcessingError extends Error {
  constructor(
    message: string,
    readonly reason: string,
    readonly retryable: boolean
  ) {
    super(message);
    this.name = "PersistenceProcessingError";
  }
}

export class PersistenceTransientError extends PersistenceProcessingError {
  constructor(reason: string, message = reason) {
    super(message, reason, true);
    this.name = "PersistenceTransientError";
  }
}

export class PersistencePermanentError extends PersistenceProcessingError {
  constructor(reason: string, message = reason) {
    super(message, reason, false);
    this.name = "PersistencePermanentError";
  }
}

export interface PersistenceErrorClassification {
  readonly reason: string;
  readonly retryable: boolean;
}

export function classifyPersistenceError(error: unknown): PersistenceErrorClassification {
  if (error instanceof PersistenceProcessingError) {
    return {
      reason: error.reason,
      retryable: error.retryable
    };
  }

  if (error instanceof Error && error.name.length > 0) {
    return {
      reason: error.name,
      retryable: true
    };
  }

  return {
    reason: "unknown-handler-error",
    retryable: true
  };
}
