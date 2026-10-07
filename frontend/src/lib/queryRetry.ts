/**
 * How many times a failed cluster query is retried before the error is shown
 * to the user.
 *
 * React Query's default is 3, which — against a backend where every attempt
 * is a fresh Kafka client, a TCP connection and (on a secured cluster) a TLS
 * and SASL handshake — quietly turns one failing panel into four rounds of
 * broker work, multiplied by every user with the app open. Two is enough to
 * ride out a leader election or a broker restart.
 */
export const MAX_QUERY_RETRIES = 2;

/** Longest gap between retries. Keeps the backoff from stretching a transient failure into a multi-minute wait. */
export const RETRY_DELAY_CAP_MS = 8_000;

/**
 * Whether a failure is the backend refusing the connection's credentials.
 *
 * Matched on the `AppError::Authentication` display text that
 * `format_report` (src-tauri/src/commands/connections.rs) puts at the front
 * of the message. Authorization failures ("topic authorization failed") are
 * deliberately not included: those say this principal can't read this
 * resource, not that the connection's credentials are wrong.
 */
export function isAuthError(error: unknown): boolean {
  return error instanceof Error && error.message.toLowerCase().startsWith("authentication error");
}

/**
 * An error a query throws to say "asking again will not help": it already
 * waited out its own deadline, or finished and found nothing. `shouldRetry`
 * never retries one, so a 100 second deadline stays 100 seconds rather than
 * becoming three times that.
 */
export class NonRetryableQueryError extends Error {}

/**
 * Retry policy for every cluster query. Rejected credentials are never
 * retried — no number of attempts makes a wrong password right, and each one
 * costs the broker a handshake it has to process and log. Nor is a
 * [`NonRetryableQueryError`].
 */
export function shouldRetry(failureCount: number, error: unknown): boolean {
  return !isAuthError(error) && !(error instanceof NonRetryableQueryError) && failureCount < MAX_QUERY_RETRIES;
}

/**
 * Exponential backoff with jitter, capped at [`RETRY_DELAY_CAP_MS`].
 *
 * The jitter matters at the scale this app runs at: without it, every open
 * copy of the app that saw the same broker blip retries at the same instant,
 * turning a stumble into a thundering herd on a cluster that is already
 * struggling.
 */
export function retryDelay(attemptIndex: number): number {
  const backoff = Math.min(RETRY_DELAY_CAP_MS, 1_000 * 2 ** attemptIndex);
  return backoff * (0.5 + Math.random() * 0.5);
}
