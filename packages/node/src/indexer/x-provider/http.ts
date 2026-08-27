// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

// overwrite the official Polkadot HttpProvider: https://github.com/polkadot-js/api/blob/master/packages/rpc-provider/src/http/index.ts
// Use context and fetch to provide http1 keepAlive and maxSocket feature

import { RpcCoder } from '@polkadot/rpc-provider/coder';
import defaults from '@polkadot/rpc-provider/defaults';
import type {
  JsonRpcResponse,
  ProviderInterface,
  ProviderInterfaceCallback,
  ProviderInterfaceEmitCb,
  ProviderInterfaceEmitted,
  ProviderStats,
} from '@polkadot/rpc-provider/types';
import { getLogger } from '@subql/node-core';
import { context } from 'fetch-h2';

const ERROR_SUBSCRIBE =
  'HTTP Provider does not have subscriptions, use WebSockets instead';

const l = getLogger('http-provider');

/**
 * Responses that mean "try again later" rather than "this request is wrong": 429 is a rate limit,
 * the others are what a proxy returns while the node behind it is restarting or overloaded.
 */
const RETRYABLE_STATUS = new Set([429, 502, 503, 504]);
export const MAX_ATTEMPTS = 10;
const BASE_DELAY_MS = 500;
// Cap on the exponential backoff between attempts. A server-supplied Retry-After is honoured in
// full up to RETRY_AFTER_CAP_MS instead — retrying sooner than asked can extend a rate-limit ban.
const MAX_DELAY_MS = 30_000;
const RETRY_AFTER_CAP_MS = 5 * 60_000;

export class HttpRequestError extends Error {
  retryAfter: string | null = null;

  constructor(
    readonly status: number,
    statusText: string,
  ) {
    super(`[${status}]: ${statusText}`);
  }
}

/**
 * A failure of the HTTP request itself (DNS, connection refused, reset, socket timeout) as opposed
 * to a response the server actually sent. Retrying one of these can succeed; a JSON-RPC application
 * error in a 200 response (an RpcError from the coder) cannot, so those are never wrapped here.
 */
export class TransportError extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause));
    this.name = 'TransportError';
  }
}

/**
 * Milliseconds to wait before retrying: the Retry-After header when the server sends one (a delay
 * in seconds or an HTTP date), otherwise exponential backoff on the attempt number.
 */
export function retryDelayMs(
  retryAfter: string | null,
  attempt: number,
  now = Date.now(),
): number {
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds) && seconds >= 0) {
      return Math.min(seconds * 1000, RETRY_AFTER_CAP_MS);
    }
    const date = Date.parse(retryAfter);
    if (!Number.isNaN(date)) {
      return Math.min(Math.max(date - now, 0), RETRY_AFTER_CAP_MS);
    }
  }
  return Math.min(BASE_DELAY_MS * 2 ** attempt, MAX_DELAY_MS);
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * # @polkadot/rpc-provider
 *
 * @name HttpProvider
 *
 * @description The HTTP Provider allows sending requests using HTTP to an HTTP RPC server TCP port. It does not support subscriptions so you won't be able to listen to events such as new blocks or balance changes. It is usually preferable using the [[WsProvider]].
 *
 * Responses are never cached: a cached request promise replays a transient failure for as long as
 * it stays cached, so every retry of the same block would hit the cache instead of the node. Rate
 * limited and gateway errors are retried here instead, honouring Retry-After.
 *
 * @example
 * <BR>
 *
 * ```javascript
 * import Api from '@polkadot/api/promise';
 * import { HttpProvider } from '@polkadot/rpc-provider';
 *
 * const provider = new HttpProvider('http://127.0.0.1:9933');
 * const api = new Api(provider);
 * ```
 *
 * @see [[WsProvider]]
 */
export class HttpProvider implements ProviderInterface {
  readonly #coder: RpcCoder;

  readonly #endpoint: string;

  readonly #headers: Record<string, string>;

  readonly #stats: ProviderStats;

  readonly #ctx: ReturnType<typeof context>;

  /**
   * A rate limit applies to the client, not to one request: once the server answers 429 every
   * request in flight is over the limit too, so all of them hold off until this time.
   */
  #pausedUntil = 0;

  /**
   * @param {string} endpoint The endpoint url starting with http://
   */
  constructor(
    endpoint: string = defaults.HTTP_URL,
    headers: Record<string, string> = {},
  ) {
    if (!/^(https|http):\/\//.test(endpoint)) {
      throw new Error(
        `Endpoint should start with 'http://' or 'https://', received '${endpoint}'`,
      );
    }

    this.#ctx = context({
      http1: {
        keepAlive: true,
        maxSockets: 10,
      },
    });

    this.#coder = new RpcCoder();
    this.#endpoint = endpoint;
    this.#headers = headers;
    this.#stats = {
      active: { requests: 0, subscriptions: 0 },
      total: {
        bytesRecv: 0,
        bytesSent: 0,
        cached: 0,
        errors: 0,
        requests: 0,
        subscriptions: 0,
        timeout: 0,
      },
    };
  }

  /**
   * @summary `true` when this provider supports subscriptions
   */
  get hasSubscriptions(): boolean {
    return false;
  }

  /**
   * @description Returns a clone of the object
   */
  clone(): HttpProvider {
    return new HttpProvider(this.#endpoint, this.#headers);
  }

  /**
   * @description Manually connect from the connection
   */
  async connect(): Promise<void> {
    // noop
  }

  /**
   * @description Manually disconnect from the connection
   */
  async disconnect(): Promise<void> {
    // noop
    await this.#ctx.disconnectAll();
  }

  /**
   * @description Returns the connection stats
   */
  get stats(): ProviderStats {
    return this.#stats;
  }

  /**
   * @summary `true` when this provider supports clone()
   */
  get isClonable(): boolean {
    return true;
  }

  /**
   * @summary Whether the node is connected or not.
   * @return {boolean} true if connected
   */
  get isConnected(): boolean {
    return true;
  }

  /**
   * @summary Events are not supported with the HttpProvider, see [[WsProvider]].
   * @description HTTP Provider does not have 'on' emitters. WebSockets should be used instead.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  on(type: ProviderInterfaceEmitted, sub: ProviderInterfaceEmitCb): () => void {
    l.error(
      "HTTP Provider does not have 'on' emitters, use WebSockets instead",
    );

    return (): void => {
      // noop
    };
  }

  /**
   * @summary Send HTTP POST Request with Body to configured HTTP Endpoint.
   */
  async send<T>(
    method: string,
    params: unknown[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    isCacheable?: boolean,
  ): Promise<T> {
    this.#stats.total.requests++;

    const [, body] = this.#coder.encodeJson(method, params);

    return this._send(body);
  }

  async _send<T>(body: string): Promise<T> {
    this.#stats.active.requests++;

    try {
      for (let attempt = 0; ; attempt++) {
        try {
          return await this.#post<T>(body);
        } catch (e) {
          // Retry only what a retry can fix: a retryable HTTP status, or a transport failure.
          // A JSON-RPC application error (RpcError) or a malformed response (SyntaxError) is
          // deterministic — retrying it just blocks the fetch queue and delays the error reaching
          // the connection pool, so those propagate immediately.
          const retryable =
            (e instanceof HttpRequestError && RETRYABLE_STATUS.has(e.status)) ||
            e instanceof TransportError;
          if (!retryable || attempt + 1 >= MAX_ATTEMPTS) {
            throw e;
          }
          const retryAfter =
            e instanceof HttpRequestError ? e.retryAfter : null;
          const delay = retryDelayMs(retryAfter, attempt);
          l.warn(
            `Request to ${this.#endpoint} failed (${(e as Error).message}), retrying in ${delay}ms`,
          );
          await sleep(delay);
        }
      }
    } catch (e) {
      this.#stats.total.errors++;

      throw e;
    } finally {
      this.#stats.active.requests--;
    }
  }

  async #post<T>(body: string): Promise<T> {
    const wait = this.#pausedUntil - Date.now();
    if (wait > 0) {
      await sleep(wait);
    }

    this.#stats.total.bytesSent += body.length;

    let response;
    try {
      response = await this.#ctx.fetch(this.#endpoint, {
        body,
        headers: {
          Accept: 'application/json',
          // Recommend dropped in HTTP2
          // 'Content-Length': `${body.length}`,
          'Content-Type': 'application/json',
          ...this.#headers,
        },
        method: 'POST',
      });
    } catch (e) {
      // Network-level failure (no response). Retryable.
      throw new TransportError(e);
    }

    if (!response.ok) {
      const error = new HttpRequestError(response.status, response.statusText);
      error.retryAfter = response.headers.get('retry-after');
      if (response.status === 429) {
        this.#pausedUntil = Math.max(
          this.#pausedUntil,
          Date.now() + retryDelayMs(error.retryAfter, 0),
        );
      }
      throw error;
    }

    const result = await response.text();

    this.#stats.total.bytesRecv += result.length;

    return this.#coder.decodeResponse(JSON.parse(result) as JsonRpcResponse<T>);
  }

  /**
   * @summary Subscriptions are not supported with the HttpProvider, see [[WsProvider]].
   */
  // eslint-disable-next-line @typescript-eslint/require-await
  async subscribe(
    types: string,
    method: string,
    params: unknown[],
    cb: ProviderInterfaceCallback,
  ): Promise<number> {
    l.error(ERROR_SUBSCRIBE);

    throw new Error(ERROR_SUBSCRIBE);
  }

  /**
   * @summary Subscriptions are not supported with the HttpProvider, see [[WsProvider]].
   */
  // eslint-disable-next-line @typescript-eslint/require-await
  async unsubscribe(
    type: string,
    method: string,
    id: number,
  ): Promise<boolean> {
    l.error(ERROR_SUBSCRIBE);

    throw new Error(ERROR_SUBSCRIBE);
  }
}
