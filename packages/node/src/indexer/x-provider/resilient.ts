// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import { ProviderInterface } from '@polkadot/rpc-provider/types';
import { getLogger } from '@subql/node-core';

const logger = getLogger('resilient-provider');

/**
 * How long a request waits for the connection to come back before its failure is surfaced.
 *
 * This is the only thing carrying a mapping-handler RPC read across an outage: block *fetches* are
 * retried by the api service (which re-selects a connection each attempt), but a read issued from a
 * handler is not, so if this wait is shorter than the outage the handler throws and the dispatcher
 * exits. It therefore needs to outlast a realistic RPC blip (a node restart can take minutes) for a
 * single-endpoint deployment. The cost is that a multi-endpoint pool does not fail a dead endpoint
 * over to a healthy one until this elapses (the fetch path still fails over via the api-service
 * retry); tune it down with SUBQL_WS_RECONNECT_WAIT_MS when several endpoints are configured.
 */
const DEFAULT_RECONNECT_WAIT_MS = 5 * 60_000;
export const RECONNECT_WAIT_MS = (() => {
  const override = Number(process.env.SUBQL_WS_RECONNECT_WAIT_MS);
  return Number.isFinite(override) && override > 0
    ? override
    : DEFAULT_RECONNECT_WAIT_MS;
})();
const POLL_MS = 500;

const DISCONNECTED = [/^disconnected from /, /WebSocket is not connected/];

export function isDisconnectedError(e: unknown): boolean {
  const message = e instanceof Error ? e.message : String(e);
  return DISCONNECTED.some((pattern) => pattern.test(message));
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Makes a websocket provider outlive a dropped connection, and disables its response cache.
 *
 * Two problems this fixes:
 *  - The provider reconnects on its own, but every request in flight when the socket closes is
 *    rejected with "disconnected from", and any request sent before the socket is back fails
 *    immediately. Both the block fetcher and mapping handlers issue such requests, and the
 *    dispatcher exits the process on any handler failure, so a few seconds of RPC downtime used to
 *    stop indexing. Here a request that fails because the connection is down waits for the provider
 *    to report connected again and is sent again, up to waitMs.
 *  - The provider caches the request promise, a rejected one included, for cacheable calls (any
 *    call naming a block hash), so a retry after a disconnect would get the same cached rejection
 *    back rather than reaching the node. Forcing `isCacheable` false on every send makes the cache
 *    key empty, so the provider always issues a fresh request. This is the reliable disable:
 *    `cacheCapacity: 0` on the constructor is not enough on its own, because the provider's own
 *    capacity-0 bypass checks a field left at the default (1024) even when 0 is requested (true on
 *    both 15.x and 16.x), and its zero-capacity LRU still hands back the most recent entry.
 *
 * Subscription sends (4th arg present) are passed straight through: WsProvider re-subscribes itself
 * on reconnect, so retrying one here would double it, and they are never cacheable.
 */
export function createResilientProvider<
  P extends ProviderInterface = ProviderInterface,
>(provider: P, waitMs = RECONNECT_WAIT_MS, pollMs = POLL_MS): P {
  const originalSend = provider.send.bind(provider);

  (provider as any).send = async (
    method: string,
    params: unknown[],
    isCacheable?: boolean,
    subscription?: unknown,
  ) => {
    if (subscription !== undefined) {
      return (originalSend as any)(method, params, isCacheable, subscription);
    }

    const deadline = Date.now() + waitMs;
    let warned = false;
    for (;;) {
      try {
        // isCacheable forced false: see the note above about 15.x cache semantics.
        return await originalSend(method, params, false);
      } catch (e) {
        if (!isDisconnectedError(e) || Date.now() >= deadline) {
          throw e;
        }
        if (!warned) {
          logger.warn(
            `${method} failed because the connection is down, waiting for it to come back`,
          );
          warned = true;
        }
        do {
          await sleep(pollMs);
          if (Date.now() >= deadline) {
            throw e;
          }
        } while (!provider.isConnected);
      }
    }
  };

  return provider;
}
