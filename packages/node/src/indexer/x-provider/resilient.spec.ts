// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import { ProviderInterface } from '@polkadot/rpc-provider/types';
import { createResilientProvider, isDisconnectedError } from './resilient';

const DROP = new Error('disconnected from ws://node: 1006:: Abnormal Closure');

function fakeProvider(results: Array<Error | string>) {
  const send = jest.fn(() => {
    const next = results.shift();
    return next instanceof Error ? Promise.reject(next) : Promise.resolve(next);
  });
  const provider = {
    isConnected: true,
    send,
  } as unknown as ProviderInterface & {
    isConnected: boolean;
  };
  return { provider, send };
}

describe('createResilientProvider', () => {
  it('resends a request that failed because the connection dropped once it is back', async () => {
    const { provider, send } = fakeProvider([DROP, DROP, 'ok']);
    provider.isConnected = false;
    const resilient = createResilientProvider(provider, 5000, 10);

    const pending = resilient.send('chain_getHeader', []);
    setTimeout(() => (provider.isConnected = true), 50);

    await expect(pending).resolves.toBe('ok');
    expect(send).toHaveBeenCalledTimes(3);
  });

  it('surfaces the original error once the wait runs out', async () => {
    const { provider } = fakeProvider([DROP, DROP, DROP, DROP, DROP, DROP]);
    provider.isConnected = false;
    const resilient = createResilientProvider(provider, 60, 10);

    await expect(resilient.send('chain_getHeader', [])).rejects.toBe(DROP);
  });

  it('does not retry errors that are not about the connection', async () => {
    const bad = new Error('-32602: Invalid params');
    const { provider, send } = fakeProvider([bad, 'ok']);
    const resilient = createResilientProvider(provider, 5000, 10);

    await expect(resilient.send('chain_getHeader', [])).rejects.toBe(bad);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('forces isCacheable off on every send, so the provider never caches a response', async () => {
    // On @polkadot/api 15.x cacheCapacity:0 does not disable the cache; forcing isCacheable false
    // is the only way to take the uncached path, so a rejected promise is never replayed.
    const { provider, send } = fakeProvider(['ok']);
    const resilient = createResilientProvider(provider, 5000, 10);

    await resilient.send('state_getRuntimeVersion', ['0xhash'], true);

    expect(send).toHaveBeenCalledWith(
      'state_getRuntimeVersion',
      ['0xhash'],
      false,
    );
  });

  it('passes subscription sends straight through and does not retry them', async () => {
    const { provider, send } = fakeProvider([DROP]);
    provider.isConnected = false;
    const resilient = createResilientProvider(provider, 5000, 10);
    const cb = () => undefined;

    await expect(
      (resilient.send as any)('chain_subscribeNewHeads', [], false, cb),
    ).rejects.toBe(DROP);
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith('chain_subscribeNewHeads', [], false, cb);
  });

  it('recognises the errors polkadot-js raises for a down socket', () => {
    expect(isDisconnectedError(DROP)).toBe(true);
    expect(isDisconnectedError(new Error('WebSocket is not connected'))).toBe(
      true,
    );
    expect(isDisconnectedError(new Error('[429]: Too Many Requests'))).toBe(
      false,
    );
  });
});
