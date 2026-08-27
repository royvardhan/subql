// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import http from 'http';
import { AddressInfo } from 'net';
import { HttpProvider, MAX_ATTEMPTS, retryDelayMs } from './http';

type Reply = {
  status: number;
  headers?: Record<string, string>;
  rpcError?: { code: number; message: string };
};

/** A JSON-RPC server that answers each request from a script of statuses, then 200 with a fresh result. */
async function rpcServer(script: Reply[]) {
  const seen: string[] = [];
  const times: number[] = [];
  const start = Date.now();
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (chunk) => (raw += chunk));
    req.on('end', () => {
      seen.push(raw);
      times.push(Date.now() - start);
      const reply = script.shift() ?? { status: 200 };
      if (reply.status !== 200) {
        res.writeHead(reply.status, reply.headers ?? {});
        res.end();
        return;
      }
      const { id } = JSON.parse(raw);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        reply.rpcError
          ? JSON.stringify({ jsonrpc: '2.0', id, error: reply.rpcError })
          : JSON.stringify({ jsonrpc: '2.0', id, result: `0x${seen.length}` }),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    seen,
    times,
    url: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

describe('HttpProvider', () => {
  it('retries a rate limited request after Retry-After and returns the eventual result', async () => {
    const server = await rpcServer([
      { status: 429, headers: { 'retry-after': '0' } },
      { status: 429, headers: { 'retry-after': '0' } },
    ]);
    const provider = new HttpProvider(server.url);

    const result = await provider.send('chain_getHeader', []);

    expect(result).toBe('0x3');
    expect(server.seen).toHaveLength(3);
    await provider.disconnect();
    await server.close();
  });

  it('holds a later request back once the client is rate limited', async () => {
    const server = await rpcServer([
      { status: 429, headers: { 'retry-after': '1' } },
    ]);
    const provider = new HttpProvider(server.url);

    const first = provider.send('chain_getHeader', []);
    await new Promise((resolve) => setTimeout(resolve, 20));
    const second = provider.send('chain_getBlockHash', [1]);
    await Promise.all([first, second]);

    // Three hits: the 429, the first's retry, and the second. The second is sent ~20ms in but is
    // held by the client-wide pause, so its hit lands well after the ~1s Retry-After rather than
    // at ~20ms. Asserting only that it was held past half the window keeps this off wall-clock
    // precision. times[0] is the 429; the second request's hit is the last one.
    expect(server.seen).toHaveLength(3);
    expect(server.times[server.times.length - 1]).toBeGreaterThanOrEqual(500);
    await provider.disconnect();
    await server.close();
  });

  it('does not retry a JSON-RPC application error returned with HTTP 200', async () => {
    const server = await rpcServer([
      { status: 200, rpcError: { code: -32602, message: 'Invalid params' } },
    ]);
    const provider = new HttpProvider(server.url);

    await expect(provider.send('state_call', [])).rejects.toThrow(
      'Invalid params',
    );
    expect(server.seen).toHaveLength(1);
    await provider.disconnect();
    await server.close();
  });

  it('does not cache: identical requests each reach the node', async () => {
    const server = await rpcServer([]);
    const provider = new HttpProvider(server.url);

    const first = await provider.send(
      'state_getRuntimeVersion',
      ['0xabc'],
      true,
    );
    const second = await provider.send(
      'state_getRuntimeVersion',
      ['0xabc'],
      true,
    );

    expect(first).toBe('0x1');
    expect(second).toBe('0x2');
    expect(server.seen).toHaveLength(2);
    await provider.disconnect();
    await server.close();
  });

  it('gives up after the maximum number of attempts with the status in the error', async () => {
    const server = await rpcServer(
      Array.from({ length: MAX_ATTEMPTS + 1 }, () => ({
        status: 429,
        headers: { 'retry-after': '0' },
      })),
    );
    const provider = new HttpProvider(server.url);

    await expect(provider.send('chain_getHeader', [])).rejects.toThrow('[429]');
    expect(server.seen).toHaveLength(MAX_ATTEMPTS);
    await provider.disconnect();
    await server.close();
  });

  it('does not retry a request the node rejected outright', async () => {
    const server = await rpcServer([{ status: 400 }]);
    const provider = new HttpProvider(server.url);

    await expect(provider.send('chain_getHeader', [])).rejects.toThrow('[400]');
    expect(server.seen).toHaveLength(1);
    await provider.disconnect();
    await server.close();
  });
});

describe('retryDelayMs', () => {
  it('uses Retry-After in seconds', () => {
    expect(retryDelayMs('2', 0)).toBe(2000);
  });

  it('uses Retry-After as an HTTP date relative to now', () => {
    const now = Date.parse('Wed, 21 Oct 2015 07:28:00 GMT');
    expect(retryDelayMs('Wed, 21 Oct 2015 07:28:05 GMT', 0, now)).toBe(5000);
    expect(retryDelayMs('Wed, 21 Oct 2015 07:27:00 GMT', 0, now)).toBe(0);
  });

  it('backs off exponentially without a header and caps the fallback wait at 30s', () => {
    expect(retryDelayMs(null, 0)).toBe(500);
    expect(retryDelayMs(null, 3)).toBe(4000);
    expect(retryDelayMs(null, 20)).toBe(30_000);
  });

  it('honours Retry-After beyond the fallback cap, up to five minutes', () => {
    expect(retryDelayMs('60', 0)).toBe(60_000);
    expect(retryDelayMs('3600', 0)).toBe(5 * 60_000);
  });
});
