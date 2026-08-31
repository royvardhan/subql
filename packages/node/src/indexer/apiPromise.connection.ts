// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import { ApiPromise, WsProvider } from '@polkadot/api';
import { ApiOptions } from '@polkadot/api/types';
import { ProviderInterface } from '@polkadot/rpc-provider/types';
import { RegisteredTypes } from '@polkadot/types/types';
import {
  ApiConnectionError,
  ApiErrorType,
  DisconnectionError,
  LargeResponseError,
  NetworkMetadataPayload,
  RateLimitError,
  TimeoutError,
  IApiConnectionSpecific,
  IBlock,
} from '@subql/node-core';
import { IEndpointConfig } from '@subql/types-core';
import * as SubstrateUtil from '../utils/substrate';
import { ApiAt, BlockContent, LightBlockContent } from './types';
import { HttpProvider } from './x-provider/http';
import { createResilientProvider } from './x-provider/resilient';

const { version: packageVersion } = require('../../package.json');

const RETRY_DELAY = 2_500;
const CONNECT_TIMEOUT_MS = 30_000;

export type FetchFunc =
  | typeof SubstrateUtil.fetchBlocksBatches
  | typeof SubstrateUtil.fetchBlocksBatchesLight;

// We use a function to get the fetch function because it can change depending on the skipTransactions feature
export type GetFetchFunc = () => FetchFunc;

export class ApiPromiseConnection
  implements
    IApiConnectionSpecific<
      ApiPromise,
      ApiAt,
      IBlock<BlockContent>[] | IBlock<LightBlockContent>[]
    >
{
  readonly networkMeta: NetworkMetadataPayload;

  private constructor(
    public unsafeApi: ApiPromise,
    private fetchBlocksBatches: GetFetchFunc,
    private endpoint: string,
  ) {
    this.networkMeta = {
      chain: unsafeApi.runtimeChain.toString(),
      specName: unsafeApi.runtimeVersion.specName.toString(),
      genesisHash: unsafeApi.genesisHash.toString(),
    };
  }

  static async create(
    endpoint: string,
    fetchBlocksBatches: GetFetchFunc,
    args: { chainTypes?: RegisteredTypes },
    config: IEndpointConfig,
  ): Promise<ApiPromiseConnection> {
    let provider: ProviderInterface;
    let throwOnConnect = false;

    const headers = {
      'User-Agent': `SubQuery-Node ${packageVersion}`,
      ...config.headers,
    };

    // Neither provider caches responses (WsProvider cache capacity 0, HttpProvider has none).
    // polkadot-js caches the request promise, a rejected one included, keyed by the request, so
    // after a disconnect every retry of the same block replayed the stale "disconnected" error
    // from memory instead of asking the node again and the endpoint never recovered.
    if (endpoint.startsWith('ws')) {
      provider = createResilientProvider(
        new WsProvider(endpoint, RETRY_DELAY, headers, undefined, 0),
      );
    } else if (endpoint.startsWith('http')) {
      provider = new HttpProvider(endpoint, headers);
      throwOnConnect = true;
    } else {
      throw new Error(`Invalid endpoint: ${endpoint}`);
    }

    const apiOption = {
      provider,
      throwOnConnect,
      noInitWarn: true,
      ...args.chainTypes,
    };
    const api = await ApiPromise.create(apiOption);
    return new ApiPromiseConnection(api, fetchBlocksBatches, endpoint);
  }

  safeApi(height: number): ApiAt {
    throw new Error(`Not Implemented`);
  }

  async fetchBlocks(
    heights: number[],
    overallSpecVer?: number,
  ): Promise<IBlock<BlockContent>[] | IBlock<LightBlockContent>[]> {
    const blocks = await this.fetchBlocksBatches()(
      this.unsafeApi,
      heights,
      overallSpecVer,
    );
    return blocks;
  }

  /**
   * Resolves once the provider reports connected, or rejects after CONNECT_TIMEOUT_MS so the
   * caller can back off and try again. The websocket provider reconnects on its own after a
   * drop, so connect() may throw "already connected" while that is in flight; the connected
   * event still fires and is what this waits for.
   */
  async apiConnect(): Promise<void> {
    if (this.unsafeApi.isConnected) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const onConnected = () => {
        cleanup();
        resolve();
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(
          new Error(
            `Timed out after ${CONNECT_TIMEOUT_MS}ms connecting to ${this.endpoint}`,
          ),
        );
      }, CONNECT_TIMEOUT_MS);
      const cleanup = () => {
        clearTimeout(timer);
        this.unsafeApi.off('connected', onConnected);
      };

      this.unsafeApi.on('connected', onConnected);
      this.unsafeApi.connect().catch(() => undefined);
      if (this.unsafeApi.isConnected) {
        onConnected();
      }
    });
  }

  async apiDisconnect(): Promise<void> {
    await this.unsafeApi.disconnect();
  }

  async updateChainTypes(chainTypes: RegisteredTypes): Promise<void> {
    // Typeof Decorate<'promise' | 'rxjs'>, but we need to access this private method
    const currentApiOptions = (this.unsafeApi as any)._options as ApiOptions;
    const apiOption = {
      ...currentApiOptions,
      ...chainTypes,
    };
    this.unsafeApi = await ApiPromise.create(apiOption);
  }

  handleError = ApiPromiseConnection.handleError;

  static handleError(e: Error): ApiConnectionError {
    let formatted_error: ApiConnectionError;
    if (e.message.startsWith(`No response received from RPC endpoint in`)) {
      formatted_error = new TimeoutError(e);
    } else if (e.message.startsWith(`disconnected from `)) {
      formatted_error = new DisconnectionError(e);
    } else if (
      e.message.startsWith(`-32029: Too Many Requests`) ||
      e.message.startsWith(`[429]`)
    ) {
      formatted_error = new RateLimitError(e);
    } else if (e.message.includes(`Exceeded max limit of`)) {
      formatted_error = new LargeResponseError(e);
    } else {
      formatted_error = new ApiConnectionError(
        e.name,
        e.message,
        ApiErrorType.Default,
      );
    }
    return formatted_error;
  }
}
