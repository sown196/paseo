import type { Logger } from "pino";
import type { ProviderUsage } from "../../server/messages.js";

export type ProviderApiFetch = typeof fetch;

export interface ProviderUsageFetcher {
  readonly providerId: string;
  readonly displayName: string;
  fetchUsage(): Promise<ProviderUsage>;
  /**
   * Usage for every account this fetcher covers, when one provider can be signed in several
   * times over (Claude, via CLAUDE_CONFIG_DIR). Fetchers that speak for a single account
   * leave this out and the service falls back to `fetchUsage`.
   */
  fetchUsageAll?(): Promise<ProviderUsage[]>;
}

export interface ProviderUsageFetcherFactoryOptions {
  logger: Logger;
  fetch?: ProviderApiFetch;
}

export interface ProviderUsageFetcherManifestEntry {
  readonly providerId: string;
  create(options: ProviderUsageFetcherFactoryOptions): ProviderUsageFetcher;
}
