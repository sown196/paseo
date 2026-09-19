import type { Logger } from "pino";
import type { ProviderUsage } from "../../server/messages.js";
import { createProviderUsageFetchers } from "./manifest.js";
import type { ProviderApiFetch, ProviderUsageFetcher } from "./provider.js";
import { unavailableUsage } from "./usage.js";

export interface ProviderUsageServiceOptions {
  logger: Logger;
  fetchers?: ProviderUsageFetcher[];
  fetch?: ProviderApiFetch;
  cacheTtlMs?: number;
  now?: () => number;
}

export interface ProviderUsageListResult {
  fetchedAt: string;
  providers: ProviderUsage[];
}

const DEFAULT_PROVIDER_USAGE_CACHE_TTL_MS = 10 * 60 * 1000;

/**
 * How long a usage snapshot is reused, overridable with PASEO_PROVIDER_USAGE_CACHE_TTL_MS.
 *
 * Every refresh costs one call per account, and these endpoints rate-limit per account
 * rather than per client, so several seats sharing one login exhaust the quota together.
 * Ten minutes stays far inside the shortest window any provider reports (five hours):
 * caching longer trades away accuracy the windows themselves do not have.
 */
function resolveDefaultCacheTtlMs(): number {
  // An empty or unparseable value means "unset", not "never cache": reading it as zero
  // would turn every poll into a live call, which is the failure this cache exists to avoid.
  const raw = process.env["PASEO_PROVIDER_USAGE_CACHE_TTL_MS"]?.trim();
  if (!raw) return DEFAULT_PROVIDER_USAGE_CACHE_TTL_MS;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : DEFAULT_PROVIDER_USAGE_CACHE_TTL_MS;
}

export class ProviderUsageService {
  private readonly logger: Logger;
  private readonly fetchers: ProviderUsageFetcher[];
  private readonly cacheTtlMs: number;
  private readonly now: () => number;
  private cached: { fetchedAtMs: number; result: ProviderUsageListResult } | null = null;
  private inFlight: Promise<ProviderUsageListResult> | null = null;

  constructor(options: ProviderUsageServiceOptions) {
    this.logger = options.logger.child({ module: "provider-usage-service" });
    this.fetchers =
      options.fetchers ??
      createProviderUsageFetchers({
        logger: this.logger,
        fetch: options.fetch,
      });
    this.cacheTtlMs = options.cacheTtlMs ?? resolveDefaultCacheTtlMs();
    this.now = options.now ?? Date.now;
  }

  async listUsage(options?: { forceRefresh?: boolean }): Promise<ProviderUsageListResult> {
    const nowMs = this.now();
    if (
      !options?.forceRefresh &&
      this.cached &&
      nowMs - this.cached.fetchedAtMs < this.cacheTtlMs
    ) {
      return this.cached.result;
    }

    if (this.inFlight) {
      return this.inFlight;
    }

    const request = this.fetchFreshUsage(nowMs);
    this.inFlight = request;
    try {
      return await request;
    } finally {
      if (this.inFlight === request) {
        this.inFlight = null;
      }
    }
  }

  private async fetchFreshUsage(nowMs: number): Promise<ProviderUsageListResult> {
    const settled = await Promise.allSettled(
      this.fetchers.map((fetcher) =>
        fetcher.fetchUsageAll
          ? fetcher.fetchUsageAll()
          : fetcher.fetchUsage().then((usage) => [usage]),
      ),
    );
    const providers = settled.flatMap((result, index) => {
      const fetcher = this.fetchers[index];
      if (result.status === "fulfilled") {
        return result.value;
      }
      this.logger.debug(
        { err: result.reason, providerId: fetcher.providerId },
        "Provider usage fetch failed",
      );
      return [
        unavailableUsage({
          providerId: fetcher.providerId,
          displayName: fetcher.displayName,
          error: result.reason instanceof Error ? result.reason.message : String(result.reason),
        }),
      ];
    });

    const result = { fetchedAt: new Date(nowMs).toISOString(), providers };
    this.cached = { fetchedAtMs: nowMs, result };
    return result;
  }
}
