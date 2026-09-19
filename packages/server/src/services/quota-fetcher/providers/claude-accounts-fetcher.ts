import type { Logger } from "pino";
import type { ProviderUsage } from "../../../server/messages.js";
import type { ProviderApiFetch, ProviderUsageFetcher } from "../provider.js";
import { unavailableUsage } from "../usage.js";
import { ClaudeQuotaProvider } from "./claude.js";
import {
  DEFAULT_CLAUDE_ACCOUNT,
  discoverClaudeAccounts,
  type ClaudeAccount,
} from "./claude-accounts.js";

export interface ClaudeAccountsUsageFetcherOptions {
  logger: Logger;
  fetch?: ProviderApiFetch;
  /** Injection seam for tests; defaults to reading the daemon config. */
  discoverAccounts?: () => ClaudeAccount[];
}

/**
 * Usage for every Claude account the daemon is configured with.
 *
 * Accounts are resolved on each refresh rather than at construction, so adding a config dir
 * shows up on the next poll instead of waiting for a daemon restart. One slow or broken
 * account cannot hide the others: each is settled independently and a failure becomes an
 * unavailable row, matching how the service treats a whole fetcher failing.
 */
export class ClaudeAccountsUsageFetcher implements ProviderUsageFetcher {
  readonly providerId = DEFAULT_CLAUDE_ACCOUNT.providerId;
  readonly displayName = DEFAULT_CLAUDE_ACCOUNT.displayName;

  private readonly logger: Logger;
  private readonly fetchApi: ProviderApiFetch | undefined;
  private readonly discoverAccounts: () => ClaudeAccount[];

  constructor(options: ClaudeAccountsUsageFetcherOptions) {
    this.logger = options.logger;
    this.fetchApi = options.fetch;
    this.discoverAccounts =
      options.discoverAccounts ?? (() => discoverClaudeAccounts({ logger: options.logger }));
  }

  private providerFor(account: ClaudeAccount): ClaudeQuotaProvider {
    return new ClaudeQuotaProvider({
      logger: this.logger,
      fetch: this.fetchApi,
      providerId: account.providerId,
      displayName: account.displayName,
      configDir: account.configDir,
    });
  }

  /** The default account alone. Kept so this fetcher still satisfies the single-row contract. */
  async fetchUsage(): Promise<ProviderUsage> {
    return this.providerFor(DEFAULT_CLAUDE_ACCOUNT).fetchUsage();
  }

  async fetchUsageAll(): Promise<ProviderUsage[]> {
    let accounts: ClaudeAccount[];
    try {
      accounts = this.discoverAccounts();
    } catch (err) {
      this.logger.debug({ err }, "Claude account discovery failed; using the default account");
      accounts = [DEFAULT_CLAUDE_ACCOUNT];
    }

    const settled = await Promise.allSettled(
      accounts.map((account) => this.providerFor(account).fetchUsage()),
    );

    return settled.map((result, index) => {
      const account = accounts[index] ?? DEFAULT_CLAUDE_ACCOUNT;
      if (result.status === "fulfilled") return result.value;
      this.logger.debug(
        { err: result.reason, providerId: account.providerId },
        "Claude account usage fetch failed",
      );
      return unavailableUsage({
        providerId: account.providerId,
        displayName: account.displayName,
        error: result.reason instanceof Error ? result.reason.message : String(result.reason),
      });
    });
  }
}
