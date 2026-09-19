import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import type { Logger } from "pino";

/**
 * One Claude account the daemon can report usage for.
 *
 * An account is a configuration home, not an agent seat: `claude-max-supervisor` and
 * `claude-max-lead` pointing at the same CLAUDE_CONFIG_DIR are one account with one quota,
 * and must produce one usage row rather than four identical ones.
 */
export interface ClaudeAccount {
  /** Usage-row id. `claude` is reserved for the account that runs without CLAUDE_CONFIG_DIR. */
  providerId: string;
  displayName: string;
  /** The account's CLAUDE_CONFIG_DIR, or undefined for the default account. */
  configDir?: string;
}

export const DEFAULT_CLAUDE_ACCOUNT: ClaudeAccount = {
  providerId: "claude",
  displayName: "Claude",
};

interface ProviderEntry {
  extends?: unknown;
  enabled?: unknown;
  usageLabel?: unknown;
  env?: unknown;
}

function paseoConfigPath(): string {
  const home = process.env["PASEO_HOME"];
  return join(home && home.trim() ? home : join(homedir(), ".paseo"), "config.json");
}

/** `.claude-max` -> `claude-max`. Empty when the name yields nothing usable. */
function slugFromConfigDir(configDir: string): string {
  return basename(configDir)
    .replace(/^\.+/, "")
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function hashOfConfigDir(configDir: string): string {
  return createHash("sha256").update(configDir.normalize("NFC")).digest("hex").slice(0, 8);
}

function isClaudeProvider(providerId: string, entry: ProviderEntry): boolean {
  if (entry.enabled === false) return false;
  return providerId === "claude" || entry.extends === "claude";
}

function readProviderEntries(path: string, logger?: Logger): Record<string, ProviderEntry> | null {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as {
      agents?: { providers?: Record<string, ProviderEntry> };
    };
    return parsed.agents?.providers ?? {};
  } catch (err) {
    logger?.debug({ err, path }, "Could not read Claude accounts from daemon config");
    return null;
  }
}

function readUsageLabel(entry: ProviderEntry): string | null {
  if (typeof entry.usageLabel !== "string") return null;
  const trimmed = entry.usageLabel.trim();
  return trimmed ? trimmed : null;
}

/**
 * Config dir -> the first usageLabel any seat pointing at it declares.
 *
 * Seats without a config dir run against the default account, which the caller already has.
 */
function groupSeatsByConfigDir(
  providers: Record<string, ProviderEntry>,
): Map<string, string | null> {
  const byConfigDir = new Map<string, string | null>();

  for (const [providerId, entry] of Object.entries(providers)) {
    if (!entry || typeof entry !== "object" || !isClaudeProvider(providerId, entry)) continue;

    const env = (entry.env ?? {}) as Record<string, unknown>;
    const raw = env["CLAUDE_CONFIG_DIR"];
    const configDir = typeof raw === "string" ? raw.trim() : "";
    if (!configDir) continue;

    byConfigDir.set(configDir, byConfigDir.get(configDir) ?? readUsageLabel(entry));
  }

  return byConfigDir;
}

/**
 * Claude accounts declared in the daemon config.
 *
 * Read straight from disk rather than taken from the parsed config because the usage
 * service is constructed without one. The default account is always included, so a missing
 * or unreadable config degrades to exactly the pre-multi-account behaviour.
 *
 * The CLAUDE_CONFIG_DIR value is used verbatim: Claude Code does not expand `~` either, so
 * a tilde-prefixed value is already broken for the CLI and mirroring that is what keeps the
 * Keychain lookup agreeing with whatever the agent actually wrote.
 */
export function discoverClaudeAccounts(options?: {
  logger?: Logger;
  configPath?: string;
}): ClaudeAccount[] {
  const providers = readProviderEntries(options?.configPath ?? paseoConfigPath(), options?.logger);

  // Always present: a config that names no bare-Claude seat still has ~/.claude sitting
  // there, and dropping it would make this change a regression rather than an addition.
  const accounts: ClaudeAccount[] = [DEFAULT_CLAUDE_ACCOUNT];
  if (!providers) return accounts;

  const taken = new Set([DEFAULT_CLAUDE_ACCOUNT.providerId]);
  for (const [configDir, label] of groupSeatsByConfigDir(providers)) {
    const slug = slugFromConfigDir(configDir);
    const candidate = slug && !taken.has(slug) ? slug : `claude-${hashOfConfigDir(configDir)}`;
    taken.add(candidate);
    accounts.push({
      providerId: candidate,
      displayName: label ?? (slug ? slug.replace(/-/g, " ") : candidate),
      configDir,
    });
  }

  return accounts;
}
