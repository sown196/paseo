import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { claudeKeychainService } from "./claude.js";
import { discoverClaudeAccounts } from "./claude-accounts.js";

const dirs: string[] = [];

function configWith(providers: unknown): string {
  const dir = mkdtempSync(join(tmpdir(), "claude-accounts-"));
  dirs.push(dir);
  const path = join(dir, "config.json");
  writeFileSync(path, JSON.stringify({ agents: { providers } }));
  return path;
}

afterEach(() => {
  while (dirs.length) rmSync(dirs.pop()!, { recursive: true, force: true });
});

describe("claudeKeychainService", () => {
  it("leaves the default account's service name bare", () => {
    expect(claudeKeychainService()).toBe("Claude Code-credentials");
    expect(claudeKeychainService("")).toBe("Claude Code-credentials");
  });

  it("appends the CLI's 8-character config-dir hash", () => {
    // Verified against a real Keychain item written by Claude Code for this config dir.
    expect(claudeKeychainService("/Users/ntq/.claude-max")).toBe(
      "Claude Code-credentials-1824da8c",
    );
  });

  it("hashes ~/.claude too, which is why the default is keyed on the variable being unset", () => {
    expect(claudeKeychainService("/Users/ntq/.claude")).toBe("Claude Code-credentials-51367d56");
    expect(claudeKeychainService()).not.toBe(claudeKeychainService("/Users/ntq/.claude"));
  });
});

describe("discoverClaudeAccounts", () => {
  it("collapses the seats sharing a config dir into one labelled account", () => {
    const path = configWith({
      claude: { enabled: true },
      "claude-max-supervisor": {
        extends: "claude",
        usageLabel: "Claude Max 20x",
        env: { CLAUDE_CONFIG_DIR: "/Users/ntq/.claude-max" },
      },
      "claude-max-lead": {
        extends: "claude",
        env: { CLAUDE_CONFIG_DIR: "/Users/ntq/.claude-max" },
      },
    });

    expect(discoverClaudeAccounts({ configPath: path })).toEqual([
      { providerId: "claude", displayName: "Claude" },
      {
        providerId: "claude-max",
        displayName: "Claude Max 20x",
        configDir: "/Users/ntq/.claude-max",
      },
    ]);
  });

  it("falls back to the directory name when no seat declares a label", () => {
    const path = configWith({
      "claude-alt": { extends: "claude", env: { CLAUDE_CONFIG_DIR: "/home/me/.claude-work" } },
    });
    const [, second] = discoverClaudeAccounts({ configPath: path });
    expect(second).toEqual({
      providerId: "claude-work",
      displayName: "claude work",
      configDir: "/home/me/.claude-work",
    });
  });

  it("ignores disabled seats and non-Claude providers", () => {
    const path = configWith({
      "claude-off": {
        extends: "claude",
        enabled: false,
        env: { CLAUDE_CONFIG_DIR: "/home/me/.claude-off" },
      },
      "codex-lead": { extends: "codex", env: { CLAUDE_CONFIG_DIR: "/home/me/.claude-codex" } },
    });
    expect(discoverClaudeAccounts({ configPath: path })).toEqual([
      { providerId: "claude", displayName: "Claude" },
    ]);
  });

  it("keeps ids unique when two config dirs share a basename", () => {
    const path = configWith({
      a: { extends: "claude", env: { CLAUDE_CONFIG_DIR: "/one/.claude-max" } },
      b: { extends: "claude", env: { CLAUDE_CONFIG_DIR: "/two/.claude-max" } },
    });
    const ids = discoverClaudeAccounts({ configPath: path }).map((a) => a.providerId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids[1]).toBe("claude-max");
    expect(ids[2]).toMatch(/^claude-[0-9a-f]{8}$/);
  });

  it("degrades to the default account when the config cannot be read", () => {
    expect(discoverClaudeAccounts({ configPath: "/nonexistent/config.json" })).toEqual([
      { providerId: "claude", displayName: "Claude" },
    ]);
  });
});
