import type { Logger } from "pino";

import type {
  AgentPermissionRequest,
  AgentPromptInput,
  AgentRunOptions,
} from "./agent-sdk-types.js";
import type { AgentManager, ManagedAgent } from "./agent-manager.js";
import type { AgentStorage } from "./agent-storage.js";
import { ensureAgentLoaded } from "./agent-loading.js";
import { isStaleProviderSessionError } from "./stale-provider-session-error.js";
import { getParentAgentIdFromLabels } from "@getpaseo/protocol/agent-labels";
import type { ActiveTurnBehavior } from "@getpaseo/protocol/messages";

export type AgentUnarchiveController = Pick<AgentManager, "notifyAgentState" | "unarchiveSnapshot">;

export type AgentRunController = Pick<
  AgentManager,
  | "getAgent"
  | "tryRunOutOfBand"
  | "hasInFlightRun"
  | "replaceAgentRun"
  | "steerOrReplaceActiveTurn"
  | "streamAgent"
> & {
  reloadAgentSession(agentId: string): Promise<unknown>;
};

export interface StartAgentRunOptions {
  replaceRunning?: boolean;
  activeTurnBehavior?: ActiveTurnBehavior;
  runOptions?: AgentRunOptions;
  /** Ask the provider to deny permissions blocking this steer. */
  clearPendingPermissions?: boolean;
}

export type PromptDispatchDisposition = "out_of_band" | "steered" | "turn_started" | "skipped";

async function steerOrReplaceActiveRun(
  agentManager: AgentRunController,
  agentId: string,
  prompt: AgentPromptInput,
  options: StartAgentRunOptions | undefined,
): Promise<
  | { disposition: "steered" }
  | {
      disposition: "turn_started";
      iterator: AsyncGenerator<import("./agent-sdk-types.js").AgentStreamEvent>;
    }
  | null
> {
  if (options?.activeTurnBehavior !== "steer") {
    return null;
  }
  const steerOptions = options.clearPendingPermissions
    ? { ...options.runOptions, clearPendingPermissions: true }
    : options.runOptions;
  const result = await agentManager.steerOrReplaceActiveTurn(agentId, prompt, steerOptions);
  if (result.status === "steered") {
    return { disposition: "steered" };
  }
  if (result.status === "replaced") {
    return { disposition: "turn_started", iterator: result.iterator };
  }
  return null;
}

async function startOrReplaceRun(
  agentManager: AgentRunController,
  agentId: string,
  prompt: AgentPromptInput,
  options: StartAgentRunOptions | undefined,
): Promise<{
  iterator: AsyncGenerator<import("./agent-sdk-types.js").AgentStreamEvent>;
  replaced: boolean;
}> {
  const replaced = Boolean(options?.replaceRunning && agentManager.hasInFlightRun(agentId));
  const iterator = replaced
    ? await agentManager.replaceAgentRun(agentId, prompt, options?.runOptions)
    : agentManager.streamAgent(agentId, prompt, options?.runOptions);
  return { iterator, replaced };
}

async function drainAgentRunIterator(
  iterator: AsyncGenerator<import("./agent-sdk-types.js").AgentStreamEvent>,
): Promise<void> {
  for await (const _ of iterator) {
    // Events are broadcast via AgentManager subscribers.
  }
}

export async function startAgentRun(
  agentManager: AgentRunController,
  agentId: string,
  prompt: AgentPromptInput,
  logger: Logger,
  options?: StartAgentRunOptions,
): Promise<{ disposition: PromptDispatchDisposition }> {
  const snapshot = agentManager.getAgent(agentId);
  logger.trace(
    {
      agentId,
      provider: snapshot?.provider,
      providerSessionId: snapshot?.persistence?.sessionId ?? undefined,
      turnId: snapshot?.activeForegroundTurnId ?? undefined,
      promptType: typeof prompt === "string" ? "string" : "structured",
      hasRunOptions: Boolean(options?.runOptions),
      replaceRunning: Boolean(options?.replaceRunning),
    },
    "agent.session.start_stream.request",
  );
  // Out-of-band commands (e.g. /goal pause) must run WITHOUT canceling an
  // in-flight turn — replaceAgentRun would interrupt the running turn. The
  // intercept lives at this layer so it covers every prompt entrypoint.
  if (agentManager.tryRunOutOfBand(agentId, prompt, options?.runOptions)) {
    return { disposition: "out_of_band" };
  }
  try {
    return await startAgentRunInner(agentManager, agentId, prompt, logger, options);
  } catch (error) {
    if (!isStaleProviderSessionError(error)) throw error;
    logger.info({ agentId, err: error }, "Provider session went stale; reopening from persistence");
    // The live session belongs to a retired plugin runtime. Reload swaps in a
    // fresh session on the current runtime while preserving history and labels.
    await agentManager.reloadAgentSession(agentId);
    return await startAgentRunInner(agentManager, agentId, prompt, logger, options);
  }
}

async function startAgentRunInner(
  agentManager: AgentRunController,
  agentId: string,
  prompt: AgentPromptInput,
  logger: Logger,
  options?: StartAgentRunOptions,
): Promise<{ disposition: PromptDispatchDisposition }> {
  const snapshot = agentManager.getAgent(agentId);
  const steered = await steerOrReplaceActiveRun(agentManager, agentId, prompt, options);
  if (steered?.disposition === "steered") {
    return steered;
  }
  const { iterator, replaced } = steered
    ? { iterator: steered.iterator, replaced: true }
    : await startOrReplaceRun(agentManager, agentId, prompt, options);
  logger.trace(
    {
      agentId,
      provider: snapshot?.provider,
      providerSessionId: snapshot?.persistence?.sessionId ?? undefined,
      shouldReplace: replaced,
    },
    "agent.session.start_stream.iterator_returned",
  );
  void (async () => {
    try {
      try {
        await drainAgentRunIterator(iterator);
      } catch (error) {
        if (!isStaleProviderSessionError(error)) throw error;
        logger.info(
          { agentId, err: error },
          "Provider session went stale; reopening from persistence",
        );
        await agentManager.reloadAgentSession(agentId);
        const retry = await startOrReplaceRun(agentManager, agentId, prompt, options);
        await drainAgentRunIterator(retry.iterator);
      }
      logger.trace(
        {
          agentId,
          provider: snapshot?.provider,
          providerSessionId: snapshot?.persistence?.sessionId ?? undefined,
        },
        "agent.session.iterator.drained",
      );
    } catch (error) {
      logger.trace(
        {
          agentId,
          provider: snapshot?.provider,
          providerSessionId: snapshot?.persistence?.sessionId ?? undefined,
          err: error,
        },
        "agent.session.iterator.error",
      );
      logger.error({ err: error, agentId }, "Agent stream failed");
    }
  })();
  return { disposition: "turn_started" };
}

/**
 * Clear the archived flag from a stored agent record.
 * Shared across Session (app/WS), MCP, and CLI so every surface that acts on
 * an archived agent unarchives it the same way.
 */
export async function unarchiveAgentState(
  _agentStorage: AgentStorage,
  agentManager: AgentUnarchiveController,
  agentId: string,
  updates?: { workspaceId?: string; labels?: Record<string, string | null> },
): Promise<boolean> {
  const unarchived = await agentManager.unarchiveSnapshot(agentId, updates);
  if (!unarchived) return false;
  agentManager.notifyAgentState(agentId);
  return true;
}

/**
 * Wrap a body in <paseo-system>…</paseo-system> so the receiving agent
 * recognizes the prompt as system-injected context — not a user turn.
 * Used by chat mentions, schedule fires, and notify-on-finish.
 */
export function formatSystemNotificationPrompt(reason: string): string {
  return `<paseo-system>\n${reason}\n</paseo-system>`;
}

const SYSTEM_ENVELOPE_PATTERN = /^<paseo-system>\n[\s\S]*\n<\/paseo-system>$/;

export function isSystemInjectedEnvelope(text: string): boolean {
  return SYSTEM_ENVELOPE_PATTERN.test(text);
}

export interface SendPromptToAgentParams {
  agentManager: AgentManager;
  agentStorage: AgentStorage;
  agentId: string;
  /** Prompt to dispatch to the provider (may include image blocks or wrapped text). */
  prompt: AgentPromptInput;
  messageId?: string;
  activeTurnBehavior?: ActiveTurnBehavior;
  runOptions?: AgentRunOptions;
  /** Optional mode to set on the agent before the run starts. */
  sessionMode?: string;
  /**
   * Default true. When false, archived agents are skipped instead of being
   * unarchived. Use false for system-injected prompts (chat mentions,
   * schedule fires, notify-on-finish).
   */
  unarchive?: boolean;
  /** See {@link StartAgentRunOptions.clearPendingPermissions}. */
  clearPendingPermissions?: boolean;
  logger: Logger;
}

export interface StartCreatedAgentInitialPromptParams {
  agentManager: AgentManager;
  agentId: string;
  snapshot?: ManagedAgent;
  prompt: AgentPromptInput | null;
  runOptions?: AgentRunOptions;
  logger: Logger;
}

/**
 * Outer bound on a run reaching "started" after dispatch.
 *
 * This wraps provider startup, so it MUST stay larger than the slowest provider's own
 * startup budget — otherwise it aborts a start the provider was still allowed to be
 * working on, and the provider's budget can never apply. OpenCode is the slowest today:
 * up to 30s for the server to boot (OPENCODE_SERVER_STARTUP_TIMEOUT_MS) and then a
 * session.create on the same budget, so this is deliberately set well above 30s.
 *
 * Not derived from the provider constant on purpose: this module is provider-agnostic
 * and must not depend on a specific provider's internals.
 */
const AGENT_RUN_START_TIMEOUT_MS = 60_000;

export async function waitForAgentRunStartWithTimeout(
  agentManager: AgentManager,
  agentId: string,
  signal?: AbortSignal,
): Promise<void> {
  const provider = agentManager.getAgent(agentId)?.provider ?? "provider";
  const startAbort = new AbortController();
  const startTimeout = setTimeout(
    () =>
      startAbort.abort(
        new Error(
          `${provider} run did not start within ${AGENT_RUN_START_TIMEOUT_MS / 1000} seconds (phase: run start)`,
        ),
      ),
    AGENT_RUN_START_TIMEOUT_MS,
  );

  try {
    await agentManager.waitForAgentRunStart(agentId, {
      signal: signal ? AbortSignal.any([startAbort.signal, signal]) : startAbort.signal,
    });
  } finally {
    clearTimeout(startTimeout);
  }
}

/**
 * Full send-prompt orchestration: (optional unarchive) → load → (optional
 * mode change) → start run.
 *
 * Every surface that sends a prompt to an agent (Session/WS, MCP, CLI-through-MCP,
 * chat mentions, notify-on-finish) MUST go through this so behavior can never
 * drift between them.
 *
 * When `unarchive` is false and the agent is archived, the call is a no-op
 * and reports disposition "skipped" — the agent is not run.
 */
export async function sendPromptToAgent(
  params: SendPromptToAgentParams,
): Promise<{ disposition: PromptDispatchDisposition }> {
  const unarchive = params.unarchive ?? true;

  const record = await params.agentStorage.get(params.agentId);
  if (record?.archivedAt) {
    if (!unarchive) {
      return { disposition: "skipped" };
    }
    await unarchiveAgentState(params.agentStorage, params.agentManager, params.agentId);
  }

  await ensureAgentLoaded(params.agentId, {
    agentManager: params.agentManager,
    agentStorage: params.agentStorage,
    logger: params.logger,
  });

  if (params.sessionMode) {
    await params.agentManager.setAgentMode(params.agentId, params.sessionMode);
  }

  const runOptions = params.messageId
    ? { ...params.runOptions, clientMessageId: params.messageId }
    : params.runOptions;

  return await startAgentRun(params.agentManager, params.agentId, params.prompt, params.logger, {
    replaceRunning: true,
    activeTurnBehavior: params.activeTurnBehavior,
    clearPendingPermissions: params.clearPendingPermissions,
    runOptions,
  });
}

export async function startCreatedAgentInitialPrompt(
  params: StartCreatedAgentInitialPromptParams,
): Promise<ManagedAgent> {
  const currentSnapshot = params.agentManager.getAgent(params.agentId) ?? params.snapshot ?? null;
  if (!currentSnapshot) {
    throw new Error(`Agent ${params.agentId} not found`);
  }

  if (params.prompt === null) {
    return currentSnapshot;
  }

  const dispatchResult = await startAgentRun(
    params.agentManager,
    params.agentId,
    params.prompt,
    params.logger,
    {
      runOptions: params.runOptions,
    },
  );

  if (dispatchResult.disposition === "turn_started") {
    await waitForAgentRunStartWithTimeout(params.agentManager, params.agentId);
  }

  const refreshedSnapshot = params.agentManager.getAgent(params.agentId) ?? params.snapshot ?? null;
  if (!refreshedSnapshot) {
    throw new Error(`Agent ${params.agentId} not found`);
  }
  return refreshedSnapshot;
}

export interface SetupFinishNotificationParams {
  agentManager: AgentManager;
  agentStorage: AgentStorage;
  childAgentId: string;
  callerAgentId: string;
  requireParentOwnership?: boolean;
  /**
   * When true the subscription survives the child's finishes and errors: the
   * caller is woken for every later turn until the child closes or the
   * notify-on-finish label is cleared. When undefined, an existing
   * subscription for the same (child, caller) keeps its persistence;
   * otherwise defaults to false (one finish, then done).
   */
  persistent?: boolean;
  /**
   * Allow subscribing to a child that has no live session yet (boot resume,
   * label edits). The subscription waits for the child to load and run.
   */
  allowUnloaded?: boolean;
  logger: Logger;
}

type FinishNotificationReason =
  | "finished"
  | "errored"
  | "needs permission"
  | "was closed"
  | "was interrupted by a daemon restart"
  | "ended before a daemon restart could deliver its notification";

const FINISH_NOTIFICATION_MESSAGE_LIMIT = 4000;

interface FinishNotificationBodyInput {
  childAgentId: string;
  title: string;
  reason: FinishNotificationReason;
  lastAssistantMessage: string | null;
  permissionRequest?: AgentPermissionRequest;
}

function formatFinishNotificationBody(params: FinishNotificationBodyInput): string {
  const statusLine = `Agent ${params.childAgentId} (${params.title}) ${params.reason}.`;
  const sections = [statusLine];
  if (params.reason === "needs permission" && params.permissionRequest) {
    sections.push(
      "Respond with `respond_to_permission` using the `agentId` and `requestId` below.",
      `<permission-request>\n${JSON.stringify(
        {
          agentId: params.childAgentId,
          requestId: params.permissionRequest.id,
          request: params.permissionRequest,
        },
        null,
        2,
      )}\n</permission-request>`,
    );
  }
  let lastAssistantMessage = params.lastAssistantMessage?.trim();
  if (lastAssistantMessage) {
    if (lastAssistantMessage.length > FINISH_NOTIFICATION_MESSAGE_LIMIT) {
      const omitted = lastAssistantMessage.length - FINISH_NOTIFICATION_MESSAGE_LIMIT;
      lastAssistantMessage = `${lastAssistantMessage.slice(0, FINISH_NOTIFICATION_MESSAGE_LIMIT)}\n[truncated ${omitted} chars; use get_agent_activity for the full response]`;
    }
    sections.push(`<agent-response>\n${lastAssistantMessage}\n</agent-response>`);
  }
  return sections.join("\n\n");
}

interface NotifySafelyOptions {
  terminal?: boolean;
  permissionRequest?: AgentPermissionRequest;
}

type FinishNotificationPolicyReader = Pick<
  AgentManager,
  "getAgent" | "resolveProviderPaseoToolPolicy"
>;

/**
 * Whether the caller's provider takes finish notifications at all. Observer
 * seats (paseoTools.finishNotifications: false) watch agents through status
 * polling, so a prompt they send must not wake them on every later turn.
 * `callerProvider` covers callers that are not loaded (boot catch-up).
 */
export function callerAcceptsFinishNotifications(params: {
  agentManager: FinishNotificationPolicyReader;
  callerAgentId: string;
  callerProvider?: string | null;
}): boolean {
  const provider =
    params.agentManager.getAgent(params.callerAgentId)?.provider ?? params.callerProvider;
  if (!provider) return true;
  return (
    params.agentManager.resolveProviderPaseoToolPolicy(provider)?.finishNotifications !== false
  );
}

/**
 * Persisted opt-in for finish notifications, stored on the child record so
 * subscriptions survive daemon restarts. Value is `once` or `always`,
 * optionally suffixed with the caller id (`always:<agentId>`). Without a
 * caller suffix the parent from `paseo.parent-agent-id` is notified.
 */
export const NOTIFY_ON_FINISH_LABEL = "paseo.notify-on-finish";

export interface ParsedNotifyOnFinishLabel {
  mode: "once" | "always";
  callerAgentId: string | null;
}

export function parseNotifyOnFinishLabel(
  labels: Record<string, unknown> | null | undefined,
): ParsedNotifyOnFinishLabel | null {
  const raw = labels?.[NOTIFY_ON_FINISH_LABEL];
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (!trimmed || trimmed === "off" || trimmed === "false" || trimmed === "0") return null;
  const separator = trimmed.indexOf(":");
  const mode = separator === -1 ? trimmed : trimmed.slice(0, separator);
  const caller = separator === -1 ? "" : trimmed.slice(separator + 1).trim();
  return {
    mode: mode === "always" ? "always" : "once",
    callerAgentId: caller.length > 0 ? caller : null,
  };
}

// Serialize label writes per child so a setup-time write can never land after
// (and resurrect) a terminal clear.
const notifyLabelWriteChains = new Map<string, Promise<void>>();

interface PersistNotifyOnFinishLabelParams {
  agentManager: AgentManager;
  agentStorage: AgentStorage;
  childAgentId: string;
  value: string | null;
  /**
   * When clearing (`value: null`), only clear if the label still holds this
   * value. The label is a single slot per child, so a retiring subscription
   * must not wipe a value a newer subscription has written since.
   */
  expectedValue?: string;
  logger: Logger;
}

/**
 * Best-effort label write; a failure must never break notification delivery.
 * Live agents go through the manager so the in-memory snapshot stays in sync;
 * unloaded agents are patched directly in storage. Clearing writes "" on the
 * live path (setLabels cannot delete) — the parser treats "" as absent.
 * Unchanged values are skipped so routine re-subscribes cost no write.
 */
async function persistNotifyOnFinishLabel(params: PersistNotifyOnFinishLabelParams): Promise<void> {
  const previous = notifyLabelWriteChains.get(params.childAgentId) ?? Promise.resolve();
  const write = previous.then(() => doPersistNotifyOnFinishLabel(params));
  notifyLabelWriteChains.set(params.childAgentId, write);
  await write;
  if (notifyLabelWriteChains.get(params.childAgentId) === write) {
    notifyLabelWriteChains.delete(params.childAgentId);
  }
}

async function doPersistNotifyOnFinishLabel(
  params: PersistNotifyOnFinishLabelParams,
): Promise<void> {
  try {
    const liveAgent = params.agentManager.getAgent(params.childAgentId);
    const record = liveAgent ? null : await params.agentStorage.get(params.childAgentId);
    if (!liveAgent && !record) return;
    const current = (liveAgent ? liveAgent.labels : record?.labels)?.[NOTIFY_ON_FINISH_LABEL] ?? "";

    if (params.value === null) {
      if (current === "") return;
      if (params.expectedValue !== undefined && current !== params.expectedValue) return;
    } else if (current === params.value) {
      return;
    }

    if (liveAgent) {
      await params.agentManager.setLabels(params.childAgentId, {
        [NOTIFY_ON_FINISH_LABEL]: params.value ?? "",
      });
      return;
    }
    if (!record) return;
    const labels = { ...record.labels };
    if (params.value === null) {
      delete labels[NOTIFY_ON_FINISH_LABEL];
    } else {
      labels[NOTIFY_ON_FINISH_LABEL] = params.value;
    }
    await params.agentStorage.upsert({ ...record, labels });
  } catch (error) {
    params.logger.warn(
      { err: error, childAgentId: params.childAgentId },
      "Failed to persist notify-on-finish label",
    );
  }
}

type CallerNotificationReason = FinishNotificationReason | "missed notifications";

/**
 * Terminal notifications retire their persisted label only once actually
 * delivered (or abandoned) — clearing at event time would drop a queued
 * notification on a daemon restart. `expectedValue` guards the compare-and-
 * clear so a newer subscription's label survives.
 */
interface NotifyLabelClear {
  childAgentId: string;
  expectedValue: string;
}

interface PendingCallerNotification {
  agentStorage: AgentStorage;
  childAgentId: string;
  callerAgentId: string;
  reason: CallerNotificationReason;
  /** Unwrapped body; wrapped (and possibly coalesced) at dispatch time. */
  body: string;
  logger: Logger;
  attempts: number;
  expiresAt: number;
  waitingForIdle: boolean;
  /** Missed-notification summaries are never re-summarized when they expire. */
  synthetic?: boolean;
  labelClears?: NotifyLabelClear[];
}

interface MissedCallerNotification {
  agentStorage: AgentStorage;
  childAgentId: string;
  callerAgentId: string;
  reason: CallerNotificationReason;
  logger: Logger;
  labelClears?: NotifyLabelClear[];
}

interface CallerNotificationQueue {
  agentManager: AgentManager;
  callerAgentId: string;
  notifications: PendingCallerNotification[];
  /** Expired-before-delivery notifications, surfaced as a summary later. */
  missed: MissedCallerNotification[];
  unsubscribe: (() => void) | null;
  wakeTimer: ReturnType<typeof setTimeout> | null;
  draining: boolean;
}

const CALLER_NOTIFICATION_MAX_ATTEMPTS = 3;
const CALLER_NOTIFICATION_RETRY_DELAY_MS = 250;
const CALLER_NOTIFICATION_TIMEOUT_MS = 5 * 60_000;
const callerNotificationQueues = new WeakMap<AgentManager, Map<string, CallerNotificationQueue>>();

// Coupled to the exact message AgentManager throws for a concurrent run. The
// head of a fresh queue replace-dispatches into a running caller by fork
// policy; this check only covers the race where a run starts between the
// drain's busy gate and the dispatch.
function isCallerBusyError(params: { error: unknown; callerAgentId: string }): boolean {
  return (
    params.error instanceof Error &&
    params.error.message === `Agent ${params.callerAgentId} already has an active run`
  );
}

function cleanupCallerNotificationQueue(queue: CallerNotificationQueue): void {
  queue.unsubscribe?.();
  queue.unsubscribe = null;
  if (queue.wakeTimer) {
    clearTimeout(queue.wakeTimer);
    queue.wakeTimer = null;
  }
  const queues = callerNotificationQueues.get(queue.agentManager);
  queues?.delete(queue.callerAgentId);
  if (queues?.size === 0) {
    callerNotificationQueues.delete(queue.agentManager);
  }
}

/** Retire the persisted labels of notifications that reached a final outcome. */
function runNotifyLabelClears(
  queue: CallerNotificationQueue,
  entries: Array<PendingCallerNotification | MissedCallerNotification>,
): void {
  for (const entry of entries) {
    for (const clear of entry.labelClears ?? []) {
      void persistNotifyOnFinishLabel({
        agentManager: queue.agentManager,
        agentStorage: entry.agentStorage,
        childAgentId: clear.childAgentId,
        value: null,
        expectedValue: clear.expectedValue,
        logger: entry.logger,
      });
    }
  }
}

function logAbandonedCallerNotifications(params: {
  queue: CallerNotificationQueue;
  cause: "archived" | "closed" | "declined";
}): void {
  const { queue, cause } = params;
  for (const notification of queue.notifications) {
    notification.logger.warn(
      {
        childAgentId: notification.childAgentId,
        callerAgentId: notification.callerAgentId,
        reason: notification.reason,
        cause,
      },
      "Abandoned caller agent notification",
    );
  }
  // The caller is gone for good; a label left behind would make every future
  // boot re-report to it, so retire the labels along with the queue.
  runNotifyLabelClears(queue, [...queue.notifications, ...queue.missed]);
  queue.notifications.length = 0;
  queue.missed.length = 0;
  cleanupCallerNotificationQueue(queue);
}

function scheduleCallerNotificationWake(params: {
  queue: CallerNotificationQueue;
  delayMs: number;
}): void {
  const { queue, delayMs } = params;
  if (queue.wakeTimer) {
    clearTimeout(queue.wakeTimer);
  }
  const notification = queue.notifications[0];
  if (!notification) {
    if (queue.missed.length === 0) {
      cleanupCallerNotificationQueue(queue);
    }
    return;
  }
  const untilExpiry = Math.max(0, notification.expiresAt - Date.now());
  queue.wakeTimer = setTimeout(
    () => {
      queue.wakeTimer = null;
      void drainCallerNotificationQueue(queue);
    },
    Math.min(delayMs, untilExpiry),
  );
}

function shouldRetryCallerNotification(params: {
  error: unknown;
  notification: PendingCallerNotification;
}): boolean {
  const { error, notification } = params;
  if (!isCallerBusyError({ error, callerAgentId: notification.callerAgentId })) {
    notification.logger.error(
      {
        err: error,
        childAgentId: notification.childAgentId,
        callerAgentId: notification.callerAgentId,
        reason: notification.reason,
      },
      "Failed to notify caller agent",
    );
    return false;
  }
  if (notification.attempts < CALLER_NOTIFICATION_MAX_ATTEMPTS) {
    return true;
  }
  notification.logger.error(
    {
      err: error,
      childAgentId: notification.childAgentId,
      callerAgentId: notification.callerAgentId,
      reason: notification.reason,
      attempts: notification.attempts,
    },
    "Gave up notifying caller agent",
  );
  return false;
}

function recordMissedCallerNotification(
  queue: CallerNotificationQueue,
  notification: PendingCallerNotification,
): void {
  if (notification.synthetic) return;
  queue.missed.push({
    agentStorage: notification.agentStorage,
    childAgentId: notification.childAgentId,
    callerAgentId: notification.callerAgentId,
    reason: notification.reason,
    logger: notification.logger,
    labelClears: notification.labelClears,
  });
}

function formatMissedNotificationsBody(missed: MissedCallerNotification[]): string {
  const lines = missed.map(
    (entry) => `- Agent ${entry.childAgentId}: ${entry.reason} (notification expired undelivered)`,
  );
  return [
    `You missed ${missed.length} notification${missed.length === 1 ? "" : "s"} from delegated agents while you were unavailable:`,
    ...lines,
    "Use `get_agent_activity` and `list_pending_permissions` to catch up.",
  ].join("\n");
}

/**
 * Turn the missed backlog into one deliverable notification. The summary
 * never expires: it is the last line of defense against silent loss, so it
 * waits as long as the caller exists.
 */
function synthesizeMissedSummary(queue: CallerNotificationQueue): PendingCallerNotification {
  const source = queue.missed[0]!;
  const labelClears = queue.missed.flatMap((entry) => entry.labelClears ?? []);
  const summary: PendingCallerNotification = {
    agentStorage: source.agentStorage,
    childAgentId: source.childAgentId,
    callerAgentId: queue.callerAgentId,
    reason: "missed notifications",
    body: formatMissedNotificationsBody(queue.missed),
    logger: source.logger,
    attempts: 0,
    expiresAt: Number.POSITIVE_INFINITY,
    waitingForIdle: true,
    synthetic: true,
    ...(labelClears.length > 0 ? { labelClears } : {}),
  };
  queue.missed.length = 0;
  return summary;
}

/** Drop expired entries into the missed backlog before picking a batch. */
function expireStaleCallerNotifications(queue: CallerNotificationQueue): void {
  for (let index = queue.notifications.length - 1; index >= 0; index -= 1) {
    const candidate = queue.notifications[index];
    if (!candidate || Date.now() < candidate.expiresAt) continue;
    candidate.logger.error(
      {
        childAgentId: candidate.childAgentId,
        callerAgentId: candidate.callerAgentId,
        reason: candidate.reason,
        attempts: candidate.attempts,
      },
      "Gave up notifying caller agent",
    );
    recordMissedCallerNotification(queue, candidate);
    queue.notifications.splice(index, 1);
  }
}

/**
 * Deliver one coalesced batch to the caller. Returns "dispatched" when the
 * batch went out, "retry" when a busy caller should be retried later,
 * "dropped" when the head notification was given up on, and "abandoned"
 * when the caller turned out to be archived mid-dispatch.
 */
async function dispatchCallerNotificationBatch(params: {
  queue: CallerNotificationQueue;
  notification: PendingCallerNotification;
  batch: PendingCallerNotification[];
}): Promise<"dispatched" | "retry" | "dropped" | "abandoned"> {
  const { queue, notification, batch } = params;
  const bodies = batch.map((entry) => entry.body);
  const missedForDispatch = queue.missed.splice(0);
  if (missedForDispatch.length > 0) {
    bodies.unshift(formatMissedNotificationsBody(missedForDispatch));
  }

  try {
    notification.attempts += 1;
    // Fork policy: finish notifications use replace-dispatch, not active-turn steering.
    const result = await sendPromptToAgent({
      agentManager: queue.agentManager,
      agentStorage: notification.agentStorage,
      agentId: notification.callerAgentId,
      prompt: formatSystemNotificationPrompt(bodies.join("\n\n")),
      unarchive: false,
      logger: notification.logger,
    });
    if (result.disposition === "skipped") {
      // The caller was archived after drain's own check; the prompt was never
      // sent, so this is an abandonment, not a delivery.
      queue.missed.unshift(...missedForDispatch);
      logAbandonedCallerNotifications({ queue, cause: "archived" });
      return "abandoned";
    }
    queue.notifications.splice(0, batch.length);
    runNotifyLabelClears(queue, [...batch, ...missedForDispatch]);
    const nextNotification = queue.notifications[0];
    if (nextNotification) {
      nextNotification.waitingForIdle = true;
    }
    return "dispatched";
  } catch (error) {
    queue.missed.unshift(...missedForDispatch);
    if (shouldRetryCallerNotification({ error, notification })) {
      return "retry";
    }
    recordMissedCallerNotification(queue, notification);
    queue.notifications.shift();
    return "dropped";
  }
}

/**
 * Where the caller stands for delivery purposes. "gone" covers archived and
 * closed callers (queue abandoned); "busy" means an actual run in progress.
 * Errored, unloaded, and idle callers are all "ready" — waiting for a literal
 * "idle" would starve a caller that rests in the error state.
 */
async function assessCallerAvailability(
  queue: CallerNotificationQueue,
  notification: PendingCallerNotification,
): Promise<"gone" | "busy" | "ready"> {
  const callerRecord = await notification.agentStorage.get(notification.callerAgentId);
  if (callerRecord?.archivedAt) {
    logAbandonedCallerNotifications({ queue, cause: "archived" });
    return "gone";
  }
  // Subscriptions restored from labels or created before the caller's provider
  // opted out still land here; drop them and retire their labels.
  if (
    !callerAcceptsFinishNotifications({
      agentManager: queue.agentManager,
      callerAgentId: notification.callerAgentId,
      callerProvider: callerRecord?.provider,
    })
  ) {
    logAbandonedCallerNotifications({ queue, cause: "declined" });
    return "gone";
  }
  const caller = queue.agentManager.getAgent(notification.callerAgentId);
  if (caller?.lifecycle === "closed") {
    logAbandonedCallerNotifications({ queue, cause: "closed" });
    return "gone";
  }
  const busy =
    caller?.lifecycle === "running" ||
    caller?.lifecycle === "initializing" ||
    queue.agentManager.hasInFlightRun(notification.callerAgentId);
  return busy ? "busy" : "ready";
}

async function drainCallerNotificationQueue(queue: CallerNotificationQueue): Promise<void> {
  if (queue.draining) return;
  queue.draining = true;
  if (queue.wakeTimer) {
    clearTimeout(queue.wakeTimer);
    queue.wakeTimer = null;
  }

  try {
    while (true) {
      expireStaleCallerNotifications(queue);

      if (queue.notifications.length === 0) {
        if (queue.missed.length === 0) return;
        queue.notifications.push(synthesizeMissedSummary(queue));
        continue;
      }

      const notification = queue.notifications[0];
      if (!notification) return;

      // Coalesce the backlog that has already accumulated for this caller into
      // one parent turn: each dispatch costs the caller a full run, so N
      // pending notifications must not cost N runs. The batch is snapshotted
      // synchronously so notifications arriving mid-dispatch go out on the
      // next iteration instead of racing into this one.
      const batch = queue.notifications.slice();

      const availability = await assessCallerAvailability(queue, notification);
      if (availability === "gone") {
        return;
      }
      if (notification.waitingForIdle && availability === "busy") {
        scheduleCallerNotificationWake({ queue, delayMs: CALLER_NOTIFICATION_TIMEOUT_MS });
        return;
      }
      notification.waitingForIdle = false;

      const outcome = await dispatchCallerNotificationBatch({ queue, notification, batch });
      if (outcome === "abandoned") {
        return;
      }
      if (outcome === "retry") {
        notification.waitingForIdle = true;
        scheduleCallerNotificationWake({
          queue,
          delayMs: CALLER_NOTIFICATION_RETRY_DELAY_MS,
        });
        return;
      }
    }
  } finally {
    queue.draining = false;
    if (queue.notifications.length === 0 && queue.missed.length === 0) {
      cleanupCallerNotificationQueue(queue);
    }
  }
}

function enqueueCallerNotification(params: {
  agentManager: AgentManager;
  notification: Omit<PendingCallerNotification, "attempts" | "expiresAt" | "waitingForIdle">;
}): void {
  const { agentManager, notification } = params;
  let queues = callerNotificationQueues.get(agentManager);
  if (!queues) {
    queues = new Map();
    callerNotificationQueues.set(agentManager, queues);
  }

  let queue = queues.get(notification.callerAgentId);
  if (!queue) {
    queue = {
      agentManager,
      callerAgentId: notification.callerAgentId,
      notifications: [],
      missed: [],
      unsubscribe: null,
      wakeTimer: null,
      draining: false,
    };
    queues.set(notification.callerAgentId, queue);
    const createdQueue = queue;
    queue.unsubscribe = agentManager.subscribe(
      (event) => {
        if (event.type !== "agent_state") return;
        if (event.agent.lifecycle === "closed") {
          logAbandonedCallerNotifications({ queue: createdQueue, cause: "closed" });
          return;
        }
        if (event.agent.lifecycle === "idle" || event.agent.lifecycle === "error") {
          void drainCallerNotificationQueue(createdQueue);
        }
      },
      { agentId: notification.callerAgentId, replayState: false },
    );
  }

  queue.notifications.push({
    ...notification,
    attempts: 0,
    expiresAt: Date.now() + CALLER_NOTIFICATION_TIMEOUT_MS,
    waitingForIdle: queue.notifications.length > 0,
  });
  void drainCallerNotificationQueue(queue);
}

interface FinishSubscriptionEntry {
  stop: () => void;
  persistent: boolean;
}

// One live subscription per (child, caller): a parent that prompts the same
// child again must replace its stale watcher, not stack a duplicate.
const activeFinishSubscriptions = new WeakMap<AgentManager, Map<string, FinishSubscriptionEntry>>();

function finishSubscriptionKey(childAgentId: string, callerAgentId: string): string {
  return `${childAgentId}\u0000${callerAgentId}`;
}

export function setupFinishNotification(params: SetupFinishNotificationParams): void {
  const {
    agentManager,
    agentStorage,
    childAgentId,
    callerAgentId,
    requireParentOwnership = false,
    allowUnloaded = false,
    logger,
  } = params;
  if (!callerAcceptsFinishNotifications({ agentManager, callerAgentId })) {
    logger.debug(
      { childAgentId, callerAgentId },
      "Caller provider declines finish notifications; not subscribing",
    );
    return;
  }
  let hasSeenRunning = false;
  let stopped = false;
  const notifiedPermissionRequestIds = new Set<string>();
  let unsubscribe: (() => void) | null = null;
  let notificationQueue = Promise.resolve();

  let registry = activeFinishSubscriptions.get(agentManager);
  if (!registry) {
    registry = new Map();
    activeFinishSubscriptions.set(agentManager, registry);
  }
  const subscriptionKey = finishSubscriptionKey(childAgentId, callerAgentId);
  // An unspecified mode inherits the replaced subscription's persistence: a
  // routine follow-up prompt must not silently downgrade an "always" watch.
  const persistent = params.persistent ?? registry.get(subscriptionKey)?.persistent ?? false;

  function stop(): void {
    if (stopped) return;
    stopped = true;
    unsubscribe?.();
    if (registry?.get(subscriptionKey)?.stop === stop) {
      registry.delete(subscriptionKey);
    }
  }

  const persistedLabelValue = `${persistent ? "always" : "once"}:${callerAgentId}`;

  function stopAndClearLabel(): void {
    if (stopped) return;
    stop();
    void persistNotifyOnFinishLabel({
      agentManager,
      agentStorage,
      childAgentId,
      value: null,
      expectedValue: persistedLabelValue,
      logger,
    });
  }

  async function notify(
    reason: FinishNotificationReason,
    permissionRequest: AgentPermissionRequest | undefined,
    options: { retireLabelOnDelivery?: boolean } = {},
  ): Promise<void> {
    const callerRecord = await agentStorage.get(callerAgentId);
    if (callerRecord?.archivedAt) {
      logger.warn(
        { childAgentId, callerAgentId, reason, cause: "archived" },
        "Abandoned caller agent notification",
      );
      // The caller will never receive this, so the persisted subscription is
      // dead weight; retire it here rather than on delivery.
      if (options.retireLabelOnDelivery) {
        void persistNotifyOnFinishLabel({
          agentManager,
          agentStorage,
          childAgentId,
          value: null,
          expectedValue: persistedLabelValue,
          logger,
        });
      }
      return;
    }
    // A subscription restored before the caller was loaded, or created before
    // its provider opted out, ends here instead of waking the caller.
    if (
      !callerAcceptsFinishNotifications({
        agentManager,
        callerAgentId,
        callerProvider: callerRecord?.provider,
      })
    ) {
      // A terminal event has already stopped the subscription, so
      // stopAndClearLabel would skip the label; retire it explicitly.
      stop();
      void persistNotifyOnFinishLabel({
        agentManager,
        agentStorage,
        childAgentId,
        value: null,
        expectedValue: persistedLabelValue,
        logger,
      });
      return;
    }

    const record = await agentStorage.get(childAgentId);
    if (requireParentOwnership && getParentAgentIdFromLabels(record?.labels) !== callerAgentId) {
      return;
    }
    const title = record?.title ?? childAgentId;
    let lastAssistantMessage: string | null = null;
    try {
      lastAssistantMessage = await agentManager.getLastAssistantMessage(childAgentId);
    } catch {
      // A closed/unloaded child may have no readable timeline; the status
      // line alone must still reach the caller.
    }
    const body = formatFinishNotificationBody({
      childAgentId,
      title,
      reason,
      lastAssistantMessage,
      permissionRequest,
    });

    enqueueCallerNotification({
      agentManager,
      notification: {
        agentStorage,
        childAgentId,
        callerAgentId,
        reason,
        body,
        logger,
        ...(options.retireLabelOnDelivery
          ? { labelClears: [{ childAgentId, expectedValue: persistedLabelValue }] }
          : {}),
      },
    });
  }

  function notifySafely(reason: FinishNotificationReason, options: NotifySafelyOptions = {}): void {
    if (stopped) return;
    const terminal = options.terminal ?? true;
    // A terminal event ends the subscription immediately, but the persisted
    // label is retired only when the queued notification is delivered — a
    // daemon restart in between must still find it and report the miss.
    if (terminal) stop();
    notificationQueue = notificationQueue
      .then(() => notify(reason, options.permissionRequest, { retireLabelOnDelivery: terminal }))
      .catch((error) => {
        logger.error(
          { err: error, childAgentId, callerAgentId, reason },
          "Failed to notify caller agent",
        );
      });
  }

  unsubscribe = agentManager.subscribe(
    (event) => {
      if (stopped) {
        return;
      }

      if (event.type === "agent_state") {
        for (const requestId of notifiedPermissionRequestIds) {
          if (!event.agent.pendingPermissions.has(requestId)) {
            notifiedPermissionRequestIds.delete(requestId);
          }
        }
        if (event.agent.lifecycle === "running") {
          if (event.agent.pendingPermissions.size === 0) {
            hasSeenRunning = true;
          }
          return;
        }
        if (event.agent.lifecycle === "error") {
          notifySafely("errored", { terminal: !persistent });
          hasSeenRunning = false;
          return;
        }
        if (event.agent.lifecycle === "idle" && hasSeenRunning) {
          notifySafely("finished", { terminal: !persistent });
          hasSeenRunning = false;
          return;
        }
        if (event.agent.lifecycle === "closed") {
          notifySafely("was closed");
          return;
        }
        return;
      }

      if (event.type === "timeline_replacement") {
        return;
      }

      if (event.event.type === "permission_requested") {
        // A permission pause is an intermediate checkpoint. Forget the run
        // observed before it so an idle state during follow-up startup cannot
        // masquerade as the final completion.
        hasSeenRunning = false;
        if (!notifiedPermissionRequestIds.has(event.event.request.id)) {
          notifiedPermissionRequestIds.add(event.event.request.id);
          notifySafely("needs permission", {
            terminal: false,
            permissionRequest: event.event.request,
          });
        }
        return;
      }

      if (event.event.type === "permission_resolved") {
        notifiedPermissionRequestIds.delete(event.event.requestId);
        const childAgent = agentManager.getAgent(childAgentId);
        if (childAgent?.pendingPermissions.size === 0) {
          hasSeenRunning = childAgent.lifecycle === "running";
        }
      }
    },
    { agentId: childAgentId, replayState: false },
  );

  registry.get(subscriptionKey)?.stop();
  registry.set(subscriptionKey, { stop, persistent });
  void persistNotifyOnFinishLabel({
    agentManager,
    agentStorage,
    childAgentId,
    value: persistedLabelValue,
    logger,
  });

  // Check if the child is already running (catches the case where
  // the lifecycle flipped before our subscribe call was processed).
  // Do NOT treat an immediate "idle" as "finished" — the agent may
  // not have started yet (streamAgent sets a pending run before
  // transitioning to "running").
  const childSnapshot = agentManager.getAgent(childAgentId);
  if (!childSnapshot) {
    if (!allowUnloaded) {
      stopAndClearLabel();
    }
    return;
  }
  if (childSnapshot.lifecycle === "closed") {
    stopAndClearLabel();
    return;
  }
  if (childSnapshot.lifecycle === "running") {
    hasSeenRunning = true;
  } else if (childSnapshot.lifecycle === "error") {
    notifySafely("errored", { terminal: !persistent });
  }
}

/** Stop every live finish subscription watching a child (label cleared/edited). */
function stopFinishSubscriptionsForChild(agentManager: AgentManager, childAgentId: string): void {
  const registry = activeFinishSubscriptions.get(agentManager);
  if (!registry) return;
  const prefix = `${childAgentId}\u0000`;
  const stops: Array<() => void> = [];
  for (const [key, entry] of registry) {
    if (key.startsWith(prefix)) stops.push(entry.stop);
  }
  for (const stop of stops) stop();
}

/**
 * Re-apply a child's notify-on-finish label after it was edited (update_agent).
 * Establishes, replaces, or tears down the live subscription to match.
 */
export async function applyNotifyOnFinishLabelChange(params: {
  agentManager: AgentManager;
  agentStorage: AgentStorage;
  childAgentId: string;
  logger: Logger;
}): Promise<void> {
  const { agentManager, agentStorage, childAgentId, logger } = params;
  const record = await agentStorage.get(childAgentId);
  if (!record) return;
  // Tear down every existing watcher first: the label is one slot per child,
  // so a caller change must not leave the previous caller's watcher running.
  stopFinishSubscriptionsForChild(agentManager, childAgentId);
  const parsed = parseNotifyOnFinishLabel(record.labels);
  if (!parsed) return;
  const callerAgentId = parsed.callerAgentId ?? getParentAgentIdFromLabels(record.labels);
  if (!callerAgentId) {
    logger.warn(
      { childAgentId, label: NOTIFY_ON_FINISH_LABEL },
      "notify-on-finish label has no caller and the agent has no parent; ignoring",
    );
    return;
  }
  setupFinishNotification({
    agentManager,
    agentStorage,
    childAgentId,
    callerAgentId,
    persistent: parsed.mode === "always",
    // A parent-derived caller keeps the ownership guard create_agent applies,
    // so a detached/re-parented child stops notifying its ex-parent.
    requireParentOwnership: getParentAgentIdFromLabels(record.labels) === callerAgentId,
    allowUnloaded: true,
    logger,
  });
}

/**
 * Rebuild finish-notification subscriptions from persisted labels after a
 * daemon restart. Runs killed by the restart are reported to their caller as
 * interrupted; one-shot subscriptions end there (the watched run is gone),
 * while `always` subscriptions are re-established.
 */
export async function resumeFinishNotificationsOnBoot(params: {
  agentManager: AgentManager;
  agentStorage: AgentStorage;
  logger: Logger;
}): Promise<void> {
  const { agentManager, agentStorage, logger } = params;
  const records = await agentStorage.list();
  // Records are independent; catch-up timeline reads must not serialize boot.
  await Promise.all(
    records.map(async (record) => {
      if (record.archivedAt) return;
      const parsed = parseNotifyOnFinishLabel(record.labels);
      if (!parsed) return;
      const callerAgentId = parsed.callerAgentId ?? getParentAgentIdFromLabels(record.labels);
      if (!callerAgentId) return;
      const wasRunning = record.lastStatus === "running";

      if (parsed.mode === "always") {
        setupFinishNotification({
          agentManager,
          agentStorage,
          childAgentId: record.id,
          callerAgentId,
          persistent: true,
          requireParentOwnership: getParentAgentIdFromLabels(record.labels) === callerAgentId,
          allowUnloaded: true,
          logger,
        });
        // Only a run that was still in flight at shutdown warrants a catch-up;
        // the subscription itself covers everything after.
        if (!wasRunning) return;
      }

      // A one-shot watched run cannot have survived the restart. Mid-flight
      // runs were killed by it; anything else ended with the notification
      // undelivered — without knowing how, so the wording must not claim a
      // completion the run may never have reached. Delivery retires the label.
      const reason: FinishNotificationReason = wasRunning
        ? "was interrupted by a daemon restart"
        : "ended before a daemon restart could deliver its notification";
      let lastAssistantMessage: string | null = null;
      try {
        lastAssistantMessage = await agentManager.getLastAssistantMessage(record.id);
      } catch {
        // Timeline may be unreadable for an unloaded agent; the status line alone is enough.
      }
      const rawLabelValue = record.labels[NOTIFY_ON_FINISH_LABEL];
      enqueueCallerNotification({
        agentManager,
        notification: {
          agentStorage,
          childAgentId: record.id,
          callerAgentId,
          reason,
          body: formatFinishNotificationBody({
            childAgentId: record.id,
            title: record.title ?? record.id,
            reason,
            lastAssistantMessage,
          }),
          logger,
          ...(parsed.mode === "once" && rawLabelValue !== undefined
            ? { labelClears: [{ childAgentId: record.id, expectedValue: rawLabelValue }] }
            : {}),
        },
      });
    }),
  );
}
