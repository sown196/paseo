import type { Logger } from "pino";

import type {
  AgentPermissionRequest,
  AgentPromptInput,
  AgentRunOptions,
} from "./agent-sdk-types.js";
import type { AgentManager, ManagedAgent } from "./agent-manager.js";
import type { AgentStorage } from "./agent-storage.js";
import { ensureAgentLoaded } from "./agent-loading.js";
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
>;

export interface StartAgentRunOptions {
  replaceRunning?: boolean;
  activeTurnBehavior?: ActiveTurnBehavior;
  runOptions?: AgentRunOptions;
  /** Ask the provider to deny permissions blocking this steer. */
  clearPendingPermissions?: boolean;
}

export type PromptDispatchDisposition = "out_of_band" | "steered" | "turn_started";

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
      for await (const _ of iterator) {
        // Events are broadcast via AgentManager subscribers.
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
    await agentManager.waitForAgentRunStart(agentId, { signal: startAbort.signal });
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
 * When `unarchive` is false and the agent is archived, the call is a silent
 * no-op (returns the normal turn-start disposition) — the agent is not run.
 */
export async function sendPromptToAgent(
  params: SendPromptToAgentParams,
): Promise<{ disposition: PromptDispatchDisposition }> {
  const unarchive = params.unarchive ?? true;

  const record = await params.agentStorage.get(params.agentId);
  if (record?.archivedAt) {
    if (!unarchive) {
      return { disposition: "turn_started" };
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
  logger: Logger;
}

type FinishNotificationReason = "finished" | "errored" | "needs permission" | "was closed";

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

interface PendingCallerNotification {
  agentStorage: AgentStorage;
  childAgentId: string;
  callerAgentId: string;
  reason: FinishNotificationReason;
  prompt: string;
  logger: Logger;
  attempts: number;
  expiresAt: number;
  waitingForIdle: boolean;
}

interface CallerNotificationQueue {
  agentManager: AgentManager;
  callerAgentId: string;
  notifications: PendingCallerNotification[];
  unsubscribe: (() => void) | null;
  wakeTimer: ReturnType<typeof setTimeout> | null;
  draining: boolean;
}

const CALLER_NOTIFICATION_MAX_ATTEMPTS = 3;
const CALLER_NOTIFICATION_RETRY_DELAY_MS = 250;
const CALLER_NOTIFICATION_TIMEOUT_MS = 5 * 60_000;
const callerNotificationQueues = new WeakMap<AgentManager, Map<string, CallerNotificationQueue>>();

const isCallerBusyError = (params: { error: unknown; callerAgentId: string }): boolean => {
  return (
    params.error instanceof Error &&
    params.error.message === `Agent ${params.callerAgentId} already has an active run`
  );
};

const cleanupCallerNotificationQueue = (queue: CallerNotificationQueue): void => {
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
};

const logAbandonedCallerNotifications = (params: {
  queue: CallerNotificationQueue;
  cause: "archived" | "closed";
}): void => {
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
  queue.notifications.length = 0;
  cleanupCallerNotificationQueue(queue);
};

const scheduleCallerNotificationWake = (params: {
  queue: CallerNotificationQueue;
  delayMs: number;
}): void => {
  const { queue, delayMs } = params;
  if (queue.wakeTimer) {
    clearTimeout(queue.wakeTimer);
  }
  const notification = queue.notifications[0];
  if (!notification) {
    cleanupCallerNotificationQueue(queue);
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
};

const shouldRetryCallerNotification = (params: {
  error: unknown;
  notification: PendingCallerNotification;
}): boolean => {
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
};

const drainCallerNotificationQueue = async (queue: CallerNotificationQueue): Promise<void> => {
  if (queue.draining) return;
  queue.draining = true;
  if (queue.wakeTimer) {
    clearTimeout(queue.wakeTimer);
    queue.wakeTimer = null;
  }

  try {
    while (queue.notifications.length > 0) {
      const notification = queue.notifications[0];
      if (!notification) break;

      if (Date.now() >= notification.expiresAt) {
        notification.logger.error(
          {
            childAgentId: notification.childAgentId,
            callerAgentId: notification.callerAgentId,
            reason: notification.reason,
            attempts: notification.attempts,
          },
          "Gave up notifying caller agent",
        );
        queue.notifications.shift();
        continue;
      }

      const callerRecord = await notification.agentStorage.get(notification.callerAgentId);
      if (callerRecord?.archivedAt) {
        logAbandonedCallerNotifications({ queue, cause: "archived" });
        return;
      }

      const caller = queue.agentManager.getAgent(notification.callerAgentId);
      if (caller?.lifecycle === "closed") {
        logAbandonedCallerNotifications({ queue, cause: "closed" });
        return;
      }
      if (
        notification.waitingForIdle &&
        (caller?.lifecycle !== "idle" ||
          queue.agentManager.hasInFlightRun(notification.callerAgentId))
      ) {
        scheduleCallerNotificationWake({ queue, delayMs: CALLER_NOTIFICATION_TIMEOUT_MS });
        return;
      }
      notification.waitingForIdle = false;

      try {
        notification.attempts += 1;
        // Fork policy: finish notifications use replace-dispatch, not active-turn steering.
        await sendPromptToAgent({
          agentManager: queue.agentManager,
          agentStorage: notification.agentStorage,
          agentId: notification.callerAgentId,
          prompt: notification.prompt,
          unarchive: false,
          logger: notification.logger,
        });
        queue.notifications.shift();
        const nextNotification = queue.notifications[0];
        if (nextNotification) {
          nextNotification.waitingForIdle = true;
        }
      } catch (error) {
        if (shouldRetryCallerNotification({ error, notification })) {
          notification.waitingForIdle = true;
          scheduleCallerNotificationWake({
            queue,
            delayMs: CALLER_NOTIFICATION_RETRY_DELAY_MS,
          });
          return;
        }
        queue.notifications.shift();
      }
    }
  } finally {
    queue.draining = false;
    if (queue.notifications.length === 0) {
      cleanupCallerNotificationQueue(queue);
    }
  }
};

const enqueueCallerNotification = (params: {
  agentManager: AgentManager;
  notification: Omit<PendingCallerNotification, "attempts" | "expiresAt" | "waitingForIdle">;
}): void => {
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
        if (event.agent.lifecycle === "idle") {
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
};

export function setupFinishNotification(params: SetupFinishNotificationParams): void {
  const {
    agentManager,
    agentStorage,
    childAgentId,
    callerAgentId,
    requireParentOwnership = false,
    logger,
  } = params;
  let hasSeenRunning = false;
  let stopped = false;
  const notifiedPermissionRequestIds = new Set<string>();
  let unsubscribe: (() => void) | null = null;
  let notificationQueue = Promise.resolve();

  function stop(): void {
    if (stopped) return;
    stopped = true;
    unsubscribe?.();
  }

  async function notify(
    reason: FinishNotificationReason,
    permissionRequest?: AgentPermissionRequest,
  ): Promise<void> {
    const callerRecord = await agentStorage.get(callerAgentId);
    if (callerRecord?.archivedAt) {
      logger.warn(
        { childAgentId, callerAgentId, reason, cause: "archived" },
        "Abandoned caller agent notification",
      );
      return;
    }

    const record = await agentStorage.get(childAgentId);
    if (requireParentOwnership && getParentAgentIdFromLabels(record?.labels) !== callerAgentId) {
      return;
    }
    const title = record?.title ?? childAgentId;
    const lastAssistantMessage = await agentManager.getLastAssistantMessage(childAgentId);
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
        prompt: formatSystemNotificationPrompt(body),
        logger,
      },
    });
  }

  function notifySafely(reason: FinishNotificationReason, options: NotifySafelyOptions = {}): void {
    if (stopped) return;
    if (options.terminal ?? true) stop();
    notificationQueue = notificationQueue
      .then(() => notify(reason, options.permissionRequest))
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
          notifySafely("errored");
          return;
        }
        if (event.agent.lifecycle === "idle" && hasSeenRunning) {
          notifySafely("finished");
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

  // Check if the child is already running (catches the case where
  // the lifecycle flipped before our subscribe call was processed).
  // Do NOT treat an immediate "idle" as "finished" — the agent may
  // not have started yet (streamAgent sets a pending run before
  // transitioning to "running").
  const childSnapshot = agentManager.getAgent(childAgentId);
  if (!childSnapshot || childSnapshot.lifecycle === "closed") {
    stop();
    return;
  }
  if (childSnapshot.lifecycle === "running") {
    hasSeenRunning = true;
  } else if (childSnapshot.lifecycle === "error") {
    notifySafely("errored");
  }
}
