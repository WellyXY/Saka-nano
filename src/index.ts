import fs from 'fs';
import path from 'path';

import {
  ASSISTANT_NAME,
  CREDENTIAL_PROXY_PORT,
  DATA_DIR,
  IDLE_TIMEOUT,
  POLL_INTERVAL,
  TIMEZONE,
  TRIGGER_PATTERN,
} from './config.js';
import { startCredentialProxy } from './credential-proxy.js';
import './channels/index.js';
import {
  getChannelFactory,
  getRegisteredChannelNames,
} from './channels/registry.js';
import {
  ContainerOutput,
  runContainerAgent,
  syncWorkerSkillsBack,
  writeGroupsSnapshot,
  writeTasksSnapshot,
} from './container-runner.js';
import {
  cleanupOrphans,
  ensureContainerRuntimeRunning,
  listRunningContainers,
  stopContainerAsync,
  PROXY_BIND_HOST,
} from './container-runtime.js';
import {
  getAllChats,
  getAllRegisteredGroups,
  getAllSessions,
  getAllTasks,
  getMessagesSince,
  getNewMessages,
  getRegisteredGroup,
  getRouterState,
  initDatabase,
  setRegisteredGroup,
  setRouterState,
  setSession,
  storeChatMetadata,
  storeMessage,
} from './db.js';
import { GroupQueue } from './group-queue.js';
import { resolveGroupFolderPath, resolveGroupIpcPath } from './group-folder.js';
import {
  startIpcWatcher,
  getCollaborationRequester,
  clearCollaboration,
} from './ipc.js';
import { findChannel, formatMessages, formatOutbound } from './router.js';
import {
  isSenderAllowed,
  isTriggerAllowed,
  loadSenderAllowlist,
  shouldDropMessage,
} from './sender-allowlist.js';
import { startSchedulerLoop } from './task-scheduler.js';
import { Channel, NewMessage, RegisteredGroup } from './types.js';
import { logger } from './logger.js';

// Re-export for backwards compatibility during refactor
export { escapeXml, formatMessages } from './router.js';

let lastTimestamp = '';
let sessions: Record<string, string> = {};
let registeredGroups: Record<string, RegisteredGroup> = {};
let lastAgentTimestamp: Record<string, string> = {};
let messageLoopRunning = false;

const channels: Channel[] = [];
const queue = new GroupQueue();

function loadState(): void {
  lastTimestamp = getRouterState('last_timestamp') || '';
  const agentTs = getRouterState('last_agent_timestamp');
  try {
    lastAgentTimestamp = agentTs ? JSON.parse(agentTs) : {};
  } catch {
    logger.warn('Corrupted last_agent_timestamp in DB, resetting');
    lastAgentTimestamp = {};
  }
  sessions = getAllSessions();
  registeredGroups = getAllRegisteredGroups();

  // Recover missing session mappings from IPC backup files.
  // When NanoClaw restarts while containers are active, the session ID may not
  // have been saved to the DB yet. Each session save also writes a .session file
  // to the group's IPC directory as a backup.
  let recovered = 0;
  for (const [, group] of Object.entries(registeredGroups)) {
    if (sessions[group.folder]) continue;
    try {
      const ipcDir = resolveGroupIpcPath(group.folder);
      const sessionFile = path.join(ipcDir, '.session');
      if (fs.existsSync(sessionFile)) {
        const sid = fs.readFileSync(sessionFile, 'utf-8').trim();
        if (sid) {
          sessions[group.folder] = sid;
          setSession(group.folder, sid);
          recovered++;
        }
      }
    } catch {
      /* non-critical */
    }
  }
  if (recovered > 0) {
    logger.info({ recovered }, 'Recovered sessions from IPC backup files');
  }

  logger.info(
    { groupCount: Object.keys(registeredGroups).length },
    'State loaded',
  );
}

function saveState(): void {
  setRouterState('last_timestamp', lastTimestamp);
  setRouterState('last_agent_timestamp', JSON.stringify(lastAgentTimestamp));
}

function registerGroup(jid: string, group: RegisteredGroup): void {
  let groupDir: string;
  try {
    groupDir = resolveGroupFolderPath(group.folder);
  } catch (err) {
    logger.warn(
      { jid, folder: group.folder, err },
      'Rejecting group registration with invalid folder',
    );
    return;
  }

  registeredGroups[jid] = group;
  setRegisteredGroup(jid, group);

  // Create group folder
  fs.mkdirSync(path.join(groupDir, 'logs'), { recursive: true });

  logger.info(
    { jid, name: group.name, folder: group.folder },
    'Group registered',
  );
}

/**
 * Get available groups list for the agent.
 * Returns groups ordered by most recent activity.
 */
export function getAvailableGroups(): import('./container-runner.js').AvailableGroup[] {
  const chats = getAllChats();
  const registeredJids = new Set(Object.keys(registeredGroups));

  return chats
    .filter(
      (c) => c.jid !== '__group_sync__' && c.is_group && !c.jid.includes(':t:'),
    )
    .map((c) => ({
      jid: c.jid,
      name: c.name,
      lastActivity: c.last_message_time,
      isRegistered: registeredJids.has(c.jid),
    }));
}

/** @internal - exported for testing */
export function _setRegisteredGroups(
  groups: Record<string, RegisteredGroup>,
): void {
  registeredGroups = groups;
}

/**
 * Write a message to the Brain agent's IPC input so it receives worker updates.
 * Uses the main group's IPC directory (the group with isMain=true).
 */
function relayToBrain(agentType: string, text: string, isFinal: boolean): void {
  const mainEntry = Object.entries(registeredGroups).find(([, g]) => g.isMain);
  if (!mainEntry) return;

  const [mainJid, mainGroup] = mainEntry;
  const prefix = isFinal
    ? `[agent:${agentType}] 完成`
    : `[agent:${agentType}] 进度`;
  const relayText = `${prefix}:\n${text}`;

  // Write via GroupQueue so it goes through the standard IPC path
  const sent = queue.sendMessage(mainJid, relayText, true);
  if (!sent) {
    // Brain container not active — write directly to IPC input dir
    const brainIpcInput = path.join(DATA_DIR, 'ipc', mainGroup.folder, 'input');
    try {
      fs.mkdirSync(brainIpcInput, { recursive: true });
      const filename = `worker-${Date.now()}-${Math.random().toString(36).slice(2, 6)}.json`;
      const tempPath = path.join(brainIpcInput, `${filename}.tmp`);
      const finalPath = path.join(brainIpcInput, filename);
      fs.writeFileSync(
        tempPath,
        JSON.stringify({ type: 'message', text: relayText }),
      );
      fs.renameSync(tempPath, finalPath);
    } catch (err) {
      logger.warn(
        { err, agentType },
        'Failed to write worker relay to Brain IPC',
      );
    }
  }

  logger.info({ agentType, isFinal }, 'Relayed worker output to Brain');
}

/**
 * Relay a collaborator agent's output back to the requesting agent's IPC input.
 */
function relayToRequester(
  requesterFolder: string,
  agentType: string,
  text: string,
): void {
  const requesterIpcInput = path.join(
    DATA_DIR,
    'ipc',
    requesterFolder,
    'input',
  );
  try {
    fs.mkdirSync(requesterIpcInput, { recursive: true });
    const relayText = `[collaboration:${agentType}] 结果:\n${text}`;
    const filename = `collab-${Date.now()}-${Math.random().toString(36).slice(2, 6)}.json`;
    const tempPath = path.join(requesterIpcInput, `${filename}.tmp`);
    const finalPath = path.join(requesterIpcInput, filename);
    fs.writeFileSync(
      tempPath,
      JSON.stringify({ type: 'message', text: relayText }),
    );
    fs.renameSync(tempPath, finalPath);
    logger.info(
      { requesterFolder, agentType, textLen: text.length },
      'Collaboration result relayed to requester',
    );
  } catch (err) {
    logger.warn(
      { err, requesterFolder, agentType },
      'Failed to relay collaboration result',
    );
  }
}

/**
 * Process all pending messages for a group.
 * Called by the GroupQueue when it's this group's turn.
 */
async function processGroupMessages(chatJid: string): Promise<boolean> {
  const group = registeredGroups[chatJid];
  if (!group) return true;

  const channel = findChannel(channels, chatJid);
  if (!channel) {
    logger.warn({ chatJid }, 'No channel owns JID, skipping messages');
    return true;
  }

  const isMainGroup = group.isMain === true;

  const sinceTimestamp = lastAgentTimestamp[chatJid] || '';
  const missedMessages = getMessagesSince(
    chatJid,
    sinceTimestamp,
    ASSISTANT_NAME,
  );

  if (missedMessages.length === 0) return true;

  // For non-main groups, check if trigger is required and present
  if (!isMainGroup && group.requiresTrigger !== false) {
    const allowlistCfg = loadSenderAllowlist();
    const hasTrigger = missedMessages.some(
      (m) =>
        TRIGGER_PATTERN.test(m.content.trim()) &&
        (m.is_from_me || isTriggerAllowed(chatJid, m.sender, allowlistCfg)),
    );
    if (!hasTrigger) return true;
  }

  const prompt = formatMessages(missedMessages, TIMEZONE);

  // Advance cursor so the piping path in startMessageLoop won't re-fetch
  // these messages. Save the old cursor so we can roll back on error.
  const previousCursor = lastAgentTimestamp[chatJid] || '';
  lastAgentTimestamp[chatJid] =
    missedMessages[missedMessages.length - 1].timestamp;
  saveState();

  logger.info(
    { group: group.name, messageCount: missedMessages.length },
    'Processing messages',
  );

  let idleTimer: ReturnType<typeof setTimeout> | null = null;

  const resetIdleTimer = () => {
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      // Brain stays alive while workers are active (it needs to receive their results)
      if (isMainGroup && queue.hasActiveWorkers()) {
        logger.debug(
          { group: group.name },
          'Brain idle but workers active, extending timeout',
        );
        resetIdleTimer();
        return;
      }
      logger.debug(
        { group: group.name },
        'Idle timeout, closing container stdin',
      );
      queue.closeStdin(chatJid);
    }, IDLE_TIMEOUT);
  };

  // Track this worker in the queue so Brain knows when workers are active
  if (!isMainGroup && group.agentType) {
    queue.trackWorker(chatJid, true);
  }

  await channel.setTyping?.(chatJid, true);
  let hadError = false;
  let outputSentToUser = false;
  let collectedOutput = '';

  const output = await runAgent(group, prompt, chatJid, async (result) => {
    if (result.result) {
      const raw =
        typeof result.result === 'string'
          ? result.result
          : JSON.stringify(result.result);
      const text = raw.replace(/<internal>[\s\S]*?<\/internal>/g, '').trim();
      logger.info({ group: group.name }, `Agent output: ${raw.slice(0, 200)}`);
      if (text) {
        collectedOutput += (collectedOutput ? '\n' : '') + text;
        await channel.sendMessage(chatJid, text);
        outputSentToUser = true;

        // Feedback loop: relay worker output to Brain's IPC input
        if (!isMainGroup && group.agentType) {
          relayToBrain(group.agentType, text, false);
        }
      }
      // If text was empty (all <internal>), still track the internal summary
      // so the Brain knows what the agent did
      if (!text && raw.trim() && !isMainGroup && group.agentType) {
        const internalMatch = raw.match(/<internal>([\s\S]*?)<\/internal>/);
        const summary =
          internalMatch?.[1]?.trim() || '(agent 已在 thread 中回复)';
        relayToBrain(group.agentType, summary, false);
      }
      resetIdleTimer();
    }

    if (result.status === 'success') {
      queue.notifyIdle(chatJid);

      // Worker completed — notify Brain of final status
      if (!isMainGroup && group.agentType) {
        relayToBrain(group.agentType, '任务已完成。', true);
      }

      // Collaboration relay: send results back to the requesting agent
      if (!isMainGroup) {
        const requesterFolder = getCollaborationRequester(group.folder);
        if (requesterFolder) {
          relayToRequester(
            requesterFolder,
            group.agentType || 'agent',
            collectedOutput,
          );
          clearCollaboration(group.folder);
        }
      }
    }

    if (result.status === 'error') {
      hadError = true;

      if (!isMainGroup && group.agentType) {
        const errMsg = result.error || '未知错误';
        relayToBrain(group.agentType, `任务执行失败: ${errMsg}`, true);
      }

      // Collaboration relay: notify requester of failure too
      if (!isMainGroup) {
        const requesterFolder = getCollaborationRequester(group.folder);
        if (requesterFolder) {
          const errMsg = result.error || '未知错误';
          relayToRequester(
            requesterFolder,
            group.agentType || 'agent',
            `协作任务失败: ${errMsg}`,
          );
          clearCollaboration(group.folder);
        }
      }
    }
  });

  await channel.setTyping?.(chatJid, false);
  if (idleTimer) clearTimeout(idleTimer);

  if (!isMainGroup && group.agentType) {
    queue.trackWorker(chatJid, false);
    syncWorkerSkillsBack(group);
  }

  if (output === 'error' || hadError) {
    // If we already sent output to the user, don't roll back the cursor —
    // the user got their response and re-processing would send duplicates.
    if (outputSentToUser) {
      logger.warn(
        { group: group.name },
        'Agent error after output was sent, skipping cursor rollback to prevent duplicates',
      );
      return true;
    }
    // Roll back cursor so retries can re-process these messages
    lastAgentTimestamp[chatJid] = previousCursor;
    saveState();
    logger.warn(
      { group: group.name },
      'Agent error, rolled back message cursor for retry',
    );
    return false;
  }

  return true;
}

async function runAgent(
  group: RegisteredGroup,
  prompt: string,
  chatJid: string,
  onOutput?: (output: ContainerOutput) => Promise<void>,
): Promise<'success' | 'error'> {
  const isMain = group.isMain === true;
  const sessionId = sessions[group.folder];

  // Update tasks snapshot for container to read (filtered by group)
  const tasks = getAllTasks();
  writeTasksSnapshot(
    group.folder,
    isMain,
    tasks.map((t) => ({
      id: t.id,
      groupFolder: t.group_folder,
      prompt: t.prompt,
      schedule_type: t.schedule_type,
      schedule_value: t.schedule_value,
      status: t.status,
      next_run: t.next_run,
    })),
  );

  // Update available groups snapshot (main group only can see all groups)
  const availableGroups = getAvailableGroups();
  writeGroupsSnapshot(
    group.folder,
    isMain,
    availableGroups,
    new Set(Object.keys(registeredGroups)),
  );

  // Persist session ID to both DB and an IPC backup file
  const persistSession = (folder: string, sid: string) => {
    sessions[folder] = sid;
    setSession(folder, sid);
    try {
      const ipcDir = resolveGroupIpcPath(folder);
      fs.writeFileSync(path.join(ipcDir, '.session'), sid);
    } catch {
      /* non-critical */
    }
  };

  // Wrap onOutput to track session ID from streamed results
  const wrappedOnOutput = onOutput
    ? async (output: ContainerOutput) => {
        if (output.newSessionId) {
          persistSession(group.folder, output.newSessionId);
        }
        await onOutput(output);
      }
    : undefined;

  try {
    const output = await runContainerAgent(
      group,
      {
        prompt,
        sessionId,
        groupFolder: group.folder,
        chatJid,
        isMain,
        assistantName: ASSISTANT_NAME,
      },
      (proc, containerName) =>
        queue.registerProcess(chatJid, proc, containerName, group.folder),
      wrappedOnOutput,
    );

    if (output.newSessionId) {
      persistSession(group.folder, output.newSessionId);
    }

    if (output.status === 'error') {
      logger.error(
        { group: group.name, error: output.error },
        'Container agent error',
      );
      return 'error';
    }

    return 'success';
  } catch (err) {
    logger.error({ group: group.name, err }, 'Agent error');
    return 'error';
  }
}

async function startMessageLoop(): Promise<void> {
  if (messageLoopRunning) {
    logger.debug('Message loop already running, skipping duplicate start');
    return;
  }
  messageLoopRunning = true;

  logger.info(`NanoClaw running (trigger: @${ASSISTANT_NAME})`);

  while (true) {
    try {
      const jids = Object.keys(registeredGroups);
      const { messages, newTimestamp } = getNewMessages(
        jids,
        lastTimestamp,
        ASSISTANT_NAME,
      );

      if (messages.length > 0) {
        logger.info({ count: messages.length }, 'New messages');

        // Advance the "seen" cursor for all messages immediately
        lastTimestamp = newTimestamp;
        saveState();

        // Deduplicate by group
        const messagesByGroup = new Map<string, NewMessage[]>();
        for (const msg of messages) {
          const existing = messagesByGroup.get(msg.chat_jid);
          if (existing) {
            existing.push(msg);
          } else {
            messagesByGroup.set(msg.chat_jid, [msg]);
          }
        }

        for (const [chatJid, groupMessages] of messagesByGroup) {
          const group = registeredGroups[chatJid];
          if (!group) continue;

          const channel = findChannel(channels, chatJid);
          if (!channel) {
            logger.warn({ chatJid }, 'No channel owns JID, skipping messages');
            continue;
          }

          const isMainGroup = group.isMain === true;
          const needsTrigger = !isMainGroup && group.requiresTrigger !== false;

          // For non-main groups, only act on trigger messages.
          // Non-trigger messages accumulate in DB and get pulled as
          // context when a trigger eventually arrives.
          if (needsTrigger) {
            const allowlistCfg = loadSenderAllowlist();
            const hasTrigger = groupMessages.some(
              (m) =>
                TRIGGER_PATTERN.test(m.content.trim()) &&
                (m.is_from_me ||
                  isTriggerAllowed(chatJid, m.sender, allowlistCfg)),
            );
            if (!hasTrigger) continue;
          }

          // Pull all messages since lastAgentTimestamp so non-trigger
          // context that accumulated between triggers is included.
          const allPending = getMessagesSince(
            chatJid,
            lastAgentTimestamp[chatJid] || '',
            ASSISTANT_NAME,
          );
          const messagesToSend =
            allPending.length > 0 ? allPending : groupMessages;
          const formatted = formatMessages(messagesToSend, TIMEZONE);

          if (queue.sendMessage(chatJid, formatted)) {
            logger.debug(
              { chatJid, count: messagesToSend.length },
              'Piped messages to active container',
            );
            lastAgentTimestamp[chatJid] =
              messagesToSend[messagesToSend.length - 1].timestamp;
            saveState();
            // Show typing indicator while the container processes the piped message
            channel
              .setTyping?.(chatJid, true)
              ?.catch((err) =>
                logger.warn({ chatJid, err }, 'Failed to set typing indicator'),
              );
          } else {
            // No active container — enqueue for a new one
            queue.enqueueMessageCheck(chatJid);
          }
        }
      }
    } catch (err) {
      logger.error({ err }, 'Error in message loop');
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL));
  }
}

/**
 * Startup recovery: check for unprocessed messages in registered groups.
 * Handles crash between advancing lastTimestamp and processing messages.
 */
function recoverPendingMessages(): void {
  for (const [chatJid, group] of Object.entries(registeredGroups)) {
    const sinceTimestamp = lastAgentTimestamp[chatJid] || '';
    const pending = getMessagesSince(chatJid, sinceTimestamp, ASSISTANT_NAME);
    if (pending.length > 0) {
      logger.info(
        { group: group.name, pendingCount: pending.length },
        'Recovery: found unprocessed messages',
      );
      queue.enqueueMessageCheck(chatJid);
    }
  }
}

function ensureContainerSystemRunning(): void {
  ensureContainerRuntimeRunning();
  cleanupOrphans();
}

async function main(): Promise<void> {
  ensureContainerSystemRunning();
  initDatabase();
  logger.info('Database initialized');
  loadState();

  // Start credential proxy (containers route API calls through this)
  const proxyServer = await startCredentialProxy(
    CREDENTIAL_PROXY_PORT,
    PROXY_BIND_HOST,
  );

  // Graceful shutdown handlers
  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Shutdown signal received');
    setRouterState('last_shutdown_clean', 'true');
    proxyServer.close();
    await queue.shutdown(10000);
    for (const ch of channels) await ch.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Channel callbacks (shared by all channels)
  const channelOpts = {
    onMessage: (chatJid: string, msg: NewMessage) => {
      // Auto-register Slack thread groups from their parent channel.
      // When a thread reply arrives (with trigger, or parent needs no trigger),
      // create an independent group so it gets its own container and runs concurrently.
      if (!registeredGroups[chatJid] && chatJid.includes(':t:')) {
        const parentJid = chatJid.replace(/:t:.*$/, '');
        const parent = registeredGroups[parentJid];
        const parentNeedsTrigger =
          !parent?.isMain && parent?.requiresTrigger !== false;
        const hasTrigger = TRIGGER_PATTERN.test(msg.content.trim());
        if (parent && (hasTrigger || !parentNeedsTrigger)) {
          const threadTs = chatJid.split(':t:')[1];
          const threadFolder = `${parent.folder}_t_${threadTs.replace('.', '_')}`;
          storeChatMetadata(
            chatJid,
            new Date().toISOString(),
            `${parent.name} thread`,
            'slack',
            false,
          );
          registerGroup(chatJid, {
            name: `${parent.name} thread`,
            folder: threadFolder,
            trigger: parent.trigger,
            added_at: new Date().toISOString(),
            requiresTrigger: parentNeedsTrigger,
            isMain: false,
          });
          // Copy parent CLAUDE.md so thread agents share group memory
          try {
            const parentGroupDir = resolveGroupFolderPath(parent.folder);
            const threadGroupDir = resolveGroupFolderPath(threadFolder);
            const claudeMd = path.join(parentGroupDir, 'CLAUDE.md');
            if (fs.existsSync(claudeMd)) {
              fs.copyFileSync(claudeMd, path.join(threadGroupDir, 'CLAUDE.md'));
            }
          } catch {
            /* non-critical */
          }
          logger.info(
            { chatJid, parentJid, threadFolder },
            'Auto-registered Slack thread group',
          );
          // Enqueue immediately so the thread message gets processed
          // (the global message loop may have already advanced past it)
          queue.enqueueMessageCheck(chatJid);
        }
      }

      // Sender allowlist drop mode: discard messages from denied senders before storing
      if (!msg.is_from_me && !msg.is_bot_message && registeredGroups[chatJid]) {
        const cfg = loadSenderAllowlist();
        if (
          shouldDropMessage(chatJid, cfg) &&
          !isSenderAllowed(chatJid, msg.sender, cfg)
        ) {
          if (cfg.logDenied) {
            logger.debug(
              { chatJid, sender: msg.sender },
              'sender-allowlist: dropping message (drop mode)',
            );
          }
          return;
        }
      }
      storeMessage(msg);
    },
    onChatMetadata: (
      chatJid: string,
      timestamp: string,
      name?: string,
      channel?: string,
      isGroup?: boolean,
    ) => storeChatMetadata(chatJid, timestamp, name, channel, isGroup),
    registeredGroups: () => registeredGroups,
  };

  // Create and connect all registered channels.
  // Each channel self-registers via the barrel import above.
  // Factories return null when credentials are missing, so unconfigured channels are skipped.
  for (const channelName of getRegisteredChannelNames()) {
    const factory = getChannelFactory(channelName)!;
    const channel = factory(channelOpts);
    if (!channel) {
      logger.warn(
        { channel: channelName },
        'Channel installed but credentials missing — skipping. Check .env or re-run the channel skill.',
      );
      continue;
    }
    channels.push(channel);
    await channel.connect();
  }
  if (channels.length === 0) {
    logger.fatal('No channels connected');
    process.exit(1);
  }

  // Start subsystems (independently of connection handler)
  startSchedulerLoop({
    registeredGroups: () => registeredGroups,
    getSessions: () => sessions,
    queue,
    onProcess: (groupJid, proc, containerName, groupFolder) =>
      queue.registerProcess(groupJid, proc, containerName, groupFolder),
    sendMessage: async (jid, rawText) => {
      const channel = findChannel(channels, jid);
      if (!channel) {
        logger.warn({ jid }, 'No channel owns JID, cannot send message');
        return;
      }
      const text = formatOutbound(rawText);
      if (text) await channel.sendMessage(jid, text);
    },
  });
  startIpcWatcher({
    sendMessage: async (jid, text) => {
      const channel = findChannel(channels, jid);
      if (!channel) throw new Error(`No channel for JID: ${jid}`);
      return channel.sendMessage(jid, text);
    },
    registeredGroups: () => registeredGroups,
    registerGroup,
    syncGroups: async (force: boolean) => {
      await Promise.all(
        channels
          .filter((ch) => ch.syncGroups)
          .map((ch) => ch.syncGroups!(force)),
      );
    },
    getAvailableGroups,
    writeGroupsSnapshot: (gf, im, ag, rj) =>
      writeGroupsSnapshot(gf, im, ag, rj),
    enqueueMessageCheck: (jid) => queue.enqueueMessageCheck(jid),
  });
  // Send startup notification to main group
  const mainEntry = Object.entries(registeredGroups).find(([, g]) => g.isMain);
  if (mainEntry) {
    const [mainJid] = mainEntry;
    const wasCleanShutdown = getRouterState('last_shutdown_clean') === 'true';
    setRouterState('last_shutdown_clean', 'false');
    const startupMsg = wasCleanShutdown
      ? '🔄 Restarted.'
      : '⚠️ Crashed and restarted.';
    const startupChannel = findChannel(channels, mainJid);
    if (startupChannel) {
      startupChannel.sendMessage(mainJid, startupMsg).catch(() => {});
    }
  }

  queue.setProcessMessagesFn(processGroupMessages);

  // Tell the queue which JID is the Brain so it can manage idle timeout
  if (mainEntry) {
    queue.setBrainJid(mainEntry[0]);
  }

  recoverPendingMessages();
  startContainerHealthMonitor();
  startMessageLoop().catch((err) => {
    logger.fatal({ err }, 'Message loop crashed unexpectedly');
    process.exit(1);
  });
}

const HEALTH_CHECK_INTERVAL = 30_000;

function startContainerHealthMonitor(): void {
  setInterval(() => {
    const running = listRunningContainers();
    const tracked = queue.getTrackedContainers();
    const zombies = queue.healthCheck();

    // Always write status snapshot so check_agents has fresh data
    writeContainerStatus(tracked, running);

    // Clean up orphaned containers not tracked by GroupQueue
    const trackedNames = new Set(
      tracked.map((t) => t.containerName).filter(Boolean),
    );
    for (const name of running) {
      if (!trackedNames.has(name) && name !== 'buildkit') {
        logger.info(
          { name },
          'Health monitor: stopping untracked orphan container',
        );
        stopContainerAsync(name);
      }
    }

    if (zombies.length === 0) return;

    for (const z of zombies) {
      logger.warn(
        { groupJid: z.groupJid, containerName: z.containerName },
        'Container health check: tracked container no longer running',
      );

      queue.forceRelease(z.groupJid);

      if (z.containerName) {
        stopContainerAsync(z.containerName);
      }
    }

    // Notify Saka about dead containers
    const mainEntry = Object.entries(registeredGroups).find(
      ([, g]) => g.isMain,
    );
    if (mainEntry) {
      const [mainJid, mainGroup] = mainEntry;
      const names = zombies.map((z) => z.containerName).join(', ');
      const report = `⚠️ Container health check: ${zombies.length} agent(s) died unexpectedly: ${names}. Slots have been freed automatically.`;

      const sent = queue.sendMessage(mainJid, report, true);
      if (!sent) {
        const brainIpcInput = path.join(
          DATA_DIR,
          'ipc',
          mainGroup.folder,
          'input',
        );
        try {
          fs.mkdirSync(brainIpcInput, { recursive: true });
          const filename = `health-${Date.now()}.json`;
          const tempPath = path.join(brainIpcInput, `${filename}.tmp`);
          const finalPath = path.join(brainIpcInput, filename);
          fs.writeFileSync(
            tempPath,
            JSON.stringify({ type: 'message', text: report }),
          );
          fs.renameSync(tempPath, finalPath);
        } catch (err) {
          logger.warn({ err }, 'Failed to notify Brain about dead containers');
        }
      }
    }
  }, HEALTH_CHECK_INTERVAL);

  logger.info(
    { intervalMs: HEALTH_CHECK_INTERVAL },
    'Container health monitor started',
  );
}

/**
 * Write container status to the Brain's IPC dir so check_agents can read it.
 * Includes both active containers AND registered thread groups for full visibility.
 */
function writeContainerStatus(
  tracked: ReturnType<typeof queue.getTrackedContainers>,
  running: string[],
): void {
  const mainEntry = Object.entries(registeredGroups).find(([, g]) => g.isMain);
  if (!mainEntry) return;

  const [, mainGroup] = mainEntry;
  const runningSet = new Set(running);
  const statusPath = path.join(
    DATA_DIR,
    'ipc',
    mainGroup.folder,
    'container_status.json',
  );

  // Active containers with live/dead status
  const activeContainers = tracked.map((t) => ({
    ...t,
    alive: t.containerName ? runningSet.has(t.containerName) : false,
  }));

  // All thread groups (dispatched agents) with their agent type
  const threadGroups: Array<{
    groupJid: string;
    folder: string;
    name: string;
    agentType?: string;
    hasContainer: boolean;
  }> = [];

  const trackedFolders = new Set(
    tracked.map((t) => t.groupFolder).filter(Boolean),
  );
  for (const [jid, group] of Object.entries(registeredGroups)) {
    if (jid.includes(':t:') && !group.isMain) {
      threadGroups.push({
        groupJid: jid,
        folder: group.folder,
        name: group.name || group.folder,
        agentType: group.agentType,
        hasContainer:
          trackedFolders.has(group.folder) ||
          running.some((r) =>
            r.includes(group.folder.replace(/[^a-zA-Z0-9_-]/g, '-')),
          ),
      });
    }
  }

  const status = {
    timestamp: new Date().toISOString(),
    runningContainers: running.filter((r) => r !== 'buildkit'),
    activeContainers,
    threadGroups,
  };

  try {
    fs.mkdirSync(path.dirname(statusPath), { recursive: true });
    const tmpPath = `${statusPath}.tmp`;
    fs.writeFileSync(tmpPath, JSON.stringify(status, null, 2));
    fs.renameSync(tmpPath, statusPath);
  } catch (err) {
    logger.debug({ err }, 'Failed to write container status snapshot');
  }
}

// Guard: only run when executed directly, not when imported by tests
const isDirectRun =
  process.argv[1] &&
  new URL(import.meta.url).pathname ===
    new URL(`file://${process.argv[1]}`).pathname;

if (isDirectRun) {
  main().catch((err) => {
    logger.error({ err }, 'Failed to start NanoClaw');
    process.exit(1);
  });
}
