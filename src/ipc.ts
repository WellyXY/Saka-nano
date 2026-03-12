import fs from 'fs';
import path from 'path';

import { CronExpressionParser } from 'cron-parser';

import { DATA_DIR, GROUPS_DIR, IPC_POLL_INTERVAL, TIMEZONE } from './config.js';
import { AvailableGroup } from './container-runner.js';
import {
  createTask,
  deleteTask,
  getTaskById,
  storeChatMetadata,
  storeMessage,
  updateTask,
} from './db.js';
import { isValidGroupFolder } from './group-folder.js';
import { logger } from './logger.js';
import { NewMessage, RegisteredGroup } from './types.js';

export interface IpcDeps {
  sendMessage: (jid: string, text: string) => Promise<string | void>;
  registeredGroups: () => Record<string, RegisteredGroup>;
  registerGroup: (jid: string, group: RegisteredGroup) => void;
  syncGroups: (force: boolean) => Promise<void>;
  getAvailableGroups: () => AvailableGroup[];
  writeGroupsSnapshot: (
    groupFolder: string,
    isMain: boolean,
    availableGroups: AvailableGroup[],
    registeredJids: Set<string>,
  ) => void;
  enqueueMessageCheck: (jid: string) => void;
}

let ipcWatcherRunning = false;

export function startIpcWatcher(deps: IpcDeps): void {
  if (ipcWatcherRunning) {
    logger.debug('IPC watcher already running, skipping duplicate start');
    return;
  }
  ipcWatcherRunning = true;

  const ipcBaseDir = path.join(DATA_DIR, 'ipc');
  fs.mkdirSync(ipcBaseDir, { recursive: true });

  const processIpcFiles = async () => {
    // Scan all group IPC directories (identity determined by directory)
    let groupFolders: string[];
    try {
      groupFolders = fs.readdirSync(ipcBaseDir).filter((f) => {
        const stat = fs.statSync(path.join(ipcBaseDir, f));
        return stat.isDirectory() && f !== 'errors';
      });
    } catch (err) {
      logger.error({ err }, 'Error reading IPC base directory');
      setTimeout(processIpcFiles, IPC_POLL_INTERVAL);
      return;
    }

    const registeredGroups = deps.registeredGroups();

    // Build folder→isMain lookup from registered groups
    const folderIsMain = new Map<string, boolean>();
    for (const group of Object.values(registeredGroups)) {
      if (group.isMain) folderIsMain.set(group.folder, true);
    }

    for (const sourceGroup of groupFolders) {
      const isMain = folderIsMain.get(sourceGroup) === true;
      const messagesDir = path.join(ipcBaseDir, sourceGroup, 'messages');
      const tasksDir = path.join(ipcBaseDir, sourceGroup, 'tasks');

      // Process messages from this group's IPC directory
      try {
        if (fs.existsSync(messagesDir)) {
          const messageFiles = fs
            .readdirSync(messagesDir)
            .filter((f) => f.endsWith('.json'));
          for (const file of messageFiles) {
            const filePath = path.join(messagesDir, file);
            try {
              const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
              if (data.type === 'message' && data.chatJid && data.text) {
                // Authorization: verify this group can send to this chatJid
                const targetGroup = registeredGroups[data.chatJid];
                if (
                  isMain ||
                  (targetGroup && targetGroup.folder === sourceGroup)
                ) {
                  await deps.sendMessage(data.chatJid, data.text);
                  logger.info(
                    { chatJid: data.chatJid, sourceGroup },
                    'IPC message sent',
                  );
                } else {
                  logger.warn(
                    { chatJid: data.chatJid, sourceGroup },
                    'Unauthorized IPC message attempt blocked',
                  );
                }
              }
              fs.unlinkSync(filePath);
            } catch (err) {
              logger.error(
                { file, sourceGroup, err },
                'Error processing IPC message',
              );
              const errorDir = path.join(ipcBaseDir, 'errors');
              fs.mkdirSync(errorDir, { recursive: true });
              fs.renameSync(
                filePath,
                path.join(errorDir, `${sourceGroup}-${file}`),
              );
            }
          }
        }
      } catch (err) {
        logger.error(
          { err, sourceGroup },
          'Error reading IPC messages directory',
        );
      }

      // Process tasks from this group's IPC directory
      try {
        if (fs.existsSync(tasksDir)) {
          const taskFiles = fs
            .readdirSync(tasksDir)
            .filter((f) => f.endsWith('.json'));
          for (const file of taskFiles) {
            const filePath = path.join(tasksDir, file);
            try {
              const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
              // Pass source group identity to processTaskIpc for authorization
              await processTaskIpc(data, sourceGroup, isMain, deps);
              fs.unlinkSync(filePath);
            } catch (err) {
              logger.error(
                { file, sourceGroup, err },
                'Error processing IPC task',
              );
              const errorDir = path.join(ipcBaseDir, 'errors');
              fs.mkdirSync(errorDir, { recursive: true });
              fs.renameSync(
                filePath,
                path.join(errorDir, `${sourceGroup}-${file}`),
              );
            }
          }
        }
      } catch (err) {
        logger.error({ err, sourceGroup }, 'Error reading IPC tasks directory');
      }
    }

    setTimeout(processIpcFiles, IPC_POLL_INTERVAL);
  };

  processIpcFiles();
  logger.info('IPC watcher started (per-group namespaces)');
}

export async function processTaskIpc(
  data: {
    type: string;
    taskId?: string;
    prompt?: string;
    schedule_type?: string;
    schedule_value?: string;
    context_mode?: string;
    groupFolder?: string;
    chatJid?: string;
    targetJid?: string;
    // For register_group
    jid?: string;
    name?: string;
    folder?: string;
    trigger?: string;
    requiresTrigger?: boolean;
    containerConfig?: RegisteredGroup['containerConfig'];
    // For dispatch_thread
    description?: string;
    agentType?: string;
    // For register_agent
    agentName?: string;
    agentDisplayName?: string;
    agentDescription?: string;
    agentSkills?: string[];
    agentTriggers?: string[];
    // For request_collaboration
    requesterFolder?: string;
    targetAgent?: string;
    task?: string;
    // For nudge_agent
    targetFolder?: string;
    message?: string;
  },
  sourceGroup: string, // Verified identity from IPC directory
  isMain: boolean, // Verified from directory path
  deps: IpcDeps,
): Promise<void> {
  const registeredGroups = deps.registeredGroups();

  switch (data.type) {
    case 'schedule_task':
      if (
        data.prompt &&
        data.schedule_type &&
        data.schedule_value &&
        data.targetJid
      ) {
        // Resolve the target group from JID
        const targetJid = data.targetJid as string;
        const targetGroupEntry = registeredGroups[targetJid];

        if (!targetGroupEntry) {
          logger.warn(
            { targetJid },
            'Cannot schedule task: target group not registered',
          );
          break;
        }

        const targetFolder = targetGroupEntry.folder;

        // Authorization: non-main groups can only schedule for themselves
        if (!isMain && targetFolder !== sourceGroup) {
          logger.warn(
            { sourceGroup, targetFolder },
            'Unauthorized schedule_task attempt blocked',
          );
          break;
        }

        const scheduleType = data.schedule_type as 'cron' | 'interval' | 'once';

        let nextRun: string | null = null;
        if (scheduleType === 'cron') {
          try {
            const interval = CronExpressionParser.parse(data.schedule_value, {
              tz: TIMEZONE,
            });
            nextRun = interval.next().toISOString();
          } catch {
            logger.warn(
              { scheduleValue: data.schedule_value },
              'Invalid cron expression',
            );
            break;
          }
        } else if (scheduleType === 'interval') {
          const ms = parseInt(data.schedule_value, 10);
          if (isNaN(ms) || ms <= 0) {
            logger.warn(
              { scheduleValue: data.schedule_value },
              'Invalid interval',
            );
            break;
          }
          nextRun = new Date(Date.now() + ms).toISOString();
        } else if (scheduleType === 'once') {
          const date = new Date(data.schedule_value);
          if (isNaN(date.getTime())) {
            logger.warn(
              { scheduleValue: data.schedule_value },
              'Invalid timestamp',
            );
            break;
          }
          nextRun = date.toISOString();
        }

        const taskId =
          data.taskId ||
          `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const contextMode =
          data.context_mode === 'group' || data.context_mode === 'isolated'
            ? data.context_mode
            : 'isolated';
        createTask({
          id: taskId,
          group_folder: targetFolder,
          chat_jid: targetJid,
          prompt: data.prompt,
          schedule_type: scheduleType,
          schedule_value: data.schedule_value,
          context_mode: contextMode,
          next_run: nextRun,
          status: 'active',
          created_at: new Date().toISOString(),
        });
        logger.info(
          { taskId, sourceGroup, targetFolder, contextMode },
          'Task created via IPC',
        );
      }
      break;

    case 'pause_task':
      if (data.taskId) {
        const task = getTaskById(data.taskId);
        if (task && (isMain || task.group_folder === sourceGroup)) {
          updateTask(data.taskId, { status: 'paused' });
          logger.info(
            { taskId: data.taskId, sourceGroup },
            'Task paused via IPC',
          );
        } else {
          logger.warn(
            { taskId: data.taskId, sourceGroup },
            'Unauthorized task pause attempt',
          );
        }
      }
      break;

    case 'resume_task':
      if (data.taskId) {
        const task = getTaskById(data.taskId);
        if (task && (isMain || task.group_folder === sourceGroup)) {
          updateTask(data.taskId, { status: 'active' });
          logger.info(
            { taskId: data.taskId, sourceGroup },
            'Task resumed via IPC',
          );
        } else {
          logger.warn(
            { taskId: data.taskId, sourceGroup },
            'Unauthorized task resume attempt',
          );
        }
      }
      break;

    case 'cancel_task':
      if (data.taskId) {
        const task = getTaskById(data.taskId);
        if (task && (isMain || task.group_folder === sourceGroup)) {
          deleteTask(data.taskId);
          logger.info(
            { taskId: data.taskId, sourceGroup },
            'Task cancelled via IPC',
          );
        } else {
          logger.warn(
            { taskId: data.taskId, sourceGroup },
            'Unauthorized task cancel attempt',
          );
        }
      }
      break;

    case 'update_task':
      if (data.taskId) {
        const task = getTaskById(data.taskId);
        if (!task) {
          logger.warn(
            { taskId: data.taskId, sourceGroup },
            'Task not found for update',
          );
          break;
        }
        if (!isMain && task.group_folder !== sourceGroup) {
          logger.warn(
            { taskId: data.taskId, sourceGroup },
            'Unauthorized task update attempt',
          );
          break;
        }

        const updates: Parameters<typeof updateTask>[1] = {};
        if (data.prompt !== undefined) updates.prompt = data.prompt;
        if (data.schedule_type !== undefined)
          updates.schedule_type = data.schedule_type as
            | 'cron'
            | 'interval'
            | 'once';
        if (data.schedule_value !== undefined)
          updates.schedule_value = data.schedule_value;

        // Recompute next_run if schedule changed
        if (data.schedule_type || data.schedule_value) {
          const updatedTask = {
            ...task,
            ...updates,
          };
          if (updatedTask.schedule_type === 'cron') {
            try {
              const interval = CronExpressionParser.parse(
                updatedTask.schedule_value,
                { tz: TIMEZONE },
              );
              updates.next_run = interval.next().toISOString();
            } catch {
              logger.warn(
                { taskId: data.taskId, value: updatedTask.schedule_value },
                'Invalid cron in task update',
              );
              break;
            }
          } else if (updatedTask.schedule_type === 'interval') {
            const ms = parseInt(updatedTask.schedule_value, 10);
            if (!isNaN(ms) && ms > 0) {
              updates.next_run = new Date(Date.now() + ms).toISOString();
            }
          }
        }

        updateTask(data.taskId, updates);
        logger.info(
          { taskId: data.taskId, sourceGroup, updates },
          'Task updated via IPC',
        );
      }
      break;

    case 'refresh_groups':
      // Only main group can request a refresh
      if (isMain) {
        logger.info(
          { sourceGroup },
          'Group metadata refresh requested via IPC',
        );
        await deps.syncGroups(true);
        // Write updated snapshot immediately
        const availableGroups = deps.getAvailableGroups();
        deps.writeGroupsSnapshot(
          sourceGroup,
          true,
          availableGroups,
          new Set(Object.keys(registeredGroups)),
        );
      } else {
        logger.warn(
          { sourceGroup },
          'Unauthorized refresh_groups attempt blocked',
        );
      }
      break;

    case 'register_group':
      // Only main group can register new groups
      if (!isMain) {
        logger.warn(
          { sourceGroup },
          'Unauthorized register_group attempt blocked',
        );
        break;
      }
      if (data.jid && data.name && data.folder && data.trigger) {
        if (!isValidGroupFolder(data.folder)) {
          logger.warn(
            { sourceGroup, folder: data.folder },
            'Invalid register_group request - unsafe folder name',
          );
          break;
        }
        // Defense in depth: agent cannot set isMain via IPC
        deps.registerGroup(data.jid, {
          name: data.name,
          folder: data.folder,
          trigger: data.trigger,
          added_at: new Date().toISOString(),
          containerConfig: data.containerConfig,
          requiresTrigger: data.requiresTrigger,
        });
      } else {
        logger.warn(
          { data },
          'Invalid register_group request - missing required fields',
        );
      }
      break;

    case 'dispatch_thread':
      if (!isMain) {
        logger.warn(
          { sourceGroup },
          'Unauthorized dispatch_thread attempt blocked',
        );
        break;
      }
      if (data.chatJid && data.description && data.prompt && data.agentType) {
        await handleDispatchThread(
          data as {
            chatJid: string;
            description: string;
            prompt: string;
            agentType: string;
          },
          sourceGroup,
          deps,
        );
      } else {
        logger.warn(
          { data },
          'Invalid dispatch_thread - missing chatJid, description, prompt, or agentType',
        );
      }
      break;

    case 'register_agent':
      if (data.agentName && data.agentDescription) {
        handleRegisterAgent(
          {
            name: data.agentName,
            displayName: data.agentDisplayName,
            description: data.agentDescription,
            skills: data.agentSkills || [],
            triggers: data.agentTriggers || [],
          },
          sourceGroup,
        );
      } else {
        logger.warn(
          { data },
          'Invalid register_agent - missing name or description',
        );
      }
      break;

    case 'request_collaboration':
      if (data.targetAgent && data.task && data.requesterFolder) {
        await handleCollaborationRequest(
          {
            chatJid: data.chatJid || '',
            requesterFolder: data.requesterFolder,
            targetAgent: data.targetAgent,
            task: data.task,
          },
          sourceGroup,
          deps,
        );
      } else {
        logger.warn({ data }, 'Invalid request_collaboration - missing fields');
      }
      break;

    case 'nudge_agent':
      if (data.targetFolder && data.message) {
        handleNudgeAgent(data.targetFolder, data.targetJid || '', data.message);
      }
      break;

    default:
      logger.warn({ type: data.type }, 'Unknown IPC task type');
  }
}

async function handleDispatchThread(
  data: {
    chatJid: string;
    description: string;
    prompt: string;
    agentType: string;
  },
  sourceGroup: string,
  deps: IpcDeps,
): Promise<void> {
  const registeredGroups = deps.registeredGroups();

  const parentJid = data.chatJid.replace(/:t:.*$/, '');
  const parent = registeredGroups[parentJid] || registeredGroups[data.chatJid];
  if (!parent) {
    logger.warn(
      { chatJid: data.chatJid },
      'dispatch_thread: parent group not found',
    );
    return;
  }

  const messageTs = await deps.sendMessage(
    parentJid,
    `📋 [${data.agentType}] ${data.description}`,
  );

  if (!messageTs) {
    logger.error(
      { chatJid: data.chatJid },
      'dispatch_thread: failed to get message ts from channel',
    );
    return;
  }

  const channelId = parentJid.replace(/^slack:/, '');
  const threadJid = `slack:${channelId}:t:${messageTs}`;
  const threadFolder = `${parent.folder}_t_${messageTs.replace('.', '_')}`;

  storeChatMetadata(
    threadJid,
    new Date().toISOString(),
    undefined,
    'slack',
    false,
  );

  deps.registerGroup(threadJid, {
    name: `${data.agentType} agent`,
    folder: threadFolder,
    trigger: parent.trigger,
    added_at: new Date().toISOString(),
    requiresTrigger: false,
    isMain: false,
    agentType: data.agentType,
  });

  const msg: NewMessage = {
    id: `dispatch-${Date.now()}`,
    chat_jid: threadJid,
    sender: 'brain',
    sender_name: 'Brain',
    content: data.prompt,
    timestamp: new Date().toISOString(),
    is_from_me: false,
    is_bot_message: false,
  };
  storeMessage(msg);

  deps.enqueueMessageCheck(threadJid);

  logger.info(
    {
      threadJid,
      threadFolder,
      agentType: data.agentType,
      description: data.description,
    },
    'dispatch_thread: agent dispatched',
  );
}

/**
 * Track pending collaboration requests so results can be routed back to the requester.
 * Key: threadFolder of the dispatched collaborator
 * Value: requesterFolder to relay results to
 */
const pendingCollaborations = new Map<string, string>();

export function getCollaborationRequester(
  threadFolder: string,
): string | undefined {
  return pendingCollaborations.get(threadFolder);
}

export function clearCollaboration(threadFolder: string): void {
  pendingCollaborations.delete(threadFolder);
}

async function handleCollaborationRequest(
  data: {
    chatJid: string;
    requesterFolder: string;
    targetAgent: string;
    task: string;
  },
  sourceGroup: string,
  deps: IpcDeps,
): Promise<void> {
  const registeredGroups = deps.registeredGroups();

  const parentFolder = sourceGroup.match(/^(.+?)_t_\d+/)?.[1] || sourceGroup;
  const parentEntry = Object.entries(registeredGroups).find(
    ([, g]) => g.folder === parentFolder,
  );
  if (!parentEntry) {
    logger.warn(
      { sourceGroup, parentFolder },
      'request_collaboration: parent group not found',
    );
    return;
  }

  const [parentJid, parent] = parentEntry;

  const agentDisplayName = data.targetAgent;
  const description = `[collab] ${agentDisplayName}: ${data.task.slice(0, 80)}`;
  const prompt = `${data.task}\n\n---\n这是来自另一个 agent 的协作请求。完成任务后，你的结果会自动转发给请求方。请直接输出结果，不需要额外确认。`;

  const messageTs = await deps.sendMessage(
    parentJid,
    `🤝 [collaboration] ${agentDisplayName}: ${data.task.slice(0, 100)}`,
  );

  if (!messageTs) {
    logger.error(
      { parentJid },
      'request_collaboration: failed to create thread',
    );
    return;
  }

  const channelId = parentJid.replace(/^slack:/, '');
  const threadJid = `slack:${channelId}:t:${messageTs}`;
  const threadFolder = `${parent.folder}_t_${messageTs.replace('.', '_')}`;

  pendingCollaborations.set(threadFolder, data.requesterFolder);

  storeChatMetadata(
    threadJid,
    new Date().toISOString(),
    undefined,
    'slack',
    false,
  );

  deps.registerGroup(threadJid, {
    name: `${agentDisplayName} agent (collab)`,
    folder: threadFolder,
    trigger: parent.trigger,
    added_at: new Date().toISOString(),
    requiresTrigger: false,
    isMain: false,
    agentType: data.targetAgent,
  });

  const msg: NewMessage = {
    id: `collab-${Date.now()}`,
    chat_jid: threadJid,
    sender: 'collaboration',
    sender_name: 'Collaboration Request',
    content: prompt,
    timestamp: new Date().toISOString(),
    is_from_me: false,
    is_bot_message: false,
  };
  storeMessage(msg);

  deps.enqueueMessageCheck(threadJid);

  logger.info(
    {
      threadFolder,
      requesterFolder: data.requesterFolder,
      targetAgent: data.targetAgent,
    },
    'request_collaboration: agent dispatched, result will relay to requester',
  );
}

function handleNudgeAgent(
  targetFolder: string,
  targetJid: string,
  message: string,
): void {
  const targetInputDir = path.join(DATA_DIR, 'ipc', targetFolder, 'input');
  if (!fs.existsSync(targetInputDir)) {
    logger.warn(
      { targetFolder },
      'nudge_agent: target IPC input dir not found',
    );
    return;
  }
  const nudge = { type: 'message', text: message };
  const filename = `${Date.now()}-nudge.json`;
  fs.writeFileSync(path.join(targetInputDir, filename), JSON.stringify(nudge));
  logger.info(
    { targetFolder, targetJid, message },
    'nudge_agent: message injected',
  );
}

function handleRegisterAgent(
  agent: {
    name: string;
    displayName?: string;
    description: string;
    skills: string[];
    triggers: string[];
  },
  sourceGroup: string,
): void {
  const parentFolder = sourceGroup.match(/^(.+)_t_\d+/)?.[1] || sourceGroup;
  const registryPath = path.join(GROUPS_DIR, parentFolder, 'agents.json');

  let registry: Record<string, any> = {};
  try {
    if (fs.existsSync(registryPath)) {
      registry = JSON.parse(fs.readFileSync(registryPath, 'utf-8'));
    }
  } catch (err) {
    logger.warn(
      { err, registryPath },
      'Failed to read agents.json for register_agent',
    );
  }

  if (registry[agent.name]) {
    logger.info(
      { agentName: agent.name },
      'register_agent: updating existing agent',
    );
  }

  const entry: Record<string, any> = {
    description: agent.description,
    skills: agent.skills,
    triggers: agent.triggers,
  };
  if (agent.displayName) {
    entry.name = agent.displayName;
    if (!entry.triggers.includes(agent.displayName.toLowerCase())) {
      entry.triggers.push(agent.displayName.toLowerCase());
    }
  }
  registry[agent.name] = entry;

  fs.writeFileSync(registryPath, JSON.stringify(registry, null, 2) + '\n');

  const agentMemoryDir = path.join(GROUPS_DIR, 'agents', agent.name);
  fs.mkdirSync(agentMemoryDir, { recursive: true });
  const claudeMd = path.join(agentMemoryDir, 'CLAUDE.md');
  const heading = agent.displayName
    ? `# ${agent.displayName} — ${agent.name.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}`
    : `# ${agent.name}`;
  if (!fs.existsSync(claudeMd)) {
    fs.writeFileSync(claudeMd, `${heading}\n\n${agent.description}\n`);
  }

  logger.info(
    {
      agentName: agent.name,
      displayName: agent.displayName,
      skills: agent.skills,
      triggers: agent.triggers,
    },
    'register_agent: new agent registered',
  );
}
