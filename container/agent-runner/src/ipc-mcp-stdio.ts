/**
 * Stdio MCP Server for NanoClaw
 * Standalone process that agent teams subagents can inherit.
 * Reads context from environment variables, writes IPC files for the host.
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import fs from 'fs';
import path from 'path';
import { CronExpressionParser } from 'cron-parser';

const IPC_DIR = '/workspace/ipc';
const MESSAGES_DIR = path.join(IPC_DIR, 'messages');
const TASKS_DIR = path.join(IPC_DIR, 'tasks');

// Context from environment variables (set by the agent runner)
const chatJid = process.env.NANOCLAW_CHAT_JID!;
const groupFolder = process.env.NANOCLAW_GROUP_FOLDER!;
const isMain = process.env.NANOCLAW_IS_MAIN === '1';

function writeIpcFile(dir: string, data: object): string {
  fs.mkdirSync(dir, { recursive: true });

  const filename = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}.json`;
  const filepath = path.join(dir, filename);

  // Atomic write: temp file then rename
  const tempPath = `${filepath}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(data, null, 2));
  fs.renameSync(tempPath, filepath);

  return filename;
}

const server = new McpServer({
  name: 'nanoclaw',
  version: '1.0.0',
});

server.tool(
  'send_message',
  "Send a message to the user or group immediately while you're still running. Use this for progress updates or to send multiple messages. You can call this multiple times.",
  {
    text: z.string().describe('The message text to send'),
    sender: z.string().optional().describe('Your role/identity name (e.g. "Researcher"). When set, messages appear from a dedicated bot in Telegram.'),
  },
  async (args) => {
    const data: Record<string, string | undefined> = {
      type: 'message',
      chatJid,
      text: args.text,
      sender: args.sender || undefined,
      groupFolder,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(MESSAGES_DIR, data);

    return { content: [{ type: 'text' as const, text: 'Message sent.' }] };
  },
);

server.tool(
  'schedule_task',
  `Schedule a recurring or one-time task. The task will run as a full agent with access to all tools. Returns the task ID for future reference. To modify an existing task, use update_task instead.

CONTEXT MODE - Choose based on task type:
\u2022 "group": Task runs in the group's conversation context, with access to chat history. Use for tasks that need context about ongoing discussions, user preferences, or recent interactions.
\u2022 "isolated": Task runs in a fresh session with no conversation history. Use for independent tasks that don't need prior context. When using isolated mode, include all necessary context in the prompt itself.

If unsure which mode to use, you can ask the user. Examples:
- "Remind me about our discussion" \u2192 group (needs conversation context)
- "Check the weather every morning" \u2192 isolated (self-contained task)
- "Follow up on my request" \u2192 group (needs to know what was requested)
- "Generate a daily report" \u2192 isolated (just needs instructions in prompt)

MESSAGING BEHAVIOR - The task agent's output is sent to the user or group. It can also use send_message for immediate delivery, or wrap output in <internal> tags to suppress it. Include guidance in the prompt about whether the agent should:
\u2022 Always send a message (e.g., reminders, daily briefings)
\u2022 Only send a message when there's something to report (e.g., "notify me if...")
\u2022 Never send a message (background maintenance tasks)

SCHEDULE VALUE FORMAT (all times are LOCAL timezone):
\u2022 cron: Standard cron expression (e.g., "*/5 * * * *" for every 5 minutes, "0 9 * * *" for daily at 9am LOCAL time)
\u2022 interval: Milliseconds between runs (e.g., "300000" for 5 minutes, "3600000" for 1 hour)
\u2022 once: Local time WITHOUT "Z" suffix (e.g., "2026-02-01T15:30:00"). Do NOT use UTC/Z suffix.`,
  {
    prompt: z.string().describe('What the agent should do when the task runs. For isolated mode, include all necessary context here.'),
    schedule_type: z.enum(['cron', 'interval', 'once']).describe('cron=recurring at specific times, interval=recurring every N ms, once=run once at specific time'),
    schedule_value: z.string().describe('cron: "*/5 * * * *" | interval: milliseconds like "300000" | once: local timestamp like "2026-02-01T15:30:00" (no Z suffix!)'),
    context_mode: z.enum(['group', 'isolated']).default('group').describe('group=runs with chat history and memory, isolated=fresh session (include context in prompt)'),
    target_group_jid: z.string().optional().describe('(Main group only) JID of the group to schedule the task for. Defaults to the current group.'),
  },
  async (args) => {
    // Validate schedule_value before writing IPC
    if (args.schedule_type === 'cron') {
      try {
        CronExpressionParser.parse(args.schedule_value);
      } catch {
        return {
          content: [{ type: 'text' as const, text: `Invalid cron: "${args.schedule_value}". Use format like "0 9 * * *" (daily 9am) or "*/5 * * * *" (every 5 min).` }],
          isError: true,
        };
      }
    } else if (args.schedule_type === 'interval') {
      const ms = parseInt(args.schedule_value, 10);
      if (isNaN(ms) || ms <= 0) {
        return {
          content: [{ type: 'text' as const, text: `Invalid interval: "${args.schedule_value}". Must be positive milliseconds (e.g., "300000" for 5 min).` }],
          isError: true,
        };
      }
    } else if (args.schedule_type === 'once') {
      if (/[Zz]$/.test(args.schedule_value) || /[+-]\d{2}:\d{2}$/.test(args.schedule_value)) {
        return {
          content: [{ type: 'text' as const, text: `Timestamp must be local time without timezone suffix. Got "${args.schedule_value}" — use format like "2026-02-01T15:30:00".` }],
          isError: true,
        };
      }
      const date = new Date(args.schedule_value);
      if (isNaN(date.getTime())) {
        return {
          content: [{ type: 'text' as const, text: `Invalid timestamp: "${args.schedule_value}". Use local time format like "2026-02-01T15:30:00".` }],
          isError: true,
        };
      }
    }

    // Non-main groups can only schedule for themselves
    const targetJid = isMain && args.target_group_jid ? args.target_group_jid : chatJid;

    const taskId = `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    const data = {
      type: 'schedule_task',
      taskId,
      prompt: args.prompt,
      schedule_type: args.schedule_type,
      schedule_value: args.schedule_value,
      context_mode: args.context_mode || 'group',
      targetJid,
      createdBy: groupFolder,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return {
      content: [{ type: 'text' as const, text: `Task ${taskId} scheduled: ${args.schedule_type} - ${args.schedule_value}` }],
    };
  },
);

server.tool(
  'list_tasks',
  "List all scheduled tasks. From main: shows all tasks. From other groups: shows only that group's tasks.",
  {},
  async () => {
    const tasksFile = path.join(IPC_DIR, 'current_tasks.json');

    try {
      if (!fs.existsSync(tasksFile)) {
        return { content: [{ type: 'text' as const, text: 'No scheduled tasks found.' }] };
      }

      const allTasks = JSON.parse(fs.readFileSync(tasksFile, 'utf-8'));

      const tasks = isMain
        ? allTasks
        : allTasks.filter((t: { groupFolder: string }) => t.groupFolder === groupFolder);

      if (tasks.length === 0) {
        return { content: [{ type: 'text' as const, text: 'No scheduled tasks found.' }] };
      }

      const formatted = tasks
        .map(
          (t: { id: string; prompt: string; schedule_type: string; schedule_value: string; status: string; next_run: string }) =>
            `- [${t.id}] ${t.prompt.slice(0, 50)}... (${t.schedule_type}: ${t.schedule_value}) - ${t.status}, next: ${t.next_run || 'N/A'}`,
        )
        .join('\n');

      return { content: [{ type: 'text' as const, text: `Scheduled tasks:\n${formatted}` }] };
    } catch (err) {
      return {
        content: [{ type: 'text' as const, text: `Error reading tasks: ${err instanceof Error ? err.message : String(err)}` }],
      };
    }
  },
);

server.tool(
  'pause_task',
  'Pause a scheduled task. It will not run until resumed.',
  { task_id: z.string().describe('The task ID to pause') },
  async (args) => {
    const data = {
      type: 'pause_task',
      taskId: args.task_id,
      groupFolder,
      isMain,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return { content: [{ type: 'text' as const, text: `Task ${args.task_id} pause requested.` }] };
  },
);

server.tool(
  'resume_task',
  'Resume a paused task.',
  { task_id: z.string().describe('The task ID to resume') },
  async (args) => {
    const data = {
      type: 'resume_task',
      taskId: args.task_id,
      groupFolder,
      isMain,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return { content: [{ type: 'text' as const, text: `Task ${args.task_id} resume requested.` }] };
  },
);

server.tool(
  'cancel_task',
  'Cancel and delete a scheduled task.',
  { task_id: z.string().describe('The task ID to cancel') },
  async (args) => {
    const data = {
      type: 'cancel_task',
      taskId: args.task_id,
      groupFolder,
      isMain,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return { content: [{ type: 'text' as const, text: `Task ${args.task_id} cancellation requested.` }] };
  },
);

server.tool(
  'update_task',
  'Update an existing scheduled task. Only provided fields are changed; omitted fields stay the same.',
  {
    task_id: z.string().describe('The task ID to update'),
    prompt: z.string().optional().describe('New prompt for the task'),
    schedule_type: z.enum(['cron', 'interval', 'once']).optional().describe('New schedule type'),
    schedule_value: z.string().optional().describe('New schedule value (see schedule_task for format)'),
  },
  async (args) => {
    // Validate schedule_value if provided
    if (args.schedule_type === 'cron' || (!args.schedule_type && args.schedule_value)) {
      if (args.schedule_value) {
        try {
          CronExpressionParser.parse(args.schedule_value);
        } catch {
          return {
            content: [{ type: 'text' as const, text: `Invalid cron: "${args.schedule_value}".` }],
            isError: true,
          };
        }
      }
    }
    if (args.schedule_type === 'interval' && args.schedule_value) {
      const ms = parseInt(args.schedule_value, 10);
      if (isNaN(ms) || ms <= 0) {
        return {
          content: [{ type: 'text' as const, text: `Invalid interval: "${args.schedule_value}".` }],
          isError: true,
        };
      }
    }

    const data: Record<string, string | undefined> = {
      type: 'update_task',
      taskId: args.task_id,
      groupFolder,
      isMain: String(isMain),
      timestamp: new Date().toISOString(),
    };
    if (args.prompt !== undefined) data.prompt = args.prompt;
    if (args.schedule_type !== undefined) data.schedule_type = args.schedule_type;
    if (args.schedule_value !== undefined) data.schedule_value = args.schedule_value;

    writeIpcFile(TASKS_DIR, data);

    return { content: [{ type: 'text' as const, text: `Task ${args.task_id} update requested.` }] };
  },
);

server.tool(
  'register_group',
  `Register a new chat/group so the agent can respond to messages there. Main group only.

Use available_groups.json to find the JID for a group. The folder name must be channel-prefixed: "{channel}_{group-name}" (e.g., "whatsapp_family-chat", "telegram_dev-team", "discord_general"). Use lowercase with hyphens for the group name part.`,
  {
    jid: z.string().describe('The chat JID (e.g., "120363336345536173@g.us", "tg:-1001234567890", "dc:1234567890123456")'),
    name: z.string().describe('Display name for the group'),
    folder: z.string().describe('Channel-prefixed folder name (e.g., "whatsapp_family-chat", "telegram_dev-team")'),
    trigger: z.string().describe('Trigger word (e.g., "@Andy")'),
  },
  async (args) => {
    if (!isMain) {
      return {
        content: [{ type: 'text' as const, text: 'Only the main group can register new groups.' }],
        isError: true,
      };
    }

    const data = {
      type: 'register_group',
      jid: args.jid,
      name: args.name,
      folder: args.folder,
      trigger: args.trigger,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return {
      content: [{ type: 'text' as const, text: `Group "${args.name}" registered. It will start receiving messages immediately.` }],
    };
  },
);

server.tool(
  'dispatch_thread',
  `Dispatch a task to a specific agent for isolated execution in a new Slack thread.
Each agent has its own skills and memory. Choose the right agent_type based on the task.

Available agents are listed in your system prompt. Use "general" for tasks that don't match any specialist agent.

You are the brain — plan and dispatch, never execute.`,
  {
    agent_type: z.string().describe('Agent type to dispatch to (e.g. "ref-video", "content-generate", "general")'),
    description: z.string().describe('Short task summary (displayed as thread header message)'),
    prompt: z.string().describe('Full task instructions for the agent. Include all necessary context — the agent has no access to this conversation.'),
  },
  async (args) => {
    if (!isMain) {
      return {
        content: [{ type: 'text' as const, text: 'Only the main (brain) agent can dispatch threads.' }],
        isError: true,
      };
    }

    const data = {
      type: 'dispatch_thread',
      chatJid,
      agentType: args.agent_type,
      description: args.description,
      prompt: args.prompt,
      groupFolder,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return {
      content: [{ type: 'text' as const, text: `Dispatched to [${args.agent_type}] agent: "${args.description}". Results will be relayed back to you.` }],
    };
  },
);

server.tool(
  'register_agent',
  `Register a new agent type in the agency. Only the HR agent (or brain) should use this.
Creates the agent definition so it can be dispatched to in the future.
The new agent will have its own isolated memory and the specified skills.`,
  {
    name: z.string().describe('Agent identifier (lowercase, hyphenated, e.g. "data-analyst")'),
    displayName: z.string().describe('Human-readable name for this agent (e.g. "Saga", "Muse"). Used as the agent\'s identity.'),
    description: z.string().describe('What this agent does (shown to the brain for dispatch decisions)'),
    skills: z.array(z.string()).describe('List of skill names this agent should have access to'),
    triggers: z.array(z.string()).describe('Keywords that help the brain identify when to dispatch to this agent'),
  },
  async (args) => {
    const data = {
      type: 'register_agent',
      agentName: args.name,
      agentDisplayName: args.displayName,
      agentDescription: args.description,
      agentSkills: args.skills,
      agentTriggers: args.triggers,
      groupFolder,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return {
      content: [{ type: 'text' as const, text: `Agent "${args.name}" registered. The brain can now dispatch tasks to it.` }],
    };
  },
);

server.tool(
  'request_collaboration',
  `Request another agent to perform a task and return results to you.
Use when you need data or work from a specialist agent (e.g. ask Echo for social media data, ask Muse to generate content).

The target agent will be dispatched automatically. When it finishes, its result will be sent back to your conversation so you can continue.

You should continue working on other parts of your task while waiting. The result will arrive as a new message.`,
  {
    target_agent: z.string().describe('Agent type to request help from (e.g. "social-media-manager", "content-creator", "dev", "researcher")'),
    task: z.string().describe('What you need the agent to do. Be specific — include all context needed.'),
  },
  async (args) => {
    const data = {
      type: 'request_collaboration',
      chatJid,
      requesterFolder: groupFolder,
      targetAgent: args.target_agent,
      task: args.task,
      timestamp: new Date().toISOString(),
    };

    writeIpcFile(TASKS_DIR, data);

    return {
      content: [{ type: 'text' as const, text: `Collaboration request sent to [${args.target_agent}]. Continue working — their result will arrive as a message when ready.` }],
    };
  },
);

// Brain-only: check status of dispatched agents
if (isMain) {
  server.tool(
    'check_agents',
    `Check the real-time status of all agent containers and dispatched threads.
Shows which agents are running, completed, or dead.
Use this when agents haven't responded or you want to monitor progress.`,
    {},
    async () => {
      const statusPath = path.join(IPC_DIR, 'container_status.json');
      const inputDir = path.join(IPC_DIR, 'input');

      // Read comprehensive status from health monitor
      let status: {
        timestamp?: string;
        runningContainers?: string[];
        activeContainers?: Array<{
          containerName: string | null;
          groupFolder: string | null;
          active: boolean;
          idleWaiting: boolean;
          isWorker: boolean;
          alive: boolean;
        }>;
        threadGroups?: Array<{
          folder: string;
          name: string;
          agentType?: string;
          hasContainer: boolean;
        }>;
      } = {};
      try {
        if (fs.existsSync(statusPath)) {
          status = JSON.parse(fs.readFileSync(statusPath, 'utf-8'));
        }
      } catch {}

      // Check IPC input dir for pending messages
      let pendingCount = 0;
      try {
        if (fs.existsSync(inputDir)) {
          pendingCount = fs.readdirSync(inputDir).filter(f => f.endsWith('.json')).length;
        }
      } catch {}

      const lines: string[] = ['## Agent Status Report'];
      if (status.timestamp) {
        lines.push(`Last updated: ${status.timestamp}`);
      }
      lines.push('');

      // Running containers
      const containers = status.runningContainers || [];
      lines.push(`### Running Containers (${containers.length})`);
      if (containers.length > 0) {
        for (const c of containers) {
          lines.push(`- 🟢 ${c}`);
        }
      } else {
        lines.push('- None');
      }
      lines.push('');

      // Active containers with queue tracking
      const active = status.activeContainers || [];
      if (active.length > 0) {
        lines.push(`### Queue-tracked Containers (${active.length})`);
        for (const c of active) {
          const emoji = c.alive ? '🟢' : '🔴';
          const type = c.isWorker ? 'worker' : 'brain';
          const idle = c.idleWaiting ? ' (idle)' : '';
          lines.push(`- ${emoji} ${c.containerName || 'unknown'} [${type}]${idle}`);
        }
        lines.push('');
      }

      // Thread groups (dispatched agents)
      const threads = status.threadGroups || [];
      if (threads.length > 0) {
        lines.push(`### Dispatched Agent Threads (${threads.length})`);
        for (const t of threads.slice(-20)) {
          const emoji = t.hasContainer ? '🟢 running' : '⚪ completed';
          const agentLabel = t.agentType ? `[${t.agentType}]` : '';
          lines.push(`- ${emoji} ${t.name} ${agentLabel}`);
        }
        lines.push('');
      }

      lines.push(`### Pending IPC messages for you: ${pendingCount}`);

      if (active.some(c => !c.alive)) {
        lines.push('');
        lines.push('⚠️ Dead containers detected — health monitor will auto-clean.');
      }

      return { content: [{ type: 'text' as const, text: lines.join('\n') }] };
    },
  );

  server.tool(
    'retry_agent',
    `Restart a stuck or failed agent by sending it a nudge message.
Use when an agent hasn't responded after being dispatched.`,
    {
      agent_type: z.string().describe('The agent_type that is stuck (e.g. "hr", "general")'),
      message: z.string().describe('Message to send to the agent, e.g. "请立即完成你的任务并回复"').default('请立即完成你的任务并回复结果。'),
    },
    async (args) => {
      // Find the thread folder for this agent type
      const availableGroupsPath = path.join(IPC_DIR, 'available_groups.json');
      let groups: Array<{ jid: string; name: string; folder: string }> = [];
      try {
        if (fs.existsSync(availableGroupsPath)) {
          groups = JSON.parse(fs.readFileSync(availableGroupsPath, 'utf-8'));
        }
      } catch {}

      const match = groups.find(g => g.name.toLowerCase().includes(args.agent_type.toLowerCase()));
      if (!match) {
        return { content: [{ type: 'text' as const, text: `No active thread found for agent type "${args.agent_type}". It may not have been dispatched yet.` }] };
      }

      // Write a nudge message to the agent's IPC input
      const agentInputDir = `/workspace/ipc/../ipc-nudge`;
      const data = {
        type: 'nudge_agent',
        targetFolder: match.folder,
        targetJid: match.jid,
        message: args.message,
        groupFolder,
        timestamp: new Date().toISOString(),
      };

      writeIpcFile(TASKS_DIR, data);

      return {
        content: [{ type: 'text' as const, text: `Sent nudge to ${match.name} (${match.folder}): "${args.message}"` }],
      };
    },
  );
}

// Start the stdio transport
const transport = new StdioServerTransport();
await server.connect(transport);
