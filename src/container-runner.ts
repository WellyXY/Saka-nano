/**
 * Container Runner for NanoClaw
 * Spawns agent execution in containers and handles IPC
 */
import { ChildProcess, exec, spawn } from 'child_process';
import fs from 'fs';
import path from 'path';

import {
  CONTAINER_IMAGE,
  CONTAINER_MAX_OUTPUT_SIZE,
  CONTAINER_TIMEOUT,
  CREDENTIAL_PROXY_PORT,
  DATA_DIR,
  GROUPS_DIR,
  IDLE_TIMEOUT,
  TIMEZONE,
} from './config.js';
import { readEnvFile } from './env.js';
import { resolveGroupFolderPath, resolveGroupIpcPath } from './group-folder.js';
import { logger } from './logger.js';
import {
  CONTAINER_HOST_GATEWAY,
  CONTAINER_RUNTIME_BIN,
  hostGatewayArgs,
  readonlyMountArgs,
  stopContainer,
} from './container-runtime.js';
import { detectAuthMode } from './credential-proxy.js';
import { validateAdditionalMounts } from './mount-security.js';
import { AgentDefinition, RegisteredGroup } from './types.js';

// Sentinel markers for robust output parsing (must match agent-runner)
const OUTPUT_START_MARKER = '---NANOCLAW_OUTPUT_START---';
const OUTPUT_END_MARKER = '---NANOCLAW_OUTPUT_END---';

export interface ImageAttachment {
  path: string;
  mediaType: string;
}

export interface ContainerInput {
  prompt: string;
  sessionId?: string;
  groupFolder: string;
  chatJid: string;
  isMain: boolean;
  isScheduledTask?: boolean;
  assistantName?: string;
  imageAttachments?: ImageAttachment[];
}

export interface ContainerOutput {
  status: 'success' | 'error';
  result: string | null;
  newSessionId?: string;
  error?: string;
}

interface VolumeMount {
  hostPath: string;
  containerPath: string;
  readonly: boolean;
}

/**
 * Returns the display name for a group's agent type from agents.json.
 */
function getAgentName(group: RegisteredGroup): string | null {
  if (!group.agentType) return null;
  const threadMatch = group.folder.match(/^(.+)_t_\d+/);
  const parentFolder = threadMatch ? threadMatch[1] : group.folder;
  const registryPath = path.join(GROUPS_DIR, parentFolder, 'agents.json');
  try {
    if (fs.existsSync(registryPath)) {
      const registry = JSON.parse(fs.readFileSync(registryPath, 'utf-8'));
      return registry[group.agentType]?.name || null;
    }
  } catch {}
  return null;
}

/**
 * Returns the skill list for a group's agent type. null = all skills (general agent).
 * Brain agents return empty array (no skills).
 */
function getAgentSkills(
  group: RegisteredGroup,
  isMain: boolean,
): string[] | null {
  if (isMain) return [];
  if (!group.agentType || group.agentType === 'general') return null;

  // Try loading agent registry from the parent group's folder
  const threadMatch = group.folder.match(/^(.+)_t_\d+/);
  const parentFolder = threadMatch ? threadMatch[1] : group.folder;
  const registryPath = path.join(GROUPS_DIR, parentFolder, 'agents.json');

  try {
    if (fs.existsSync(registryPath)) {
      const registry: Record<string, AgentDefinition> = JSON.parse(
        fs.readFileSync(registryPath, 'utf-8'),
      );
      const def = registry[group.agentType];
      if (def && def.skills.length > 0) return def.skills;
    }
  } catch (err) {
    logger.warn({ err, registryPath }, 'Failed to read agents.json');
  }

  return null;
}

/**
 * After a worker container finishes, sync any NEW skills it created
 * back to the parent session and register them in agents.json.
 * This prevents skills created during a session from being lost.
 */
export function syncWorkerSkillsBack(group: RegisteredGroup): void {
  if (group.isMain || !group.agentType) return;

  const threadMatch = group.folder.match(/^(.+)_t_\d+/);
  if (!threadMatch) return;

  const parentFolder = threadMatch[1];
  const workerSkillsDir = path.join(
    DATA_DIR,
    'sessions',
    group.folder,
    '.claude',
    'skills',
  );
  const parentSkillsDir = path.join(
    DATA_DIR,
    'sessions',
    parentFolder,
    '.claude',
    'skills',
  );

  if (!fs.existsSync(workerSkillsDir)) return;

  const workerSkills = fs
    .readdirSync(workerSkillsDir)
    .filter((d) => fs.statSync(path.join(workerSkillsDir, d)).isDirectory());
  const parentSkills = new Set(
    fs.existsSync(parentSkillsDir)
      ? fs
          .readdirSync(parentSkillsDir)
          .filter((d) =>
            fs.statSync(path.join(parentSkillsDir, d)).isDirectory(),
          )
      : [],
  );

  const newSkills: string[] = [];
  for (const skill of workerSkills) {
    if (!parentSkills.has(skill)) {
      const src = path.join(workerSkillsDir, skill);
      const dst = path.join(parentSkillsDir, skill);
      try {
        fs.mkdirSync(parentSkillsDir, { recursive: true });
        fs.cpSync(src, dst, { recursive: true });
        newSkills.push(skill);
      } catch (err) {
        logger.warn(
          { err, skill },
          'Failed to sync worker skill back to parent',
        );
      }
    }
  }

  if (newSkills.length === 0) return;

  // Also register new skills in agents.json so future workers of this type get them
  const registryPath = path.join(GROUPS_DIR, parentFolder, 'agents.json');
  try {
    if (fs.existsSync(registryPath)) {
      const registry: Record<string, AgentDefinition> = JSON.parse(
        fs.readFileSync(registryPath, 'utf-8'),
      );
      const def = registry[group.agentType];
      if (def) {
        const existing = new Set(def.skills);
        for (const skill of newSkills) {
          if (!existing.has(skill)) {
            def.skills.push(skill);
          }
        }
        fs.writeFileSync(
          registryPath,
          JSON.stringify(registry, null, 2) + '\n',
        );
      }
    }
  } catch (err) {
    logger.warn({ err }, 'Failed to update agents.json with new skills');
  }

  logger.info(
    { agent: group.agentType, newSkills },
    'Synced worker-created skills back to parent session',
  );
}

function buildVolumeMounts(
  group: RegisteredGroup,
  isMain: boolean,
): VolumeMount[] {
  const mounts: VolumeMount[] = [];
  const projectRoot = process.cwd();
  const groupDir = resolveGroupFolderPath(group.folder);

  // Thread groups share their parent's group folder so they inherit
  // CLAUDE.md (group memory), conversations/, and other persistent data.
  const threadFolderMatch = group.folder.match(/^(.+)_t_\d+/);
  const parentGroupDir = threadFolderMatch
    ? resolveGroupFolderPath(threadFolderMatch[1])
    : null;

  if (isMain) {
    // Main gets the project root read-only. Writable paths the agent needs
    // (group folder, IPC, .claude/) are mounted separately below.
    // Read-only prevents the agent from modifying host application code
    // (src/, dist/, package.json, etc.) which would bypass the sandbox
    // entirely on next restart.
    mounts.push({
      hostPath: projectRoot,
      containerPath: '/workspace/project',
      readonly: true,
    });

    // Main also gets its group folder as the working directory
    mounts.push({
      hostPath: groupDir,
      containerPath: '/workspace/group',
      readonly: false,
    });
  } else {
    // Thread groups mount the parent's group folder so they share memory.
    // Non-thread groups mount their own folder.
    mounts.push({
      hostPath: parentGroupDir || groupDir,
      containerPath: '/workspace/group',
      readonly: false,
    });

    // Global memory directory (read-only for non-main)
    // Only directory mounts are supported, not file mounts
    const globalDir = path.join(GROUPS_DIR, 'global');
    if (fs.existsSync(globalDir)) {
      mounts.push({
        hostPath: globalDir,
        containerPath: '/workspace/global',
        readonly: true,
      });
    }
  }

  // Per-group Claude sessions directory.
  // Brain and non-thread groups use their own session folder.
  // Thread workers with agentType get their OWN .claude/ (not parent's)
  // so skill filtering doesn't destroy the parent's installed skills.
  // Thread workers without agentType share parent's .claude/ (legacy behavior).
  const threadMatch = group.folder.match(/^(.+)_t_\d+/);
  const parentSessionFolder = threadMatch ? threadMatch[1] : null;
  const sessionFolder =
    threadMatch && group.agentType
      ? group.folder // agent workers get isolated session
      : parentSessionFolder || group.folder; // legacy: share parent
  const groupSessionsDir = path.join(
    DATA_DIR,
    'sessions',
    sessionFolder,
    '.claude',
  );
  fs.mkdirSync(groupSessionsDir, { recursive: true });
  const settingsFile = path.join(groupSessionsDir, 'settings.json');
  if (!fs.existsSync(settingsFile)) {
    fs.writeFileSync(
      settingsFile,
      JSON.stringify(
        {
          env: {
            // Enable agent swarms (subagent orchestration)
            // https://code.claude.com/docs/en/agent-teams#orchestrate-teams-of-claude-code-sessions
            CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS: '1',
            // Load CLAUDE.md from additional mounted directories
            // https://code.claude.com/docs/en/memory#load-memory-from-additional-directories
            CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD: '1',
            // Enable Claude's memory feature (persists user preferences between sessions)
            // https://code.claude.com/docs/en/memory#manage-auto-memory
            CLAUDE_CODE_DISABLE_AUTO_MEMORY: '0',
          },
        },
        null,
        2,
      ) + '\n',
    );
  }

  // Sync skills: workers get a filtered copy from the parent session's skills.
  // The parent session (e.g. slack_main) holds the master set of installed skills.
  // container/skills/ provides bundled skills (e.g. agent-browser).
  const bundledSkillsSrc = path.join(process.cwd(), 'container', 'skills');
  const parentSkillsSrc = parentSessionFolder
    ? path.join(DATA_DIR, 'sessions', parentSessionFolder, '.claude', 'skills')
    : null;
  const skillsDst = path.join(groupSessionsDir, 'skills');
  const allowedSkills = getAgentSkills(group, isMain);

  if (!isMain && group.agentType) {
    // Agent workers: build skills dir from parent + bundled, filtered by agent type
    if (fs.existsSync(skillsDst)) {
      fs.rmSync(skillsDst, { recursive: true, force: true });
    }
    fs.mkdirSync(skillsDst, { recursive: true });

    // Copy from parent session skills (the master set)
    if (parentSkillsSrc && fs.existsSync(parentSkillsSrc)) {
      for (const skillDir of fs.readdirSync(parentSkillsSrc)) {
        const srcDir = path.join(parentSkillsSrc, skillDir);
        if (!fs.statSync(srcDir).isDirectory()) continue;
        if (allowedSkills === null || allowedSkills.includes(skillDir)) {
          fs.cpSync(srcDir, path.join(skillsDst, skillDir), {
            recursive: true,
          });
        }
      }
    }

    // Also copy from bundled skills (container/skills/)
    if (fs.existsSync(bundledSkillsSrc)) {
      for (const skillDir of fs.readdirSync(bundledSkillsSrc)) {
        const srcDir = path.join(bundledSkillsSrc, skillDir);
        if (!fs.statSync(srcDir).isDirectory()) continue;
        if (allowedSkills === null || allowedSkills.includes(skillDir)) {
          const dstDir = path.join(skillsDst, skillDir);
          if (!fs.existsSync(dstDir)) {
            fs.cpSync(srcDir, dstDir, { recursive: true });
          }
        }
      }
    }
  } else if (!isMain) {
    // Legacy workers (no agentType): just sync bundled skills into shared session
    if (fs.existsSync(bundledSkillsSrc)) {
      fs.mkdirSync(skillsDst, { recursive: true });
      for (const skillDir of fs.readdirSync(bundledSkillsSrc)) {
        const srcDir = path.join(bundledSkillsSrc, skillDir);
        if (!fs.statSync(srcDir).isDirectory()) continue;
        const dstDir = path.join(skillsDst, skillDir);
        fs.cpSync(srcDir, dstDir, { recursive: true });
      }
    }
  }

  // Copy scripts from parent session so skill JS files are available
  if (!isMain && group.agentType && parentSessionFolder) {
    const parentScripts = path.join(
      DATA_DIR,
      'sessions',
      parentSessionFolder,
      '.claude',
      'scripts',
    );
    const workerScripts = path.join(groupSessionsDir, 'scripts');
    if (fs.existsSync(parentScripts)) {
      fs.mkdirSync(workerScripts, { recursive: true });
      fs.cpSync(parentScripts, workerScripts, { recursive: true });
    }
  }

  mounts.push({
    hostPath: groupSessionsDir,
    containerPath: '/home/node/.claude',
    readonly: false,
  });

  if (isMain) {
    // Brain: mount empty skills (prevent SDK auto-discovery — brain dispatches, never executes)
    const emptySkillsDir = path.join(DATA_DIR, 'brain-empty-skills');
    fs.mkdirSync(emptySkillsDir, { recursive: true });
    mounts.push({
      hostPath: emptySkillsDir,
      containerPath: '/home/node/.claude/skills',
      readonly: true,
    });

    // Brain: readonly mount of all agent memory directories for cross-agent awareness
    const agentsMemoryDir = path.join(GROUPS_DIR, 'agents');
    if (fs.existsSync(agentsMemoryDir)) {
      mounts.push({
        hostPath: agentsMemoryDir,
        containerPath: '/workspace/agents',
        readonly: true,
      });
    }
  } else if (group.agentType) {
    // Worker: mount this agent's persistent memory as writable
    const agentMemoryDir = path.join(GROUPS_DIR, 'agents', group.agentType);
    fs.mkdirSync(agentMemoryDir, { recursive: true });
    mounts.push({
      hostPath: agentMemoryDir,
      containerPath: '/workspace/agent-memory',
      readonly: false,
    });
  }

  // Per-group IPC namespace: each group gets its own IPC directory
  // This prevents cross-group privilege escalation via IPC
  const groupIpcDir = resolveGroupIpcPath(group.folder);
  fs.mkdirSync(path.join(groupIpcDir, 'messages'), { recursive: true });
  fs.mkdirSync(path.join(groupIpcDir, 'tasks'), { recursive: true });
  fs.mkdirSync(path.join(groupIpcDir, 'input'), { recursive: true });

  // Thread groups: copy parent's attachments into the thread IPC dir
  // (symlinks don't work inside Apple Container — they point to absolute
  // host paths outside the container's VirtioFS mounts, and nested mounts
  // are not supported either).
  if (threadFolderMatch) {
    const parentIpcDir = resolveGroupIpcPath(threadFolderMatch[1]);
    const parentAttachments = path.join(parentIpcDir, 'attachments');
    const threadAttachments = path.join(groupIpcDir, 'attachments');
    if (fs.existsSync(parentAttachments)) {
      // Remove stale symlink if present, then copy actual files
      try {
        fs.rmSync(threadAttachments, { recursive: true, force: true });
      } catch {}
      fs.cpSync(parentAttachments, threadAttachments, { recursive: true });
    }
  }

  mounts.push({
    hostPath: groupIpcDir,
    containerPath: '/workspace/ipc',
    readonly: false,
  });

  // Sync agent-runner source on every container startup to prevent stale code.
  // Previous approach only copied when dir didn't exist, causing subtle bugs
  // when agent-runner code was updated but containers ran old versions.
  const agentRunnerSrc = path.join(
    projectRoot,
    'container',
    'agent-runner',
    'src',
  );
  const groupAgentRunnerDir = path.join(
    DATA_DIR,
    'sessions',
    group.folder,
    'agent-runner-src',
  );
  if (fs.existsSync(agentRunnerSrc)) {
    fs.mkdirSync(groupAgentRunnerDir, { recursive: true });
    fs.cpSync(agentRunnerSrc, groupAgentRunnerDir, { recursive: true });
  }
  mounts.push({
    hostPath: groupAgentRunnerDir,
    containerPath: '/app/src',
    readonly: false,
  });

  // Additional mounts validated against external allowlist (tamper-proof from containers)
  if (group.containerConfig?.additionalMounts) {
    const validatedMounts = validateAdditionalMounts(
      group.containerConfig.additionalMounts,
      group.name,
      isMain,
    );
    mounts.push(...validatedMounts);
  }

  return mounts;
}

function buildContainerArgs(
  mounts: VolumeMount[],
  containerName: string,
  isMain: boolean,
): string[] {
  const args: string[] = ['run', '-i', '--rm', '--name', containerName];

  // Pass host timezone so container's local time matches the user's
  args.push('-e', `TZ=${TIMEZONE}`);

  // Route API traffic through the credential proxy (containers never see real secrets)
  args.push(
    '-e',
    `ANTHROPIC_BASE_URL=http://${CONTAINER_HOST_GATEWAY}:${CREDENTIAL_PROXY_PORT}`,
  );

  // Mirror the host's auth method with a placeholder value.
  const authMode = detectAuthMode();
  if (authMode === 'api-key') {
    args.push('-e', 'ANTHROPIC_API_KEY=placeholder');
  } else {
    args.push('-e', 'CLAUDE_CODE_OAUTH_TOKEN=placeholder');
  }

  // Pass model override so the SDK inside the container uses the configured model
  const {
    CLAUDE_MODEL: modelOverride,
    GMI_API_KEY: gmiKey,
    TIKHUB_API_KEY: tikhubKey,
    GITHUB_TOKEN: ghToken,
    LATE_API_KEY: lateKey,
  } = readEnvFile([
    'CLAUDE_MODEL',
    'GMI_API_KEY',
    'TIKHUB_API_KEY',
    'GITHUB_TOKEN',
    'LATE_API_KEY',
  ]);
  const claudeModel = process.env.CLAUDE_MODEL || modelOverride;
  if (claudeModel) {
    args.push('-e', `CLAUDE_MODEL=${claudeModel}`);
  }

  // Expose API keys for skills that call external services directly
  const gmiApiKey = process.env.GMI_API_KEY || gmiKey;
  if (gmiApiKey) {
    args.push('-e', `GMI_API_KEY=${gmiApiKey}`);
  }

  const tikhubApiKey = process.env.TIKHUB_API_KEY || tikhubKey;
  if (tikhubApiKey) {
    args.push('-e', `TIKHUB_API_KEY=${tikhubApiKey}`);
  }

  const githubToken = process.env.GITHUB_TOKEN || ghToken;
  if (githubToken) {
    args.push('-e', `GITHUB_TOKEN=${githubToken}`);
  }

  const lateApiKey = process.env.LATE_API_KEY || lateKey;
  if (lateApiKey) {
    args.push('-e', `LATE_API_KEY=${lateApiKey}`);
  }

  // Runtime-specific args for host gateway resolution
  args.push(...hostGatewayArgs());

  // Run as host user so bind-mounted files are accessible.
  // Skip when running as root (uid 0), as the container's node user (uid 1000),
  // or when getuid is unavailable (native Windows without WSL).
  const hostUid = process.getuid?.();
  const hostGid = process.getgid?.();
  if (hostUid != null && hostUid !== 0 && hostUid !== 1000) {
    if (isMain) {
      // Main containers start as root so the entrypoint can mount --bind
      // to shadow .env. Privileges are dropped via setpriv in entrypoint.sh.
      args.push('-e', `RUN_UID=${hostUid}`);
      args.push('-e', `RUN_GID=${hostGid}`);
    } else {
      args.push('--user', `${hostUid}:${hostGid}`);
    }
    args.push('-e', 'HOME=/home/node');
  }

  for (const mount of mounts) {
    if (mount.readonly) {
      args.push(...readonlyMountArgs(mount.hostPath, mount.containerPath));
    } else {
      args.push('-v', `${mount.hostPath}:${mount.containerPath}`);
    }
  }

  args.push(CONTAINER_IMAGE);

  return args;
}

export async function runContainerAgent(
  group: RegisteredGroup,
  input: ContainerInput,
  onProcess: (proc: ChildProcess, containerName: string) => void,
  onOutput?: (output: ContainerOutput) => Promise<void>,
): Promise<ContainerOutput> {
  const startTime = Date.now();

  // Override assistantName with agent-specific name for workers
  if (!input.isMain && group.agentType) {
    const agentDisplayName = getAgentName(group);
    if (agentDisplayName) {
      input.assistantName = agentDisplayName;
    }
  }

  const groupDir = resolveGroupFolderPath(group.folder);
  fs.mkdirSync(groupDir, { recursive: true });

  const mounts = buildVolumeMounts(group, input.isMain);
  const safeName = group.folder.replace(/[^a-zA-Z0-9-]/g, '-');
  const containerName = `nanoclaw-${safeName}-${Date.now()}`;
  const containerArgs = buildContainerArgs(mounts, containerName, input.isMain);

  logger.debug(
    {
      group: group.name,
      containerName,
      mounts: mounts.map(
        (m) =>
          `${m.hostPath} -> ${m.containerPath}${m.readonly ? ' (ro)' : ''}`,
      ),
      containerArgs: containerArgs.join(' '),
    },
    'Container mount configuration',
  );

  logger.info(
    {
      group: group.name,
      containerName,
      mountCount: mounts.length,
      isMain: input.isMain,
    },
    'Spawning container agent',
  );

  const logsDir = path.join(groupDir, 'logs');
  fs.mkdirSync(logsDir, { recursive: true });

  return new Promise((resolve) => {
    const container = spawn(CONTAINER_RUNTIME_BIN, containerArgs, {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    onProcess(container, containerName);

    let stdout = '';
    let stderr = '';
    let stdoutTruncated = false;
    let stderrTruncated = false;

    container.stdin.write(JSON.stringify(input));
    container.stdin.end();

    // Streaming output: parse OUTPUT_START/END marker pairs as they arrive
    let parseBuffer = '';
    let newSessionId: string | undefined;
    let outputChain = Promise.resolve();

    container.stdout.on('data', (data) => {
      const chunk = data.toString();

      // Always accumulate for logging
      if (!stdoutTruncated) {
        const remaining = CONTAINER_MAX_OUTPUT_SIZE - stdout.length;
        if (chunk.length > remaining) {
          stdout += chunk.slice(0, remaining);
          stdoutTruncated = true;
          logger.warn(
            { group: group.name, size: stdout.length },
            'Container stdout truncated due to size limit',
          );
        } else {
          stdout += chunk;
        }
      }

      // Stream-parse for output markers
      if (onOutput) {
        parseBuffer += chunk;
        let startIdx: number;
        while ((startIdx = parseBuffer.indexOf(OUTPUT_START_MARKER)) !== -1) {
          const endIdx = parseBuffer.indexOf(OUTPUT_END_MARKER, startIdx);
          if (endIdx === -1) break; // Incomplete pair, wait for more data

          const jsonStr = parseBuffer
            .slice(startIdx + OUTPUT_START_MARKER.length, endIdx)
            .trim();
          parseBuffer = parseBuffer.slice(endIdx + OUTPUT_END_MARKER.length);

          try {
            const parsed: ContainerOutput = JSON.parse(jsonStr);
            if (parsed.newSessionId) {
              newSessionId = parsed.newSessionId;
            }
            hadStreamingOutput = true;
            // Activity detected — reset the hard timeout
            resetTimeout();
            // Call onOutput for all markers (including null results)
            // so idle timers start even for "silent" query completions.
            outputChain = outputChain.then(() => onOutput(parsed));
          } catch (err) {
            logger.warn(
              { group: group.name, error: err },
              'Failed to parse streamed output chunk',
            );
          }
        }
      }
    });

    container.stderr.on('data', (data) => {
      const chunk = data.toString();
      const lines = chunk.trim().split('\n');
      for (const line of lines) {
        if (line && line.includes('[agent-runner]'))
          logger.info({ container: group.folder }, line);
        else if (line) logger.debug({ container: group.folder }, line);
      }
      // Don't reset timeout on stderr — SDK writes debug logs continuously.
      // Timeout only resets on actual output (OUTPUT_MARKER in stdout).
      if (stderrTruncated) return;
      const remaining = CONTAINER_MAX_OUTPUT_SIZE - stderr.length;
      if (chunk.length > remaining) {
        stderr += chunk.slice(0, remaining);
        stderrTruncated = true;
        logger.warn(
          { group: group.name, size: stderr.length },
          'Container stderr truncated due to size limit',
        );
      } else {
        stderr += chunk;
      }
    });

    let timedOut = false;
    let hadStreamingOutput = false;
    const configTimeout = group.containerConfig?.timeout || CONTAINER_TIMEOUT;
    // Grace period: hard timeout must be at least IDLE_TIMEOUT + 30s so the
    // graceful _close sentinel has time to trigger before the hard kill fires.
    const timeoutMs = Math.max(configTimeout, IDLE_TIMEOUT + 30_000);

    const killOnTimeout = () => {
      timedOut = true;
      logger.error(
        { group: group.name, containerName },
        'Container timeout, stopping gracefully',
      );
      exec(stopContainer(containerName), { timeout: 15000 }, (err) => {
        if (err) {
          logger.warn(
            { group: group.name, containerName, err },
            'Graceful stop failed, force killing',
          );
          container.kill('SIGKILL');
        }
      });
    };

    let timeout = setTimeout(killOnTimeout, timeoutMs);

    // Reset the timeout whenever there's activity (streaming output)
    const resetTimeout = () => {
      clearTimeout(timeout);
      timeout = setTimeout(killOnTimeout, timeoutMs);
    };

    container.on('close', (code) => {
      clearTimeout(timeout);
      const duration = Date.now() - startTime;

      if (timedOut) {
        const ts = new Date().toISOString().replace(/[:.]/g, '-');
        const timeoutLog = path.join(logsDir, `container-${ts}.log`);
        fs.writeFileSync(
          timeoutLog,
          [
            `=== Container Run Log (TIMEOUT) ===`,
            `Timestamp: ${new Date().toISOString()}`,
            `Group: ${group.name}`,
            `Container: ${containerName}`,
            `Duration: ${duration}ms`,
            `Exit Code: ${code}`,
            `Had Streaming Output: ${hadStreamingOutput}`,
          ].join('\n'),
        );

        // Timeout after output = idle cleanup, not failure.
        // The agent already sent its response; this is just the
        // container being reaped after the idle period expired.
        if (hadStreamingOutput) {
          logger.info(
            { group: group.name, containerName, duration, code },
            'Container timed out after output (idle cleanup)',
          );
          outputChain.then(() => {
            resolve({
              status: 'success',
              result: null,
              newSessionId,
            });
          });
          return;
        }

        logger.error(
          { group: group.name, containerName, duration, code },
          'Container timed out with no output',
        );

        resolve({
          status: 'error',
          result: null,
          error: `Container timed out after ${configTimeout}ms`,
        });
        return;
      }

      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const logFile = path.join(logsDir, `container-${timestamp}.log`);
      const isVerbose =
        process.env.LOG_LEVEL === 'debug' || process.env.LOG_LEVEL === 'trace';

      const logLines = [
        `=== Container Run Log ===`,
        `Timestamp: ${new Date().toISOString()}`,
        `Group: ${group.name}`,
        `IsMain: ${input.isMain}`,
        `Duration: ${duration}ms`,
        `Exit Code: ${code}`,
        `Stdout Truncated: ${stdoutTruncated}`,
        `Stderr Truncated: ${stderrTruncated}`,
        ``,
      ];

      const isError = code !== 0;

      if (isVerbose || isError) {
        logLines.push(
          `=== Input ===`,
          JSON.stringify(input, null, 2),
          ``,
          `=== Container Args ===`,
          containerArgs.join(' '),
          ``,
          `=== Mounts ===`,
          mounts
            .map(
              (m) =>
                `${m.hostPath} -> ${m.containerPath}${m.readonly ? ' (ro)' : ''}`,
            )
            .join('\n'),
          ``,
          `=== Stderr${stderrTruncated ? ' (TRUNCATED)' : ''} ===`,
          stderr,
          ``,
          `=== Stdout${stdoutTruncated ? ' (TRUNCATED)' : ''} ===`,
          stdout,
        );
      } else {
        logLines.push(
          `=== Input Summary ===`,
          `Prompt length: ${input.prompt.length} chars`,
          `Session ID: ${input.sessionId || 'new'}`,
          ``,
          `=== Mounts ===`,
          mounts
            .map((m) => `${m.containerPath}${m.readonly ? ' (ro)' : ''}`)
            .join('\n'),
          ``,
        );
      }

      fs.writeFileSync(logFile, logLines.join('\n'));
      logger.debug({ logFile, verbose: isVerbose }, 'Container log written');

      if (code !== 0) {
        logger.error(
          {
            group: group.name,
            code,
            duration,
            stderr,
            stdout,
            logFile,
          },
          'Container exited with error',
        );

        resolve({
          status: 'error',
          result: null,
          error: `Container exited with code ${code}: ${stderr.slice(-200)}`,
        });
        return;
      }

      // Streaming mode: wait for output chain to settle, return completion marker
      if (onOutput) {
        outputChain.then(() => {
          logger.info(
            { group: group.name, duration, newSessionId },
            'Container completed (streaming mode)',
          );
          resolve({
            status: 'success',
            result: null,
            newSessionId,
          });
        });
        return;
      }

      // Legacy mode: parse the last output marker pair from accumulated stdout
      try {
        // Extract JSON between sentinel markers for robust parsing
        const startIdx = stdout.indexOf(OUTPUT_START_MARKER);
        const endIdx = stdout.indexOf(OUTPUT_END_MARKER);

        let jsonLine: string;
        if (startIdx !== -1 && endIdx !== -1 && endIdx > startIdx) {
          jsonLine = stdout
            .slice(startIdx + OUTPUT_START_MARKER.length, endIdx)
            .trim();
        } else {
          // Fallback: last non-empty line (backwards compatibility)
          const lines = stdout.trim().split('\n');
          jsonLine = lines[lines.length - 1];
        }

        const output: ContainerOutput = JSON.parse(jsonLine);

        logger.info(
          {
            group: group.name,
            duration,
            status: output.status,
            hasResult: !!output.result,
          },
          'Container completed',
        );

        resolve(output);
      } catch (err) {
        logger.error(
          {
            group: group.name,
            stdout,
            stderr,
            error: err,
          },
          'Failed to parse container output',
        );

        resolve({
          status: 'error',
          result: null,
          error: `Failed to parse container output: ${err instanceof Error ? err.message : String(err)}`,
        });
      }
    });

    container.on('error', (err) => {
      clearTimeout(timeout);
      logger.error(
        { group: group.name, containerName, error: err },
        'Container spawn error',
      );
      resolve({
        status: 'error',
        result: null,
        error: `Container spawn error: ${err.message}`,
      });
    });
  });
}

export function writeTasksSnapshot(
  groupFolder: string,
  isMain: boolean,
  tasks: Array<{
    id: string;
    groupFolder: string;
    prompt: string;
    schedule_type: string;
    schedule_value: string;
    status: string;
    next_run: string | null;
  }>,
): void {
  // Write filtered tasks to the group's IPC directory
  const groupIpcDir = resolveGroupIpcPath(groupFolder);
  fs.mkdirSync(groupIpcDir, { recursive: true });

  // Main sees all tasks, others only see their own
  const filteredTasks = isMain
    ? tasks
    : tasks.filter((t) => t.groupFolder === groupFolder);

  const tasksFile = path.join(groupIpcDir, 'current_tasks.json');
  fs.writeFileSync(tasksFile, JSON.stringify(filteredTasks, null, 2));
}

export interface AvailableGroup {
  jid: string;
  name: string;
  lastActivity: string;
  isRegistered: boolean;
}

/**
 * Write available groups snapshot for the container to read.
 * Only main group can see all available groups (for activation).
 * Non-main groups only see their own registration status.
 */
export function writeGroupsSnapshot(
  groupFolder: string,
  isMain: boolean,
  groups: AvailableGroup[],
  registeredJids: Set<string>,
): void {
  const groupIpcDir = resolveGroupIpcPath(groupFolder);
  fs.mkdirSync(groupIpcDir, { recursive: true });

  // Main sees all groups; others see nothing (they can't activate groups)
  const visibleGroups = isMain ? groups : [];

  const groupsFile = path.join(groupIpcDir, 'available_groups.json');
  fs.writeFileSync(
    groupsFile,
    JSON.stringify(
      {
        groups: visibleGroups,
        lastSync: new Date().toISOString(),
      },
      null,
      2,
    ),
  );
}
