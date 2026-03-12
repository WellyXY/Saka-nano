/**
 * Container runtime abstraction for NanoClaw.
 * All runtime-specific logic lives here so swapping runtimes means changing one file.
 */
import { execSync, exec } from 'child_process';

import { logger } from './logger.js';

/** The container runtime binary name. */
export const CONTAINER_RUNTIME_BIN = 'container';

/**
 * Hostname containers use to reach the host machine.
 * Apple Container VMs access the host via the default gateway (192.168.64.1).
 */
export const CONTAINER_HOST_GATEWAY = '192.168.64.1';

/**
 * Address the credential proxy binds to on the host.
 * Must be 0.0.0.0 so the Apple Container VM can reach it via the gateway.
 */
export const PROXY_BIND_HOST = '0.0.0.0';

/**
 * CLI args needed for the container to resolve the host gateway.
 * Apple Container provides host networking natively on macOS — no extra args needed.
 */
export function hostGatewayArgs(): string[] {
  return [];
}

/** Returns CLI args for a readonly bind mount. */
export function readonlyMountArgs(
  hostPath: string,
  containerPath: string,
): string[] {
  return [
    '--mount',
    `type=bind,source=${hostPath},target=${containerPath},readonly`,
  ];
}

/** Returns the shell command to stop a container by name. */
export function stopContainer(name: string): string {
  return `${CONTAINER_RUNTIME_BIN} stop ${name}`;
}

/** Ensure the container runtime is running, starting it if needed. */
export function ensureContainerRuntimeRunning(): void {
  try {
    execSync(`${CONTAINER_RUNTIME_BIN} system status`, { stdio: 'pipe' });
    logger.debug('Container runtime already running');
  } catch {
    logger.info('Starting container runtime...');
    try {
      execSync(`${CONTAINER_RUNTIME_BIN} system start`, {
        stdio: 'pipe',
        timeout: 30000,
      });
      logger.info('Container runtime started');
    } catch (err) {
      logger.error({ err }, 'Failed to start container runtime');
      console.error(
        '\n╔════════════════════════════════════════════════════════════════╗',
      );
      console.error(
        '║  FATAL: Container runtime failed to start                      ║',
      );
      console.error(
        '║                                                                ║',
      );
      console.error(
        '║  Agents cannot run without a container runtime. To fix:        ║',
      );
      console.error(
        '║  1. Ensure Apple Container is installed                        ║',
      );
      console.error(
        '║  2. Run: container system start                                ║',
      );
      console.error(
        '║  3. Restart NanoClaw                                           ║',
      );
      console.error(
        '╚════════════════════════════════════════════════════════════════╝\n',
      );
      throw new Error('Container runtime is required but failed to start');
    }
  }
}

/** Kill orphaned NanoClaw containers from previous runs (non-blocking). */
export function cleanupOrphans(): void {
  let output: string;
  try {
    output = execSync(`${CONTAINER_RUNTIME_BIN} ls --format json`, {
      stdio: ['pipe', 'pipe', 'pipe'],
      encoding: 'utf-8',
      timeout: 5000,
    });
  } catch (err) {
    logger.warn({ err }, 'Failed to list containers for orphan cleanup');
    return;
  }

  const containers: { status: string; configuration: { id: string } }[] =
    JSON.parse(output || '[]');
  const orphans = containers
    .filter(
      (c) =>
        c.status === 'running' && c.configuration.id.startsWith('nanoclaw-'),
    )
    .map((c) => c.configuration.id);

  if (orphans.length === 0) return;

  logger.info(
    { count: orphans.length, names: orphans },
    'Stopping orphaned containers (async)',
  );
  for (const name of orphans) {
    exec(stopContainer(name), { timeout: 15000 }, (err) => {
      if (err) {
        logger.debug(
          { name, err: err.message },
          'Orphan stop failed (may already be gone)',
        );
      } else {
        logger.info({ name }, 'Orphaned container stopped');
      }
    });
  }
}

/**
 * List currently running NanoClaw containers.
 * Uses text output because `--format json` can miss recently started containers.
 */
export function listRunningContainers(): string[] {
  try {
    const output = execSync(`${CONTAINER_RUNTIME_BIN} list`, {
      stdio: ['pipe', 'pipe', 'pipe'],
      encoding: 'utf-8',
      timeout: 5000,
    });
    return output
      .split('\n')
      .filter((line) => line.includes('nanoclaw-') && line.includes('running'))
      .map((line) => line.trim().split(/\s+/)[0])
      .filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Stop a container asynchronously (fire-and-forget with timeout).
 */
export function stopContainerAsync(name: string): void {
  exec(stopContainer(name), { timeout: 15000 }, (err) => {
    if (err) {
      logger.debug({ name }, 'Async container stop failed');
    }
  });
}
