import fs from 'fs';
import path from 'path';

import { App, LogLevel } from '@slack/bolt';
import type { GenericMessageEvent, BotMessageEvent } from '@slack/types';

import { ASSISTANT_NAME, TRIGGER_PATTERN, DATA_DIR } from '../config.js';
import { updateChatName } from '../db.js';
import { readEnvFile } from '../env.js';
import { logger } from '../logger.js';
import { registerChannel, ChannelOpts } from './registry.js';
import {
  Channel,
  OnInboundMessage,
  OnChatMetadata,
  RegisteredGroup,
} from '../types.js';

// Slack's chat.postMessage API limits text to ~4000 characters per call.
// Messages exceeding this are split into sequential chunks.
const MAX_MESSAGE_LENGTH = 4000;

// The message subtypes we process. Bolt delivers all subtypes via app.event('message');
// we filter to regular messages (GenericMessageEvent, subtype undefined) and bot messages
// (BotMessageEvent, subtype 'bot_message') so we can track our own output.
type HandledMessageEvent = GenericMessageEvent | BotMessageEvent;

export interface SlackChannelOpts {
  onMessage: OnInboundMessage;
  onChatMetadata: OnChatMetadata;
  registeredGroups: () => Record<string, RegisteredGroup>;
}

export class SlackChannel implements Channel {
  name = 'slack';

  private app: App;
  private botUserId: string | undefined;
  private connected = false;
  private outgoingQueue: Array<{ jid: string; text: string }> = [];
  private flushing = false;
  private userNameCache = new Map<string, string>();

  private opts: SlackChannelOpts;
  private botToken: string;

  constructor(opts: SlackChannelOpts) {
    this.opts = opts;

    // Read tokens from .env (not process.env — keeps secrets off the environment
    // so they don't leak to child processes, matching NanoClaw's security pattern)
    const env = readEnvFile(['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN']);
    const botToken = env.SLACK_BOT_TOKEN;
    const appToken = env.SLACK_APP_TOKEN;

    if (!botToken || !appToken) {
      throw new Error(
        'SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env',
      );
    }

    this.botToken = botToken;

    this.app = new App({
      token: botToken,
      appToken,
      socketMode: true,
      logLevel: LogLevel.ERROR,
    });

    this.setupEventHandlers();
  }

  private setupEventHandlers(): void {
    // Use app.event('message') instead of app.message() to capture all
    // message subtypes including bot_message (needed to track our own output)
    this.app.event('message', async ({ event }) => {
      // Bolt's event type is the full MessageEvent union (17+ subtypes).
      // We filter on subtype first, then narrow to the two types we handle.
      const subtype = (event as { subtype?: string }).subtype;
      if (subtype && subtype !== 'bot_message' && subtype !== 'file_share')
        return;

      // After filtering, event is either GenericMessageEvent or BotMessageEvent
      const msg = event as HandledMessageEvent;

      // Extract file attachments (images, docs, etc.)
      type SlackFile = {
        id: string;
        name?: string;
        url_private_download?: string;
      };
      const files: SlackFile[] = (msg as { files?: SlackFile[] }).files || [];

      // Skip if no text and no files
      if (!msg.text && files.length === 0) return;

      // Thread-aware JID: replies in a thread get their own JID so they can
      // run as independent groups (enabling concurrent execution per thread).
      const threadTs = (msg as { thread_ts?: string }).thread_ts;
      const isThreadReply = !!threadTs && threadTs !== msg.ts;

      if (threadTs) {
        logger.info(
          { channel: msg.channel, ts: msg.ts, threadTs, isThreadReply },
          'Slack message thread detection',
        );
      }

      const channelJid = `slack:${msg.channel}`;
      const jid = isThreadReply
        ? `slack:${msg.channel}:t:${threadTs}`
        : channelJid;
      const timestamp = new Date(parseFloat(msg.ts) * 1000).toISOString();
      const isGroup = msg.channel_type !== 'im';

      // Always report metadata for group discovery (channel level)
      this.opts.onChatMetadata(
        channelJid,
        timestamp,
        undefined,
        'slack',
        isGroup,
      );

      // Also register thread JID in chats table so messages can reference it
      if (isThreadReply) {
        this.opts.onChatMetadata(jid, timestamp, undefined, 'slack', isGroup);
      }

      // Accept messages if the exact JID or parent channel is registered
      const groups = this.opts.registeredGroups();
      if (!groups[jid] && !groups[channelJid]) return;

      const isBotMessage = !!msg.bot_id || msg.user === this.botUserId;

      let senderName: string;
      if (isBotMessage) {
        senderName = ASSISTANT_NAME;
      } else {
        senderName =
          (msg.user ? await this.resolveUserName(msg.user) : undefined) ||
          msg.user ||
          'unknown';
      }

      // Translate Slack <@UBOTID> mentions into TRIGGER_PATTERN format.
      // Slack encodes @mentions as <@U12345>, which won't match TRIGGER_PATTERN
      // (e.g., ^@<ASSISTANT_NAME>\b), so we prepend the trigger when the bot is @mentioned.
      let content = msg.text || '';
      if (this.botUserId && !isBotMessage) {
        const mentionPattern = `<@${this.botUserId}>`;
        if (
          content.includes(mentionPattern) &&
          !TRIGGER_PATTERN.test(content)
        ) {
          content = `@${ASSISTANT_NAME} ${content}`;
        }
      }

      // Download file attachments and append paths the container agent can read
      if (files.length > 0 && !isBotMessage) {
        const group = groups[jid];
        const attachDir = path.join(
          DATA_DIR,
          'ipc',
          group.folder,
          'attachments',
        );
        await fs.promises.mkdir(attachDir, { recursive: true });

        const botToken = this.botToken;
        const attachmentLines: string[] = [];

        for (const file of files) {
          if (!file.url_private_download) continue;
          const ext = (file.name?.split('.').pop() || 'bin').toLowerCase();
          const filename = `${file.id}.${ext}`;
          const hostPath = path.join(attachDir, filename);
          const containerPath = `/workspace/ipc/attachments/${filename}`;

          try {
            // fetch drops Authorization on cross-domain redirects; follow manually
            let downloadUrl = file.url_private_download;
            let response = await fetch(downloadUrl, {
              headers: { Authorization: `Bearer ${botToken}` },
              redirect: 'manual',
            });
            for (
              let i = 0;
              i < 3 && response.status >= 300 && response.status < 400;
              i++
            ) {
              const location = response.headers.get('location');
              if (!location) break;
              response = await fetch(location, {
                headers: { Authorization: `Bearer ${botToken}` },
              });
            }
            const contentType = response.headers.get('content-type') || '';
            if (!response.ok || contentType.includes('text/html')) {
              logger.warn(
                { fileId: file.id, status: response.status, contentType },
                'Slack attachment download failed — bot token may be missing files:read scope',
              );
              attachmentLines.push(
                `[Attachment: ${file.name || filename} — could not download (add files:read scope to Slack bot)]`,
              );
              continue;
            }
            const buffer = await response.arrayBuffer();
            await fs.promises.writeFile(hostPath, Buffer.from(buffer));
            attachmentLines.push(
              `[Attachment: ${file.name || filename} → ${containerPath}]`,
            );
            logger.info(
              { file: file.name, containerPath },
              'Slack attachment downloaded',
            );
          } catch (err) {
            logger.warn(
              { err, fileId: file.id },
              'Failed to download Slack attachment',
            );
          }
        }

        if (attachmentLines.length > 0) {
          content = [content, ...attachmentLines].filter(Boolean).join('\n');
        }
      }

      // Skip if content is still empty after processing
      if (!content) return;

      this.opts.onMessage(jid, {
        id: msg.ts,
        chat_jid: jid,
        sender: msg.user || msg.bot_id || '',
        sender_name: senderName,
        content,
        timestamp,
        is_from_me: isBotMessage,
        is_bot_message: isBotMessage,
      });
    });
  }

  async connect(): Promise<void> {
    await this.app.start();

    // Get bot's own user ID for self-message detection.
    // Resolve this BEFORE setting connected=true so that messages arriving
    // during startup can correctly detect bot-sent messages.
    try {
      const auth = await this.app.client.auth.test();
      this.botUserId = auth.user_id as string;
      logger.info({ botUserId: this.botUserId }, 'Connected to Slack');
    } catch (err) {
      logger.warn({ err }, 'Connected to Slack but failed to get bot user ID');
    }

    this.connected = true;

    // Flush any messages queued before connection
    await this.flushOutgoingQueue();

    // Sync channel names on startup
    await this.syncChannelMetadata();
  }

  async sendMessage(jid: string, text: string): Promise<string | void> {
    const stripped = jid.replace(/^slack:/, '');
    const threadMatch = stripped.match(/^(.+):t:(.+)$/);
    const channelId = threadMatch ? threadMatch[1] : stripped;
    const threadTs = threadMatch ? threadMatch[2] : undefined;

    if (!this.connected) {
      this.outgoingQueue.push({ jid, text });
      logger.info(
        { jid, queueSize: this.outgoingQueue.length },
        'Slack disconnected, message queued',
      );
      return;
    }

    const postOpts = {
      channel: channelId,
      text: '',
      ...(threadTs ? { thread_ts: threadTs } : {}),
    };

    try {
      let firstTs: string | undefined;
      if (text.length <= MAX_MESSAGE_LENGTH) {
        const result = await this.app.client.chat.postMessage({
          ...postOpts,
          text,
        });
        firstTs = result?.ts;
      } else {
        for (let i = 0; i < text.length; i += MAX_MESSAGE_LENGTH) {
          const result = await this.app.client.chat.postMessage({
            ...postOpts,
            text: text.slice(i, i + MAX_MESSAGE_LENGTH),
          });
          if (i === 0) firstTs = result?.ts;
        }
      }
      logger.info({ jid, length: text.length, threadTs }, 'Slack message sent');
      return firstTs;
    } catch (err) {
      this.outgoingQueue.push({ jid, text });
      logger.warn(
        { jid, err, queueSize: this.outgoingQueue.length },
        'Failed to send Slack message, queued',
      );
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  ownsJid(jid: string): boolean {
    return jid.startsWith('slack:');
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    await this.app.stop();
  }

  // Slack does not expose a typing indicator API for bots.
  // This no-op satisfies the Channel interface so the orchestrator
  // doesn't need channel-specific branching.
  async setTyping(_jid: string, _isTyping: boolean): Promise<void> {
    // no-op: Slack Bot API has no typing indicator endpoint
  }

  /**
   * Sync channel metadata from Slack.
   * Fetches channels the bot is a member of and stores their names in the DB.
   */
  async syncChannelMetadata(): Promise<void> {
    try {
      logger.info('Syncing channel metadata from Slack...');
      let cursor: string | undefined;
      let count = 0;

      do {
        const result = await this.app.client.conversations.list({
          types: 'public_channel,private_channel',
          exclude_archived: true,
          limit: 200,
          cursor,
        });

        for (const ch of result.channels || []) {
          if (ch.id && ch.name && ch.is_member) {
            updateChatName(`slack:${ch.id}`, ch.name);
            count++;
          }
        }

        cursor = result.response_metadata?.next_cursor || undefined;
      } while (cursor);

      logger.info({ count }, 'Slack channel metadata synced');
    } catch (err) {
      logger.error({ err }, 'Failed to sync Slack channel metadata');
    }
  }

  private async resolveUserName(userId: string): Promise<string | undefined> {
    if (!userId) return undefined;

    const cached = this.userNameCache.get(userId);
    if (cached) return cached;

    try {
      const result = await this.app.client.users.info({ user: userId });
      const name = result.user?.real_name || result.user?.name;
      if (name) this.userNameCache.set(userId, name);
      return name;
    } catch (err) {
      logger.debug({ userId, err }, 'Failed to resolve Slack user name');
      return undefined;
    }
  }

  private async flushOutgoingQueue(): Promise<void> {
    if (this.flushing || this.outgoingQueue.length === 0) return;
    this.flushing = true;
    try {
      logger.info(
        { count: this.outgoingQueue.length },
        'Flushing Slack outgoing queue',
      );
      while (this.outgoingQueue.length > 0) {
        const item = this.outgoingQueue.shift()!;
        const stripped = item.jid.replace(/^slack:/, '');
        const tm = stripped.match(/^(.+):t:(.+)$/);
        const channelId = tm ? tm[1] : stripped;
        const threadTs = tm ? tm[2] : undefined;
        await this.app.client.chat.postMessage({
          channel: channelId,
          text: item.text,
          ...(threadTs ? { thread_ts: threadTs } : {}),
        });
        logger.info(
          { jid: item.jid, length: item.text.length },
          'Queued Slack message sent',
        );
      }
    } finally {
      this.flushing = false;
    }
  }
}

registerChannel('slack', (opts: ChannelOpts) => {
  const envVars = readEnvFile(['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN']);
  if (!envVars.SLACK_BOT_TOKEN || !envVars.SLACK_APP_TOKEN) {
    logger.warn('Slack: SLACK_BOT_TOKEN or SLACK_APP_TOKEN not set');
    return null;
  }
  return new SlackChannel(opts);
});
