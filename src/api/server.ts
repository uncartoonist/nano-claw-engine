/**
 * HTTP API server with stepped agent loop.
 *
 * Exposes the agent loop over HTTP with tool confirmation:
 * - POST /api/chat         — start agent loop, returns text or pending tools
 * - POST /api/chat/approve — approve pending tools, continue loop
 * - POST /api/chat/reject  — reject tools, LLM retries without them
 * - GET  /api/health       — health check
 */

import http from 'node:http';
import crypto from 'node:crypto';
import { AgentConfig, ToolCall, StreamEvent, LLMResponse } from '../types';
import { ProviderManager } from '../providers/index';
import { Memory } from '../agent/memory';
import { ContextBuilder } from '../agent/context';
import { resolveKnowledgeFiles } from '../agent/knowledge';
import { SkillsLoader } from '../agent/skills';
import { ToolRegistry } from '../agent/tools/registry';
import { ShellTool } from '../agent/tools/shell';
import { ReadFileTool, WriteFileTool } from '../agent/tools/file';
import { Config } from '../config/schema';
import { getConfig, createDefaultConfig, mergeEnvConfig } from '../config/index';
import { logger } from '../utils/logger';
import { modelsWithAvailability, DEFAULT_MODEL } from '../agent/models';
import { isLocked, apiHost, corsOrigin } from '../mission_control'; // [sc]

// ── Types ────────────────────────────────────────────────────

interface DebugInfo {
  iteration: number;
  messageCount: number;
  model: string;
  tokenUsage?: {
    prompt: number;
    completion: number;
    total: number;
    cacheRead?: number;
    cacheWrite?: number;
  };
  durationMs: number;
  firstTokenMs?: number;
  finishReason?: string;
}

interface PendingToolState {
  memory: Memory;
  toolCalls: ToolCall[];
  assistantContent: string;
  iteration: number;
  agentConfig: AgentConfig;
}

type ApiResponse =
  | { type: 'final'; response: string; debug: DebugInfo }
  | { type: 'tool_pending'; requestId: string; tools: { name: string; args: Record<string, unknown> }[]; debug: DebugInfo };

// ── Pending state store ──────────────────────────────────────

const pendingRequests = new Map<string, PendingToolState>();

// Clean up stale pending requests after 10 minutes
const PENDING_TTL_MS = 10 * 60 * 1000;
const pendingTimestamps = new Map<string, number>();

function cleanupStale(): void {
  const now = Date.now();
  for (const [id, ts] of pendingTimestamps) {
    if (now - ts > PENDING_TTL_MS) {
      pendingRequests.delete(id);
      pendingTimestamps.delete(id);
    }
  }
}

// ── Shared instances ─────────────────────────────────────────

let config: Config;
let providerManager: ProviderManager;
let skillsLoader: SkillsLoader;

let sharedInitialized = false;

function initShared(): void {
  if (sharedInitialized) return;

  try {
    config = getConfig();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (/not found/i.test(message)) {
      // No config file on disk (e.g. `nano-claw onboard` never run, or
      // running under test). Degrade to defaults instead of crashing —
      // any env-supplied provider keys still get picked up. Real HTTP
      // requests that need a provider will fail with a clear "No provider
      // configured" error from ProviderManager at call time.
      logger.warn('No nano-claw config file found; using defaults');
      config = mergeEnvConfig(createDefaultConfig());
    } else {
      // Config file exists but is corrupted (invalid JSON) or fails schema
      // validation — this must fail loud, not silently degrade to a looser
      // default config (e.g. restrictToWorkspace: false). Leave
      // `sharedInitialized` false so a retry (e.g. after the caller fixes
      // the file) re-attempts init instead of silently no-op'ing.
      throw error;
    }
  }

  // Preserve a test-injected provider manager (see
  // __setProviderManagerForTest) instead of clobbering it.
  if (!providerManager) providerManager = new ProviderManager(config);
  skillsLoader = new SkillsLoader();
  sharedInitialized = true;
}

/** Test-only: inject a stub provider manager. */
export function __setProviderManagerForTest(pm: unknown): void {
  providerManager = pm as ProviderManager;
}

function createToolRegistry(): ToolRegistry {
  const registry = new ToolRegistry();
  const toolsConfig = config.tools;
  // Knowledge-only mode: no tools registered → no tools offered to the LLM,
  // no approval pauses; the agent answers purely from persona + knowledge.
  if (toolsConfig?.enabled === false) return registry;
  registry.register(
    new ShellTool(
      toolsConfig?.restrictToWorkspace,
      toolsConfig?.allowedCommands,
      toolsConfig?.deniedCommands
    )
  );
  registry.register(new ReadFileTool());
  registry.register(new WriteFileTool());
  return registry;
}

/**
 * Build the agent config for a turn. If `modelOverride` names a model in the
 * catalog, it wins; otherwise falls back to the configured default.
 */
export function getAgentConfig(modelOverride?: string): AgentConfig {
  initShared();
  const valid = !!modelOverride && modelsWithAvailability(config).some((m) => m.id === modelOverride && m.available);
  return {
    model: valid ? (modelOverride as string) : (config.agents?.defaults?.model || DEFAULT_MODEL),
    temperature: config.agents?.defaults?.temperature || 0.7,
    maxTokens: config.agents?.defaults?.maxTokens || 4096,
    systemPrompt: config.agents?.defaults?.systemPrompt,
    knowledgeFiles: resolveKnowledgeFiles(config),
  };
}

// ── Stepped agent loop ───────────────────────────────────────

const MAX_ITERATIONS = 10;

/**
 * Run one LLM call and either return final text or pause at tool calls.
 */
async function stepLoop(
  memory: Memory,
  agentConfig: AgentConfig,
  iteration: number
): Promise<ApiResponse> {
  initShared();
  const toolRegistry = createToolRegistry();

  while (iteration < MAX_ITERATIONS) {
    iteration++;
    const messageCount = memory.getMessages().length;
    const startTime = Date.now();

    const skills = skillsLoader.getSkills();
    const tools = toolRegistry.getDefinitions();
    const contextBuilder = new ContextBuilder(agentConfig);
    const contextMessages = contextBuilder.buildContextMessages(
      memory.getMessages(),
      skills,
      tools
    );

    const response = await providerManager.complete(
      contextMessages,
      agentConfig.model,
      agentConfig.temperature,
      agentConfig.maxTokens,
      tools
    );

    const durationMs = Date.now() - startTime;

    const debug: DebugInfo = {
      iteration,
      messageCount,
      model: agentConfig.model,
      tokenUsage: response.usage
        ? {
            prompt: response.usage.promptTokens,
            completion: response.usage.completionTokens,
            total: response.usage.totalTokens,
            cacheRead: response.usage.cacheReadTokens,
            cacheWrite: response.usage.cacheWriteTokens,
          }
        : undefined,
      durationMs,
      finishReason: response.finishReason,
    };

    logger.info({
      iteration,
      messageCount,
      model: agentConfig.model,
      tokenUsage: debug.tokenUsage,
      durationMs,
      finishReason: response.finishReason,
      hasToolCalls: !!(response.toolCalls && response.toolCalls.length > 0),
    }, 'Agent loop iteration complete');

    if (response.toolCalls && response.toolCalls.length > 0) {
      // Add assistant message with tool calls to memory
      memory.addMessage({
        role: 'assistant',
        content: response.content || '',
        tool_calls: response.toolCalls,
      });

      // Pause — return tool calls for browser approval
      const requestId = crypto.randomUUID();
      pendingRequests.set(requestId, {
        memory,
        toolCalls: response.toolCalls,
        assistantContent: response.content || '',
        iteration,
        agentConfig,
      });
      pendingTimestamps.set(requestId, Date.now());

      return {
        type: 'tool_pending',
        requestId,
        tools: response.toolCalls.map((tc) => ({
          name: tc.function.name,
          args: safeParseToolArgs(tc.function.arguments),
        })),
        debug,
      };
    }

    // No tool calls — final response
    memory.addMessage({
      role: 'assistant',
      content: response.content,
    });

    return { type: 'final', response: response.content, debug };
  }

  return { type: 'final', response: 'Max iterations reached.', debug: { iteration: MAX_ITERATIONS, messageCount: memory.getMessages().length, model: agentConfig.model, durationMs: 0, finishReason: 'max_iterations' } };
}

/**
 * Streaming variant of stepLoop — yields text deltas as they arrive, then a
 * terminal `tool_pending` or `final` event (mirrors stepLoop's return value).
 */
export async function* stepLoopStream(
  memory: Memory,
  agentConfig: AgentConfig,
  iteration: number
): AsyncGenerator<StreamEvent | ApiResponse> {
  initShared();
  const toolRegistry = createToolRegistry();

  while (iteration < MAX_ITERATIONS) {
    iteration++;
    const messageCount = memory.getMessages().length;
    const startTime = Date.now();
    const skills = skillsLoader.getSkills();
    const tools = toolRegistry.getDefinitions();
    const contextBuilder = new ContextBuilder(agentConfig);
    const contextMessages = contextBuilder.buildContextMessages(memory.getMessages(), skills, tools);

    let text = '';
    let toolCalls: ToolCall[] | undefined;
    let finishReason: string | undefined;
    let usage: LLMResponse['usage'];
    let firstTokenAt: number | undefined;

    for await (const ev of providerManager.completeStream(
      contextMessages, agentConfig.model, agentConfig.temperature, agentConfig.maxTokens, tools
    )) {
      if (ev.type === 'text') {
        if (firstTokenAt === undefined) firstTokenAt = Date.now();
        text += ev.delta;
        yield ev; // forward the delta to the SSE writer
      } else if (ev.type === 'tool_calls') {
        toolCalls = ev.toolCalls;
      } else if (ev.type === 'done') {
        finishReason = ev.finishReason;
        usage = ev.usage;
      }
    }

    const debug: DebugInfo = {
      iteration,
      messageCount,
      model: agentConfig.model,
      tokenUsage: usage
        ? {
            prompt: usage.promptTokens,
            completion: usage.completionTokens,
            total: usage.totalTokens,
            cacheRead: usage.cacheReadTokens,
            cacheWrite: usage.cacheWriteTokens,
          }
        : undefined,
      durationMs: Date.now() - startTime,
      firstTokenMs: firstTokenAt !== undefined ? firstTokenAt - startTime : undefined,
      finishReason,
    };

    if (toolCalls && toolCalls.length > 0) {
      memory.addMessage({ role: 'assistant', content: text, tool_calls: toolCalls });
      const requestId = crypto.randomUUID();
      pendingRequests.set(requestId, { memory, toolCalls, assistantContent: text, iteration, agentConfig });
      pendingTimestamps.set(requestId, Date.now());
      yield {
        type: 'tool_pending',
        requestId,
        tools: toolCalls.map((tc) => ({ name: tc.function.name, args: safeParseToolArgs(tc.function.arguments) })),
        debug,
      };
      return;
    }

    memory.addMessage({ role: 'assistant', content: text });
    yield { type: 'final', response: text, debug };
    return;
  }

  yield { type: 'final', response: 'Max iterations reached.', debug: { iteration: MAX_ITERATIONS, messageCount: memory.getMessages().length, model: agentConfig.model, durationMs: 0, finishReason: 'max_iterations' } };
}

// ── Session memory cache ─────────────────────────────────────

const sessionMemories = new Map<string, Memory>();

function getMemory(sessionId: string): Memory {
  let memory = sessionMemories.get(sessionId);
  if (!memory) {
    memory = new Memory(sessionId);
    sessionMemories.set(sessionId, memory);
  }
  return memory;
}

// ── HTTP helpers ─────────────────────────────────────────────

function setCorsHeaders(res: http.ServerResponse): void {
  res.setHeader('Access-Control-Allow-Origin', corsOrigin()); // [sc] env-configurable
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendJson(res: http.ServerResponse, statusCode: number, body: unknown): void {
  setCorsHeaders(res);
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

const STREAM_ENABLED = process.env.NANO_CLAW_STREAM !== '0' && process.env.NANO_CLAW_STREAM !== 'false';

function wantsStream(req: http.IncomingMessage): boolean {
  return STREAM_ENABLED && (req.headers['accept'] || '').includes('text/event-stream');
}

function sseWrite(res: http.ServerResponse, event: string, data: unknown): void {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

async function streamLoopToSSE(
  res: http.ServerResponse,
  gen: AsyncGenerator<StreamEvent | ApiResponse>
): Promise<void> {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });
  try {
    for await (const ev of gen) {
      if ((ev as StreamEvent).type === 'text') sseWrite(res, 'delta', { text: (ev as { delta: string }).delta });
      else if ((ev as ApiResponse).type === 'tool_pending') sseWrite(res, 'tool_pending', ev);
      else if ((ev as ApiResponse).type === 'final') sseWrite(res, 'final', ev);
    }
  } catch (err) {
    sseWrite(res, 'error', { error: err instanceof Error ? err.message : 'stream error' });
  } finally {
    res.end();
  }
}

const MAX_BODY_BYTES = 1024 * 1024; // 1 MB

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    req.on('data', (chunk: Buffer) => {
      totalBytes += chunk.length;
      if (totalBytes > MAX_BODY_BYTES) {
        req.destroy();
        reject(new Error('Request body too large'));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString()));
    req.on('error', reject);
  });
}

function parseJsonBody(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function safeParseToolArgs(argsJson: string): Record<string, unknown> {
  try {
    return JSON.parse(argsJson) as Record<string, unknown>;
  } catch {
    return { _raw: argsJson };
  }
}

// ── Route handlers ───────────────────────────────────────────

function handleModels(res: http.ServerResponse): void {
  initShared();
  // Advertise the deployment's configured default (docker/default-config.json),
  // not the compiled-in constant — the voice UI uses this for fresh browsers.
  sendJson(res, 200, { models: modelsWithAvailability(config), default: config.agents?.defaults?.model || DEFAULT_MODEL });
}

async function handleChat(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
  const body = parseJsonBody(await readBody(req)) as { message?: string; sessionId?: string; model?: string } | null;
  if (!body || typeof body.message !== 'string' || !body.message.trim()) {
    sendJson(res, 400, { error: 'Missing or empty "message" field' });
    return;
  }
  const sessionId = body.sessionId || 'default';
  const memory = getMemory(sessionId);

  memory.addMessage({ role: 'user', content: body.message });

  // [sc] Locked mode: model selection is server policy, never client input.
  const agentConfig = getAgentConfig(isLocked() ? undefined : body.model);
  if (wantsStream(req)) {
    await streamLoopToSSE(res, stepLoopStream(memory, agentConfig, 0));
    return;
  }
  const result = await stepLoop(memory, agentConfig, 0);
  sendJson(res, 200, result);
}

async function handleApprove(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
  const body = parseJsonBody(await readBody(req)) as { requestId?: string; sessionId?: string } | null;
  if (!body || typeof body.requestId !== 'string') {
    sendJson(res, 400, { error: 'Missing "requestId" field' });
    return;
  }
  const pending = pendingRequests.get(body.requestId);

  if (!pending) {
    sendJson(res, 404, { error: 'Unknown or expired requestId' });
    return;
  }

  pendingRequests.delete(body.requestId);
  pendingTimestamps.delete(body.requestId);

  // Execute approved tools
  const toolRegistry = createToolRegistry();
  for (const toolCall of pending.toolCalls) {
    const toolName = toolCall.function.name;
    const toolArgs = safeParseToolArgs(toolCall.function.arguments);

    const toolStart = Date.now();
    const toolResult = await toolRegistry.execute(toolName, toolArgs);
    const toolDuration = Date.now() - toolStart;

    logger.info({
      tool: toolName,
      success: toolResult.success,
      durationMs: toolDuration,
      ...(toolResult.error && { error: toolResult.error }),
    }, 'Tool execution complete');

    pending.memory.addMessage({
      role: 'tool',
      content: toolResult.success ? toolResult.output : `Error: ${toolResult.error}`,
      name: toolName,
      tool_call_id: toolCall.id,
    });
  }

  // Continue the loop
  if (wantsStream(req)) {
    await streamLoopToSSE(res, stepLoopStream(pending.memory, pending.agentConfig, pending.iteration));
    return;
  }
  const result = await stepLoop(pending.memory, pending.agentConfig, pending.iteration);
  sendJson(res, 200, result);
}

async function handleReject(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
  const body = parseJsonBody(await readBody(req)) as { requestId?: string; sessionId?: string } | null;
  if (!body || typeof body.requestId !== 'string') {
    sendJson(res, 400, { error: 'Missing "requestId" field' });
    return;
  }
  const pending = pendingRequests.get(body.requestId);

  if (!pending) {
    sendJson(res, 404, { error: 'Unknown or expired requestId' });
    return;
  }

  pendingRequests.delete(body.requestId);
  pendingTimestamps.delete(body.requestId);

  // Add tool rejection messages so LLM knows tools were denied
  for (const toolCall of pending.toolCalls) {
    pending.memory.addMessage({
      role: 'tool',
      content: 'Tool execution was rejected by the user.',
      name: toolCall.function.name,
      tool_call_id: toolCall.id,
    });
  }

  // Continue loop — LLM will respond without tool results
  if (wantsStream(req)) {
    await streamLoopToSSE(res, stepLoopStream(pending.memory, pending.agentConfig, pending.iteration));
    return;
  }
  const result = await stepLoop(pending.memory, pending.agentConfig, pending.iteration);
  sendJson(res, 200, result);
}

// ── Server ───────────────────────────────────────────────────

export function createServer(): http.Server {
  initShared();

  // Periodic cleanup of stale pending requests
  const cleanupInterval = setInterval(cleanupStale, 60_000);

  const server = http.createServer(async (req, res) => {
    const url = req.url || '';
    const method = req.method || '';

    // CORS preflight
    if (method === 'OPTIONS') {
      setCorsHeaders(res);
      res.writeHead(204);
      res.end();
      return;
    }

    try {
      if (method === 'GET' && url === '/api/health') {
        sendJson(res, 200, { status: 'ok' });
      } else if (method === 'GET' && url === '/api/models') {
        setCorsHeaders(res);
        handleModels(res);
      } else if (method === 'POST' && (url === '/api/chat' || url === '/api/chat/approve' || url === '/api/chat/reject')) {
        const ct = req.headers['content-type'] || '';
        if (!ct.includes('application/json')) {
          sendJson(res, 415, { error: 'Content-Type must be application/json' });
          return;
        }
        if (url === '/api/chat') await handleChat(req, res);
        else if (url === '/api/chat/approve') await handleApprove(req, res);
        else await handleReject(req, res);
      } else {
        sendJson(res, 404, { error: 'Not found' });
      }
    } catch (error) {
      logger.error({ error, url, method }, 'API error');
      sendJson(res, 500, { error: (error as Error).message });
    }
  });

  server.on('close', () => clearInterval(cleanupInterval));

  return server;
}

/**
 * Start the API server (used by entrypoint and CLI).
 */
export async function startServer(port: number): Promise<void> {
  const server = createServer();

  // [sc] Optional bind host (NANO_CLAW_API_HOST=127.0.0.1 in the public
  // deployment so only the co-located voice server can reach the API).
  const host = apiHost();
  await new Promise<void>((resolve) => {
    if (host) server.listen(port, host, () => resolve());
    else server.listen(port, () => resolve());
  });

  logger.info({ port, host: host ?? '0.0.0.0' }, 'nano-claw API server listening');
}
