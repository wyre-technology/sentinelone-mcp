/**
 * Multitenant Streamable HTTP wrapper for sentinel-one/purple-mcp.
 *
 * Why this exists:
 *   purple-mcp natively supports `--mode streamable-http`, but it's a
 *   single-tenant process: credentials are read from PURPLEMCP_CONSOLE_TOKEN
 *   and PURPLEMCP_CONSOLE_BASE_URL at startup. Our MCP gateway forwards
 *   per-tenant credentials as HTTP headers on every request, so we need a
 *   thin shim in front of purple-mcp that translates request headers into
 *   per-tenant child processes.
 *
 * Strategy:
 *   - Listen on 0.0.0.0:8080 with `POST /mcp` and `GET /health`.
 *   - For each /mcp request, read x-purplemcp-token and x-purplemcp-base-url.
 *   - Look up (or lazily spawn) a purple-mcp child for that tenant on a
 *     private loopback port. Children are cached in-memory keyed by a hash
 *     of the credentials.
 *   - Proxy the request body + content-type + accept headers to the child's
 *     own /mcp endpoint, stream the response back to the caller.
 *   - Evict idle children after IDLE_EVICT_MS to bound memory.
 *
 * This intentionally does NOT try to multiplex JSON-RPC requests across
 * tenants on a shared process — purple-mcp's auth is per-process, so
 * isolation has to be at the process boundary.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { createServer } from "node:net";
import Fastify from "fastify";
import { verifyS2sHeader, S2S_HEADER } from "./s2s-verify.js";

const S2S_SECRET = process.env.CONDUIT_S2S_SECRET || "";

const PORT = Number(process.env.PORT ?? 8080);
const PURPLE_MCP_DIR = process.env.PURPLE_MCP_DIR ?? "/opt/purple-mcp";
const PURPLE_MCP_PYTHON =
  process.env.PURPLE_MCP_PYTHON ?? `${PURPLE_MCP_DIR}/.venv/bin/python`;
// Keep a spawned purple-mcp child warm for 60 min by default. The child has a
// heavy (~10s+) cold start (pandas/fastmcp/uvicorn imports), and the gateway
// enforces a short per-vendor tool-fetch timeout — so an aggressive idle evict
// makes the first call after each gap pay that cold start and risk a gateway
// timeout. A longer TTL keeps steady-state traffic on a warm child.
const IDLE_EVICT_MS = Number(process.env.IDLE_EVICT_MS ?? 60 * 60 * 1000); // 60 min
const SPAWN_READY_TIMEOUT_MS = Number(process.env.SPAWN_READY_TIMEOUT_MS ?? 30_000);

// Header names the gateway forwards. Match vendor-config.ts headerMapping.
const HEADER_TOKEN = "x-purplemcp-token";
const HEADER_BASE_URL = "x-purplemcp-base-url";

interface TenantChild {
  port: number;
  proc: ChildProcess;
  ready: Promise<void>;
  lastUsed: number;
  credHash: string;
}

const children = new Map<string, TenantChild>();

/** Allocate a free TCP port on 127.0.0.1 by binding ephemeral and reading it back. */
function allocatePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.unref();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (addr && typeof addr === "object") {
        const p = addr.port;
        srv.close(() => resolve(p));
      } else {
        srv.close(() => reject(new Error("Could not allocate port")));
      }
    });
  });
}

function hashCreds(token: string, baseUrl: string): string {
  return createHash("sha256").update(`${token}\0${baseUrl}`).digest("hex").slice(0, 16);
}

/**
 * Wait until child's HTTP server is accepting connections on /mcp (or /).
 *
 * `getSpawnError` lets the caller surface a child spawn failure (e.g. ENOENT
 * when the interpreter path is missing) so we fail fast with the real error
 * instead of polling a dead port until the full timeout elapses.
 */
async function waitForReady(
  port: number,
  timeoutMs: number,
  getSpawnError?: () => Error | null,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const url = `http://127.0.0.1:${port}/mcp`;
  while (Date.now() < deadline) {
    const spawnErr = getSpawnError?.();
    if (spawnErr) throw spawnErr;
    try {
      // Any HTTP response (even 4xx) means the server is up.
      const res = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json, text/event-stream" },
        body: JSON.stringify({ jsonrpc: "2.0", id: "ping", method: "ping" }),
        signal: AbortSignal.timeout(2000),
      });
      // 2xx, 4xx, 405, etc. all mean it's listening
      if (res.status > 0) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`purple-mcp child did not become ready within ${timeoutMs}ms`);
}

async function getOrSpawnChild(token: string, baseUrl: string): Promise<TenantChild> {
  const credHash = hashCreds(token, baseUrl);
  const existing = children.get(credHash);
  if (existing) {
    existing.lastUsed = Date.now();
    return existing;
  }

  const port = await allocatePort();
  // Random auth token — purple-mcp's docker-entrypoint refuses the placeholder
  // value, and on streamable-http with --allow-remote-access we want a token
  // set anyway. We don't expose this child to the network, only loopback,
  // so the value just has to not be the placeholder.
  const internalAuthToken = randomUUID();

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    PURPLEMCP_CONSOLE_TOKEN: token,
    PURPLEMCP_CONSOLE_BASE_URL: baseUrl,
    PURPLEMCP_AUTH_TOKEN: internalAuthToken,
    MCP_MODE: "streamable-http",
    MCP_HOST: "127.0.0.1",
    MCP_PORT: String(port),
    PURPLEMCP_STATELESS_HTTP: "True",
  };

  // Spawn purple-mcp via the venv python directly. We avoid the docker-entrypoint
  // shell wrapper so we don't need /bin/sh quirks; the CLI handles its own args.
  const proc = spawn(
    PURPLE_MCP_PYTHON,
    [
      "-u",
      "-m",
      "purple_mcp.cli",
      "--mode",
      "streamable-http",
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
    ],
    {
      cwd: PURPLE_MCP_DIR,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  // Tag child stdout/stderr with the tenant hash for debuggability.
  proc.stdout?.on("data", (chunk) => {
    process.stderr.write(`[s1:${credHash}:out] ${chunk}`);
  });
  proc.stderr?.on("data", (chunk) => {
    process.stderr.write(`[s1:${credHash}:err] ${chunk}`);
  });
  // A spawn failure — most commonly ENOENT when PURPLE_MCP_PYTHON points at a
  // missing interpreter — emits an 'error' event on the child. With NO listener
  // Node re-throws it as an uncaught exception, crashing the ENTIRE wrapper and
  // taking every other tenant down with it (followed by a container restart).
  // Capture it here so the failure stays scoped to THIS request: record it,
  // clean up, and let waitForReady() surface it so getOrSpawnChild rejects and
  // the /mcp handler returns a clean 502 for this tenant. The next request
  // retries a fresh spawn.
  let spawnError: Error | null = null;
  proc.once("error", (err) => {
    spawnError = err;
    process.stderr.write(`[s1:${credHash}] purple-mcp failed to spawn: ${err}\n`);
    children.delete(credHash);
  });

  proc.on("exit", (code, signal) => {
    process.stderr.write(`[s1:${credHash}] purple-mcp exited code=${code} signal=${signal}\n`);
    children.delete(credHash);
  });

  const ready = waitForReady(port, SPAWN_READY_TIMEOUT_MS, () => spawnError);
  const child: TenantChild = {
    port,
    proc,
    ready,
    lastUsed: Date.now(),
    credHash,
  };
  children.set(credHash, child);

  try {
    await ready;
  } catch (err) {
    // If it never came up, kill and remove so the next request retries cleanly.
    try { proc.kill("SIGKILL"); } catch { /* ignore */ }
    children.delete(credHash);
    throw err;
  }
  return child;
}

// Idle eviction sweep
setInterval(() => {
  const now = Date.now();
  for (const [hash, child] of children) {
    if (now - child.lastUsed > IDLE_EVICT_MS) {
      process.stderr.write(`[s1:${hash}] evicting idle child after ${IDLE_EVICT_MS}ms\n`);
      try { child.proc.kill("SIGTERM"); } catch { /* ignore */ }
      children.delete(hash);
    }
  }
}, 60_000).unref();

// ---------- HTTP server ----------

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? "info" } });

app.get("/health", async () => ({
  status: "ok",
  tenants: children.size,
}));

// We accept the request body as a Buffer so we can forward it verbatim
// without having to know whether it's a single JSON-RPC request, a batch,
// or anything else. This also avoids re-serializing.
app.addContentTypeParser(
  ["application/json", "application/json-rpc", "application/*+json"],
  { parseAs: "buffer" },
  (_req, body, done) => done(null, body),
);

app.post("/mcp", async (req, reply) => {
  if (S2S_SECRET && !verifyS2sHeader(req.headers[S2S_HEADER] as string | undefined, S2S_SECRET)) {
    reply.code(401);
    return {
      error: "Missing or invalid X-Gateway-S2S header: this endpoint only accepts requests signed by the gateway.",
    };
  }

  const headers = req.headers;
  const token = (headers[HEADER_TOKEN] as string | undefined)?.trim();
  const baseUrl = (headers[HEADER_BASE_URL] as string | undefined)?.trim();

  if (!token || !baseUrl) {
    reply.code(400);
    return {
      jsonrpc: "2.0",
      id: null,
      error: {
        code: -32001,
        message: `Missing SentinelOne credentials. Set headers ${HEADER_TOKEN} and ${HEADER_BASE_URL}.`,
      },
    };
  }

  let child: TenantChild;
  try {
    child = await getOrSpawnChild(token, baseUrl);
  } catch (err) {
    req.log.error({ err }, "failed to spawn purple-mcp child");
    reply.code(502);
    return {
      jsonrpc: "2.0",
      id: null,
      error: {
        code: -32002,
        message: `Failed to start SentinelOne MCP server: ${err instanceof Error ? err.message : String(err)}`,
      },
    };
  }

  child.lastUsed = Date.now();

  // Forward the request to the child's /mcp endpoint.
  const upstream = `http://127.0.0.1:${child.port}/mcp`;
  const fwdHeaders: Record<string, string> = {
    "content-type": (headers["content-type"] as string) ?? "application/json",
    accept: (headers["accept"] as string) ?? "application/json, text/event-stream",
  };
  // Forward MCP session id if present (for stateful clients)
  const sessionId = headers["mcp-session-id"];
  if (typeof sessionId === "string") fwdHeaders["mcp-session-id"] = sessionId;

  const body = req.body as Buffer | undefined;

  let upstreamRes: Response;
  try {
    upstreamRes = await fetch(upstream, {
      method: "POST",
      headers: fwdHeaders,
      body: body ? new Uint8Array(body) : undefined,
    });
  } catch (err) {
    req.log.error({ err }, "upstream fetch failed");
    reply.code(502);
    return {
      jsonrpc: "2.0",
      id: null,
      error: {
        code: -32003,
        message: `purple-mcp upstream unreachable: ${err instanceof Error ? err.message : String(err)}`,
      },
    };
  }

  // Mirror upstream status + relevant headers, then stream the body.
  reply.code(upstreamRes.status);
  upstreamRes.headers.forEach((value, key) => {
    // Skip hop-by-hop headers; let Fastify/Node manage transfer encoding.
    const lower = key.toLowerCase();
    if (
      lower === "transfer-encoding" ||
      lower === "connection" ||
      lower === "keep-alive" ||
      lower === "content-length"
    ) {
      return;
    }
    reply.header(key, value);
  });

  if (!upstreamRes.body) {
    return reply.send();
  }
  // Stream the body through. Fastify accepts a Node Readable, so convert.
  const { Readable } = await import("node:stream");
  const nodeStream = Readable.fromWeb(upstreamRes.body as any);
  return reply.send(nodeStream);
});

// ---------- shutdown ----------

function shutdown(signal: string) {
  process.stderr.write(`[s1] received ${signal}, shutting down\n`);
  for (const [, child] of children) {
    try { child.proc.kill("SIGTERM"); } catch { /* ignore */ }
  }
  app.close().finally(() => process.exit(0));
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

app.listen({ host: "0.0.0.0", port: PORT }).then(
  () => {
    process.stderr.write(`[s1] sentinelone-mcp wrapper listening on :${PORT}\n`);
  },
  (err) => {
    process.stderr.write(`[s1] failed to listen: ${err}\n`);
    process.exit(1);
  },
);
