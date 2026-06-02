# syntax=docker/dockerfile:1.10
#
# Multitenant Streamable HTTP wrapper for sentinel-one/purple-mcp.
#
# purple-mcp natively supports streamable-http transport but reads its
# SentinelOne credentials (PURPLEMCP_CONSOLE_TOKEN / PURPLEMCP_CONSOLE_BASE_URL)
# from environment variables at process startup. The wyre-technology MCP
# gateway forwards per-tenant credentials as HTTP headers on every request,
# so this image runs a small Node proxy on port 8080 that:
#
#   1. Reads the SentinelOne credential headers from the incoming request
#   2. Lazily spawns one purple-mcp child per (token, base_url) tenant on a
#      private loopback port with the right env vars
#   3. Proxies the request to that child and streams the response back
#   4. Evicts idle children after a timeout
#
# Result: a single container that the gateway can talk to like any other
# vendor MCP server (POST http://gwp-sentinelone:8080/mcp).

# ---- Stage 1: install purple-mcp into a virtualenv via uv ----
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS upstream

ENV PURPLE_MCP_REF=main
WORKDIR /opt
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth=1 --branch "${PURPLE_MCP_REF}" \
      https://github.com/Sentinel-One/purple-mcp.git /opt/purple-mcp \
 && cd /opt/purple-mcp \
 && git submodule update --init --recursive --depth=1 \
 && uv sync --locked --no-dev

# ---- Stage 2: build the Node proxy ----
FROM node:22-bookworm-slim AS proxy-build
WORKDIR /app
COPY package.json package-lock.json* tsconfig.json ./
RUN npm install
COPY src ./src
RUN npm run build

# ---- Stage 3: runtime image ----
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    NODE_ENV=production \
    PORT=8080

# Install Node.js 22 (no apt repo - use the official tarball)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl xz-utils \
 && curl -fsSL https://nodejs.org/dist/v22.11.0/node-v22.11.0-linux-x64.tar.xz -o /tmp/node.tar.xz \
 && tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1 \
 && rm /tmp/node.tar.xz \
 && rm -rf /var/lib/apt/lists/*

# Bring in purple-mcp + its venv
COPY --from=upstream /opt/purple-mcp /opt/purple-mcp
ENV PURPLE_MCP_DIR=/opt/purple-mcp \
    PURPLE_MCP_PYTHON=/opt/purple-mcp/.venv/bin/python

# Bring in the compiled proxy
WORKDIR /app
COPY --from=proxy-build /app/node_modules ./node_modules
COPY --from=proxy-build /app/dist ./dist
COPY --from=proxy-build /app/package.json ./package.json

LABEL io.modelcontextprotocol.server.name="io.github.wyre-technology/sentinelone-mcp"

EXPOSE 8080
CMD ["node", "dist/index.js"]
