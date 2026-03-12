#!/bin/bash
# Start LiteLLM proxy for GMI Cloud routing
# This must be running before NanoClaw starts (when using GMI mode)

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load GMI_API_KEY from .env
if [ -f "$PROJECT_DIR/.env" ]; then
  export GMI_API_KEY=$(grep '^GMI_API_KEY=' "$PROJECT_DIR/.env" | cut -d'=' -f2-)
fi

if [ -z "$GMI_API_KEY" ]; then
  echo "Error: GMI_API_KEY not found in .env"
  exit 1
fi

export LITELLM_MASTER_KEY=sk-litellm-nanoclaw

echo "Starting LiteLLM proxy on port 4000 (GMI Cloud)..."
exec litellm --config "$PROJECT_DIR/litellm-config.yaml" --port 4000
