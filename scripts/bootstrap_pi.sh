#!/usr/bin/env bash
set -euo pipefail
sudo apt-get update
sudo apt-get install -y curl git socat
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
echo "Next:"
echo "  uv sync --dev"
echo "  cp .env.example .env"
echo "  uv run intelipump-fdc"
