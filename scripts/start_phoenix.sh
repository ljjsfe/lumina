#!/bin/bash
# Launch Phoenix trace viewer
# Usage: ./scripts/start_phoenix.sh
#
# After starting, open http://localhost:6006 in your browser.
# All traces are persisted in ~/.phoenix/ — they survive restarts.

set -e

echo "Starting Phoenix trace viewer..."
echo "  UI: http://localhost:6006"
echo "  Data: ~/.phoenix/"
echo ""

export PHOENIX_PORT=6006
export PHOENIX_HOST=127.0.0.1
phoenix serve
