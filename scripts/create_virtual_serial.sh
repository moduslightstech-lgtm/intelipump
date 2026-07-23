#!/usr/bin/env bash
set -euo pipefail
rm -f /tmp/dart-controller /tmp/dart-pump
echo "Controller: /tmp/dart-controller"
echo "Simulator:  /tmp/dart-pump"
exec socat -d -d   PTY,raw,echo=0,link=/tmp/dart-controller   PTY,raw,echo=0,link=/tmp/dart-pump
