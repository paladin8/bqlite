#!/bin/sh
set -eu

payload="$(cat)"
title="$(printf '%s' "$payload" | jq -r '.title // "Claude Code"')"
message="$(printf '%s' "$payload" | jq -r '.message // "Claude needs attention"')"

# Strip chars that would break the OSC 777 sequence.
title=$(printf '%s' "$title" | tr '\r\n' ' ' | tr ';' ':')
message=$(printf '%s' "$message" | tr '\r\n' ' ' | tr ';' ':')

if [ -w /dev/tty ]; then
  printf '\033]777;notify;%s;%s\033\\' "$title" "$message" > /dev/tty
fi
