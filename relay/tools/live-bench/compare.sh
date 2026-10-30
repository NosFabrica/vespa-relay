#!/usr/bin/env bash
# Alternates variants of the relay through cpu.sh for ROUNDS rounds, so drift on a shared machine falls on every
# variant alike instead of on whichever ran last. One variant per line of the file:
#
#   name | VAR=value | VAR=value ...
#
#   baseline   | NATIVE_WEBSOCKET=true
#   parallelgc | NATIVE_WEBSOCKET=true | JAVA_TOOL_OPTIONS=-Xms4g -Xmx4g -XX:+UseParallelGC
#   jdk25      | NATIVE_WEBSOCKET=true | JAVA_HOME=/opt/jdk-25
#   other-build| RELAY_HOME=/path/to/another/install | NATIVE_WEBSOCKET=true
#
#   compare.sh VARIANTS_FILE [ROUNDS=2]
#
# RELAY_HOME picks the build for that variant; every other pair is the relay's environment.
source "$(dirname "$0")/lib.sh"
file=$1
rounds=${2:-2}
[ -f "$file" ] || { echo "usage: compare.sh VARIANTS_FILE [ROUNDS]" >&2; exit 2; }
for round in $(seq 1 "$rounds"); do
  while IFS= read -r line; do
    [[ -z "${line// /}" || "$line" == \#* ]] && continue
    IFS='|' read -r -a parts <<< "$line"
    name=$(echo "${parts[0]}" | xargs)
    home=$RELAY_HOME
    env_args=()
    for part in "${parts[@]:1}"; do
      pair=$(echo "$part" | sed 's/^ *//; s/ *$//')
      [ -z "$pair" ] && continue
      if [[ "$pair" == RELAY_HOME=* ]]; then home=${pair#RELAY_HOME=}; else env_args+=("$pair"); fi
    done
    echo "$name (round $round): $(RELAY_HOME=$home "$(dirname "$0")/cpu.sh" "${env_args[@]}" 2>/dev/null | tail -1)"
  done < "$file"
done
