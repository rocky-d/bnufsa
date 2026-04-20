#!/bin/bash

if [ -z "$BNUFSA_HOME" ]; then
    BNUFSA_HOME="$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")"
fi

alias activate='uv sync --project "$BNUFSA_HOME" && source "$BNUFSA_HOME"/.venv/bin/activate'

bnufsa() {
    cd "$BNUFSA_HOME" || return 1
}

logd() {
    find "$BNUFSA_HOME"/logs -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1
}

log() {
    cd "$(logd)" || return 1
}

mvo() {
    out_path="$BNUFSA_HOME"/out
    if [ -f "$out_path" ]; then mv "$out_path" "$(logd)"; fi
}

pnl() {
    for arg in "$@"; do echo -n "$arg"':pnl = ' && grep 'pnl = ' "$arg" | awk -F'=' '{sum += $2} END {print 0+sum}'; done
}

peek() {
    pgrep -a python | grep "$BNUFSA_HOME"/main.py
}

start() {
    peek && return 1
    mvo && nohup uv run --directory "$BNUFSA_HOME" python "$BNUFSA_HOME"/main.py >"$BNUFSA_HOME"/out 2>&1 &
}

stop() {
    peek || return 1
    peek | awk '{print $1}' | xargs kill -2
}
