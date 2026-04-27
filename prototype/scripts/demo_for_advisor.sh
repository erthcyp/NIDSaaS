#!/usr/bin/env bash
# demo_for_advisor.sh
# ----------------------------------------------------------------------
# Live end-to-end demo of the proposed NIDSaaS architecture for an
# advisor / committee meeting. Sets up a 4-pane tmux layout that
# shows each pipeline stage processing the same stream in real time.
#
# Usage:
#   ssh siit@10.10.11.96
#   cd ~/NIDSaaS-Earth/prototype
#   bash scripts/demo_for_advisor.sh
# ----------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")/.."
PROTOTYPE_DIR="$PWD"

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
bold()   { printf "\033[1m%s\033[0m\n" "$*"; }
hr()     { printf "\033[2m%s\033[0m\n" "────────────────────────────────────────────────"; }

SESSION="nidsaas-demo"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml"

# ----------------------------------------------------------------------
# Step 1: Pre-flight
# ----------------------------------------------------------------------
hr; bold "Step 1 / 5  — Pre-flight check"; hr

if ! command -v tmux >/dev/null 2>&1; then
    red "  tmux not installed — sudo apt install -y tmux"
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    red "  Docker not reachable."
    exit 1
fi

if ! $COMPOSE ps --status running 2>/dev/null | grep -q "spark_preprocessor"; then
    yellow "  Spark preprocessor not running — bringing stack up..."
    $COMPOSE up -d
    yellow "  waiting 60 s for Spark to be ready..."
    sleep 60
fi

if ! curl -sf -m 5 localhost:8080/healthz >/dev/null 2>&1; then
    red "  Gateway not responding on :8080."
    exit 1
fi

green "  ✓ stack healthy"

# Kill any old demo session
tmux kill-session -t "$SESSION" 2>/dev/null || true

# ----------------------------------------------------------------------
# Step 2: Reset webhook + stop tenant_simulator
# ----------------------------------------------------------------------
hr; bold "Step 2 / 5  — Clean state"; hr
curl -sf -X POST localhost:9000/traces/reset >/dev/null
$COMPOSE stop tenant_simulator >/dev/null 2>&1 || true
green "  ✓ webhook traces cleared, tenant_simulator stopped"

# ----------------------------------------------------------------------
# Step 3: Write pane helper scripts
# ----------------------------------------------------------------------
hr; bold "Step 3 / 5  — Generating pane scripts"; hr

# Pane 0 — Spark preprocessor log
cat > /tmp/demo_pane0.sh <<EOF
#!/bin/bash
cd $PROTOTYPE_DIR
clear
echo '=== 1. SPARK PREPROCESSOR (validates + stages flows) ==='
echo
$COMPOSE logs -f --tail=0 spark_preprocessor 2>&1 \\
    | grep --line-buffered -E 'spark-preprocessor|streaming started|batch|spent|WARN'
EOF
chmod +x /tmp/demo_pane0.sh

# Pane 1 — Detector log
cat > /tmp/demo_pane1.sh <<EOF
#!/bin/bash
cd $PROTOTYPE_DIR
clear
echo '=== 2. DETECTOR (Hybrid Cascade scoring) ==='
echo
$COMPOSE logs -f --tail=0 detector 2>&1 \\
    | grep --line-buffered -E 'worker|consumer|tier|verdict' \\
    | grep -v --line-buffered 'aiokafka.consumer'
EOF
chmod +x /tmp/demo_pane1.sh

# Pane 2 — Webhook receiver log
cat > /tmp/demo_pane2.sh <<EOF
#!/bin/bash
cd $PROTOTYPE_DIR
clear
echo '=== 3. WEBHOOK RECEIVER (tenant-side alert sink) ==='
echo
$COMPOSE logs -f --tail=0 webhook_receiver 2>&1 \\
    | grep --line-buffered -E 'POST|alert|tenant'
EOF
chmod +x /tmp/demo_pane2.sh

# Pane 3 — Control terminal (interactive)
cat > /tmp/demo_pane3.sh <<EOF
#!/bin/bash
cd $PROTOTYPE_DIR
clear
echo '================================================================'
echo '  4. CONTROL  —  press ENTER to start the demo'
echo '================================================================'
echo
echo '  This pane will:'
echo '    1. Source the venv'
echo '    2. POST 900 synthetic flows over 30 s @ 30 req/s'
echo '    3. Wait 20 s for Spark micro-batches to drain'
echo '    4. Print the final alert summary + sample payload'
echo
echo '  Watch the three other panes light up with real-time activity.'
echo
read -p 'Press ENTER to begin demo ... '

# Activate venv. Print success/failure explicitly so a missing venv
# is obvious to the operator instead of failing silently downstream.
if source ~/NIDSaaS-Earth/.venv/bin/activate 2>/dev/null; then
    echo "  ✓ venv activated"
else
    echo "  ! venv activation failed — falling back to system python3"
fi
PY=\$(which python3)
echo "  using python3: \$PY"

echo
echo '>>> Resetting webhook traces ...'
curl -sf -X POST localhost:9000/traces/reset >/dev/null
sleep 1

echo '>>> Sending load: 900 flows @ 30 req/s over 30 s ...'
echo '    (you should see HTTP 202 Accepted lines streaming in for 30 s,'
echo '     then a 20 s settling pause, then the summary)'
echo

# No grep filter — let *all* output through so a stack-trace or
# missing-module error is visible immediately.
python3 loadtest/run_experiment.py e1 \\
    --mode kafka --rate 30 --duration 30 --settle 20 2>&1 \\
    | tail -20    # show only the last 20 lines (final summary)

echo
echo '>>> Final alert summary:'
curl -s localhost:9000/traces | python3 -c "
import sys, json
d = json.load(sys.stdin)
traces = d.get('traces', {})
tenants = sorted({v[0]['tenant'] for v in traces.values() if v})
print(f'  total alert events delivered: {d.get(\"count\", 0)}')
print(f'  unique tenants reporting:     {tenants}')
sample = next(iter(traces.values()), [None])[0]
if sample:
    print()
    print('  example alert payload:')
    print(json.dumps(sample, indent=4))
"

echo
echo '>>> demo complete.'
echo '    detach with Ctrl-b d'
echo '    kill all panes:  tmux kill-session -t $SESSION'
EOF
chmod +x /tmp/demo_pane3.sh

green "  ✓ pane scripts written to /tmp/demo_pane[0-3].sh"

# ----------------------------------------------------------------------
# Step 4: Build tmux layout
# ----------------------------------------------------------------------
hr; bold "Step 4 / 5  — Building 4-pane tmux dashboard"; hr

# Pass each pane's command directly to new-session / split-window so the
# command starts when the pane spawns. This avoids the send-keys race
# condition with shells that source ~/.bashrc (e.g. half-initialised
# pyenv setups print errors that visually interrupt the demo).

tmux new-session -d -s "$SESSION" -c "$PROTOTYPE_DIR" -x 220 -y 50 \
    "bash /tmp/demo_pane0.sh"

tmux split-window -h -t "$SESSION:0" -c "$PROTOTYPE_DIR" \
    "bash /tmp/demo_pane1.sh"

tmux select-pane -t "$SESSION:0.0"
tmux split-window -v -t "$SESSION:0.0" -c "$PROTOTYPE_DIR" \
    "bash /tmp/demo_pane2.sh"

tmux select-pane -t "$SESSION:0.1"
tmux split-window -v -t "$SESSION:0.1" -c "$PROTOTYPE_DIR" \
    "bash /tmp/demo_pane3.sh"

tmux select-pane -t "$SESSION:0.3"
tmux select-layout -t "$SESSION:0" tiled

green "  ✓ tmux layout ready"

# ----------------------------------------------------------------------
# Step 5: Instructions + auto-attach
# ----------------------------------------------------------------------
hr; bold "Step 5 / 5  — Demo ready"; hr
echo
echo "  Layout:"
echo "    pane 0 (top-left)     — Spark preprocessor log"
echo "    pane 1 (top-right)    — Detector log"
echo "    pane 2 (bottom-left)  — Webhook receiver log"
echo "    pane 3 (bottom-right) — Control terminal (focus here, press ENTER)"
echo
echo "  Inside tmux:"
echo "    ENTER (in bottom-right pane)  — start the load test"
echo "    Ctrl-b →/←/↑/↓                — switch focus between panes"
echo "    Ctrl-b d                      — detach (session keeps running)"
echo "    tmux a -t $SESSION            — re-attach later"
echo "    tmux kill-session -t $SESSION — close all panes"
echo
yellow "  Attaching now..."
sleep 2

tmux attach -t "$SESSION"
