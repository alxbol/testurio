#!/usr/bin/env bash
# Start kubectl port-forward to Kafka broker pod (test-stable) for local
# Step 7 of mt5-market-symbols-v2 smoke. Bash equivalent of the .ps1 script.
#
# Usage:
#   ./kafka-port-forward.sh             # default localhost:9092 -> kafka-controller-0:9092
#   ./kafka-port-forward.sh 9094        # use a different local port
#
# Run in a SEPARATE terminal; keeps the tunnel alive with auto-restart on drop.
# Ctrl+C to stop.
#
# Prereq (one-time, requires admin):
#   Add to C:\Windows\System32\drivers\etc\hosts:
#     127.0.0.1 kafka-controller-0.kafka-controller-headless.test-stable.svc.cluster.local

set -u

LOCAL_PORT="${1:-9092}"
POD="${POD:-kafka-controller-0}"
NAMESPACE="${NAMESPACE:-test-stable}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-C:/Users/abolshakov/Documents/k8s/kubeconfig}"
KUBECTL="${KUBECTL:-C:/Users/abolshakov/bin/kubectl}"

ADVERTISED_HOST="${POD}.kafka-controller-headless.${NAMESPACE}.svc.cluster.local"
HOSTS_FILE="/c/Windows/System32/drivers/etc/hosts"

# --- Pre-flight ---
[[ -x "$KUBECTL" ]] || { echo "kubectl not found at $KUBECTL" >&2; exit 1; }
[[ -f "$KUBECONFIG_PATH" ]] || { echo "Kubeconfig not found at $KUBECONFIG_PATH" >&2; exit 1; }

if [[ -r "$HOSTS_FILE" ]] && grep -q "$ADVERTISED_HOST" "$HOSTS_FILE"; then
    echo "Hosts entry OK: 127.0.0.1 $ADVERTISED_HOST"
else
    echo "MISSING hosts entry. Add (in admin PowerShell):"
    echo "  Add-Content -Path \$env:windir\\System32\\drivers\\etc\\hosts -Value '127.0.0.1 $ADVERTISED_HOST'"
    echo "Continuing anyway — kafkajs metadata resolve will fail without it."
fi

cat <<EOF

=========================================
 Kafka port-forward — keep this terminal open
 pod/$POD:9092 -> localhost:$LOCAL_PORT
 In another shell, run the test:
EOF
[[ "$LOCAL_PORT" != "9092" ]] && echo "   export KAFKA_BROKERS=localhost:$LOCAL_PORT"
cat <<EOF
   npx vitest run tests/smoke/mt5-market-symbols-v2.integration.test.ts
 Ctrl+C to stop.
=========================================

EOF

attempt=0
while true; do
    attempt=$((attempt + 1))
    stamp=$(date +%H:%M:%S)
    echo "[$stamp] attempt #$attempt — starting tunnel"
    "$KUBECTL" \
        --kubeconfig="$KUBECONFIG_PATH" \
        -n "$NAMESPACE" \
        port-forward "pod/$POD" "${LOCAL_PORT}:9092"
    code=$?
    stamp=$(date +%H:%M:%S)
    echo "[$stamp] tunnel exited with code $code, restarting in 3s..."
    sleep 3
done
