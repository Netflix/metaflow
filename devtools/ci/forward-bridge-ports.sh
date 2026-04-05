#!/usr/bin/env bash
set -euo pipefail
#
# Forward devstack ports to the Docker bridge gateway so containers
# (launched by localbatch) can reach MinIO, metadata service, and DynamoDB
# via host.docker.internal.
#
# On Linux, kubectl port-forward binds to 127.0.0.1 only. Docker containers
# resolve host.docker.internal to the bridge gateway (172.17.0.1), which
# can't reach 127.0.0.1. socat bridges the gap.
#
# Options:
#   --verbose   Print network diagnostics before forwarding

verbose=false
for arg in "$@"; do
  case "$arg" in
    --verbose) verbose=true ;;
  esac
done

sudo apt-get install -y -q socat

BRIDGE=$(docker network inspect bridge \
  --format='{{range .IPAM.Config}}{{.Gateway}}{{end}}')
echo "Docker bridge gateway: ${BRIDGE}"

if [[ "$verbose" == "true" || "${RUNNER_DEBUG:-}" == "1" ]]; then
  echo "=== Network diagnostic: host port 8000 ==="
  echo "Host 127.0.0.1:8000 test:"
  timeout 3 bash -c 'echo > /dev/tcp/127.0.0.1/8000' 2>&1 && echo 'CONNECT OK' || echo 'CONNECT FAILED'
  echo "Host ${BRIDGE}:8000 test:"
  timeout 3 bash -c "echo > /dev/tcp/${BRIDGE}/8000" 2>&1 && echo "CONNECT OK" || echo "CONNECT FAILED"
  echo "iptables INPUT on port 8000:"
  sudo iptables -L INPUT -n | grep "8000\|REJECT\|DROP" | head -5 || echo "no rules"
  echo "ufw status:"
  sudo ufw status 2>&1 | head -3 || echo "ufw not available"
  MINIKUBE_CONTAINER=$(docker ps --filter "name=minikube" --format "{{.ID}}" | head -1)
  echo "minikube container: ${MINIKUBE_CONTAINER}"
  MINIKUBE_GW=$(docker network inspect minikube --format='{{range .IPAM.Config}}{{.Gateway}}{{end}}' 2>/dev/null || echo "192.168.49.1")
  echo "minikube gateway: ${MINIKUBE_GW}"
  docker exec "${MINIKUBE_CONTAINER}" bash -c "
    echo 'Trying 172.17.0.1:8000...'
    timeout 3 bash -c 'echo > /dev/tcp/172.17.0.1/8000' 2>&1 && echo 'CONNECT OK' || echo 'CONNECT FAILED'
    echo 'Trying ${MINIKUBE_GW}:8000...'
    timeout 3 bash -c 'echo > /dev/tcp/${MINIKUBE_GW}/8000' 2>&1 && echo 'CONNECT OK' || echo 'CONNECT FAILED'
    echo 'ip route:'
    ip route | head -5
  " || echo "minikube exec failed"
fi

# MinIO (S3), Metaflow metadata service, and DynamoDB local
sudo socat TCP-LISTEN:9000,fork,reuseaddr,bind="${BRIDGE}" TCP:127.0.0.1:9000 &
sudo socat TCP-LISTEN:8080,fork,reuseaddr,bind="${BRIDGE}" TCP:127.0.0.1:8080 &
sudo socat TCP-LISTEN:8765,fork,reuseaddr,bind="${BRIDGE}" TCP:127.0.0.1:8765 &
sleep 1
echo "Forwarding 9000, 8080, and 8765 on ${BRIDGE} → 127.0.0.1"
