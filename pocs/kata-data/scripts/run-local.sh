#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════╗"
echo "║   KataData 2026 — Pipeline Runner            ║"
echo "║   DataFusion + SQLite + SOAP + axum          ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"

SOAP_PID=""
PROMETHEUS_CID=""
CONTAINER_CMD=""

if command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
fi

cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down...${NC}"
    [ -n "$SOAP_PID" ] && kill "$SOAP_PID" 2>/dev/null && echo "  SOAP mock stopped"
    if [ -n "$PROMETHEUS_CID" ] && [ -n "$CONTAINER_CMD" ]; then
        $CONTAINER_CMD stop "$PROMETHEUS_CID" > /dev/null 2>&1 && echo "  Prometheus stopped"
    fi
    echo "Done."
}
trap cleanup EXIT INT TERM

echo -e "${YELLOW}[1/5] Building all binaries...${NC}"
cargo build --release || { echo -e "${RED}Build failed${NC}"; exit 1; }

echo -e "${YELLOW}[2/5] Starting Prometheus on :9090...${NC}"

if [ -z "$CONTAINER_CMD" ]; then
    echo -e "${RED}  No container runtime found (podman or docker) — skipping Prometheus${NC}"
else
    echo "  Using: $CONTAINER_CMD"

    # Stop any existing container on port 9090
    EXISTING=$($CONTAINER_CMD ps -q --filter "publish=9090" 2>/dev/null)
    if [ -n "$EXISTING" ]; then
        echo "  Stopping existing container on :9090..."
        $CONTAINER_CMD stop "$EXISTING" > /dev/null 2>&1 || true
        sleep 1
    fi

    mkdir -p "$(pwd)/config"

    cat > "$(pwd)/config/prometheus-local.yml" << 'PROMEOF'
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: kata-data-api
    static_configs:
      - targets: ["host.containers.internal:8080"]
    metrics_path: /metrics
PROMEOF

    PROMETHEUS_CID=$($CONTAINER_CMD run -d \
        --rm \
        -p 9090:9090 \
        -v "$(pwd)/config/prometheus-local.yml:/etc/prometheus/prometheus.yml:ro" \
        --add-host=host.containers.internal:host-gateway \
        prom/prometheus:latest \
        --config.file=/etc/prometheus/prometheus.yml 2>&1) || true

    if echo "$PROMETHEUS_CID" | grep -qE '^[a-f0-9]{64}$'; then
        echo -e "${GREEN}  Prometheus started → http://localhost:9090${NC}"
    else
        echo -e "${RED}  Prometheus failed to start:${NC}"
        echo "  $PROMETHEUS_CID"
        echo -e "${RED}  Tip: make sure port 9090 is free and $CONTAINER_CMD machine is running${NC}"
        PROMETHEUS_CID=""
    fi
fi

echo -e "${YELLOW}[3/5] Starting Mock SOAP WS-* server on :8081...${NC}"
RUST_LOG=info cargo run --release --package mock-soap --bin mock-soap &
SOAP_PID=$!

echo "  Waiting for SOAP server..."
for i in {1..15}; do
    if curl -sf http://localhost:8081/wsdl > /dev/null 2>&1; then
        echo -e "${GREEN}  SOAP server ready!${NC}"
        break
    fi
    sleep 1
    if [ "$i" -eq 15 ]; then
        echo -e "${RED}  SOAP server did not start in time${NC}"
    fi
done

echo -e "${YELLOW}[4/5] Running data pipelines...${NC}"
RUST_LOG=info cargo run --release --bin pipeline || { echo -e "${RED}Pipeline failed${NC}"; exit 1; }

echo -e "${YELLOW}[5/5] Starting REST API on :8080...${NC}"
echo ""
echo -e "${GREEN}┌──────────────────────────────────────────────────┐${NC}"
echo -e "${GREEN}│  Services                                        │${NC}"
echo -e "${GREEN}├──────────────────────────────────────────────────┤${NC}"
echo -e "${GREEN}│     Scalar Docs  >  http://localhost:8080/docs   │${NC}"
echo -e "${GREEN}│     Health       >  http://localhost:8080/health │${NC}"
echo -e "${GREEN}│     Prometheus   >  http://localhost:9090        │${NC}"
echo -e "${GREEN}│     SOAP WSDL    >  http://localhost:8081/wsdl   │${NC}"
echo -e "${GREEN}└──────────────────────────────────────────────────┘${NC}"
echo ""
echo "Press Ctrl+C to stop all services"
echo ""

RUST_LOG=info cargo run --release --bin api
