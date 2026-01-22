#!/bin/bash
# setup-colima.sh - Complete Colima setup for Metaflow development

set -e

COLIMA_PROFILE="${COLIMA_PROFILE:-default}"
COLIMA_CPUS="${COLIMA_CPUS:-4}"
COLIMA_MEMORY="${COLIMA_MEMORY:-8}"
COLIMA_DISK="${COLIMA_DISK:-60}"

echo "Setting up Colima for Metaflow development..."

# Check if Colima is installed
if ! command -v colima &> /dev/null; then
    echo "❌ Colima not found. Installing via Homebrew..."
    brew install colima
fi

# Check if Docker CLI is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker CLI not found. Installing..."
    brew install docker
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "⚠️  Docker Compose not found. Installing..."
    brew install docker-compose
fi

# Stop existing Colima instance if running
if colima status &> /dev/null; then
    echo "Stopping existing Colima instance..."
    colima stop
fi

# Start Colima with appropriate resources for Metaflow
echo "Starting Colima with optimized settings for Metaflow..."
echo "  CPUs: ${COLIMA_CPUS}"
echo "  Memory: ${COLIMA_MEMORY}GB"
echo "  Disk: ${COLIMA_DISK}GB"

# Detect macOS version for vm-type
if [[ $(uname) == "Darwin" ]] && [[ $(sw_vers -productVersion | cut -d. -f1) -ge 13 ]]; then
    VM_TYPE="--vm-type vz"
else
    VM_TYPE=""
fi

colima start \
    --cpu "${COLIMA_CPUS}" \
    --memory "${COLIMA_MEMORY}" \
    --disk "${COLIMA_DISK}" \
    ${VM_TYPE} \
    --network-address

# Wait for Colima to be ready
echo "Waiting for Colima to be ready..."
for i in {1..30}; do
    if docker version &> /dev/null; then
        break
    fi
    sleep 1
done

# Set up Docker context
echo "Configuring Docker context..."
docker context use colima 2>/dev/null || {
    echo "Creating colima context..."
    docker context create colima \
        --description "Colima Docker Desktop replacement" \
        --docker "host=unix://${HOME}/.colima/${COLIMA_PROFILE}/docker.sock"
    docker context use colima
}

# Create environment file
echo "Creating environment configuration..."
cat > .env.colima <<EOF
# Colima configuration for Metaflow
export DOCKER_HOST="unix://\${HOME}/.colima/${COLIMA_PROFILE}/docker.sock"
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_DEFAULT_PLATFORM=linux/amd64
EOF

# Create a helper script
cat > activate-colima.sh <<'EOF'
#!/bin/bash
# Source this file to activate Colima environment
# Usage: source activate-colima.sh

if [ -f .env.colima ]; then
    source .env.colima
    echo "✓ Colima environment activated"
    echo "  DOCKER_HOST=${DOCKER_HOST}"
else
    echo "❌ .env.colima not found. Run setup-colima.sh first."
fi
EOF
chmod +x activate-colima.sh

# Test the setup
echo "Testing Docker connectivity..."
if docker run --rm hello-world &> /dev/null; then
    echo "✓ Docker is working correctly through Colima"
else
    echo "❌ Docker test failed"
    exit 1
fi

echo ""
echo "✅ Colima setup complete!"
echo ""
echo "To use Colima in your current shell, run:"
echo "  source activate-colima.sh"