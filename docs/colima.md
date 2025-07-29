# Using Colima with Metaflow Development

Colima is an open-source alternative to Docker Desktop for macOS.

## Installation

```bash
brew install colima docker docker-compose
```

## Quick Setup

Run the setup script:
```bash
./devtools/setup-colima.sh
```

Then activate Colima in your shell:
```bash
source activate-colima.sh
```

## Manual Setup

If you prefer to set up manually:

```bash
# Start Colima
colima start --cpu 4 --memory 8 --disk 60

# Use Colima context
docker context use colima

# Set environment variables
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
```

## Verify Setup

```bash
make check-docker
```

You should see: "✓ Using Colima as Docker runtime"

## Switching Between Docker Desktop and Colima

To use Colima:
```bash
docker context use colima
source activate-colima.sh
```

To use Docker Desktop:
```bash
docker context use desktop-linux
unset DOCKER_HOST
```

## Troubleshooting

If Docker commands fail:
1. Check Colima is running: `colima status`
2. Start if needed: `colima start`
3. Re-run setup: `./devtools/setup-colima.sh`