SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

#   make up      - Start the development environment
#   make down    - Stop and clean up the environment

ifeq ($(shell uname), Darwin)
	minikube_os = darwin
	tilt_os = mac
else
	minikube_os = linux
	tilt_os = linux
endif

ifeq ($(shell uname -m), x86_64)
	arch = amd64
else
	arch = arm64
endif

HELM_VERSION := v3.14.0
MINIKUBE_VERSION := v1.32.0
TILT_VERSION := v0.33.11
DEVTOOLS_DIR := $(CURDIR)/.devtools
MINIKUBE_DIR := $(DEVTOOLS_DIR)/minikube
MINIKUBE := $(MINIKUBE_DIR)/minikube
TILT_DIR := $(DEVTOOLS_DIR)/tilt
TILT := $(TILT_DIR)/tilt

helm:
	@if ! command -v helm >/dev/null 2>&1; then \
		echo "📥 Installing Helm $(HELM_VERSION) (may require sudo access)..."; \
		curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash; \
		echo "✅ Helm installation complete"; \
	fi

check-docker:
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "❌ Docker is not installed. Please install Docker first: https://docs.docker.com/get-docker/"; \
		exit 1; \
	fi
	@echo "🔍 Checking Docker daemon..."
	@if [ "$(shell uname)" = "Darwin" ]; then \
		open -a Docker || (echo "❌ Please start Docker Desktop" && exit 1); \
	else \
		systemctl is-active --quiet docker || (echo "❌ Docker daemon is not running. Start with 'sudo systemctl start docker'" && exit 1); \
	fi
	@echo "✅ Docker is running"

install-brew:
	@if [ "$(shell uname)" = "Darwin" ] && ! command -v brew >/dev/null 2>&1; then \
		echo "📥 Installing Homebrew..."; \
		/bin/bash -c "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"; \
		echo "✅ Homebrew installation complete"; \
	fi

install-curl:
	@if ! command -v curl >/dev/null 2>&1; then \
		echo "📥 Installing curl..."; \
		if [ "$(shell uname)" = "Darwin" ]; then \
			brew install curl; \
		elif command -v apt-get >/dev/null 2>&1; then \
			sudo apt-get update && sudo apt-get install -y curl; \
		elif command -v yum >/dev/null 2>&1; then \
			sudo yum install -y curl; \
		elif command -v dnf >/dev/null 2>&1; then \
			sudo dnf install -y curl; \
		else \
			echo "❌ Could not install curl. Please install manually."; \
			exit 1; \
		fi; \
		echo "✅ curl installation complete"; \
	fi

install-gum:
	@echo "🔍 Checking if gum is installed..."
	@if ! command -v gum >/dev/null 2>&1; then \
		echo "📥 Installing gum..."; \
		if [ "$(shell uname)" = "Darwin" ]; then \
			brew install gum; \
		elif command -v apt-get >/dev/null 2>&1; then \
			sudo apt-get update && sudo apt-get install -y gum; \
		elif command -v yum >/dev/null 2>&1; then \
			sudo yum install -y gum; \
		elif command -v dnf >/dev/null 2>&1; then \
			sudo dnf install -y gum; \
		else \
			echo "❌ Could not determine how to install gum for your platform. Please install manually."; \
			exit 1; \
		fi; \
		echo "✅ gum installation complete"; \
	else \
		echo "✅ gum is already installed."; \
	fi

setup-minikube:
	@if [ ! -f "$(MINIKUBE)" ]; then \
		echo "📥 Installing Minikube $(MINIKUBE_VERSION)"; \
		mkdir -p $(MINIKUBE_DIR); \
		curl -L --fail https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-$(minikube_os)-$(arch) -o $(MINIKUBE) || (echo "❌ Failed to download minikube" && exit 1); \
		chmod +x $(MINIKUBE); \
		echo "✅ Minikube $(MINIKUBE_VERSION) installed successfully"; \
	fi
	@echo "🔧 Setting up Minikube $(MINIKUBE_VERSION) cluster..."
	@if ! $(MINIKUBE) status >/dev/null 2>&1; then \
		echo "🚀 Starting new Minikube $(MINIKUBE_VERSION) cluster..."; \
		$(MINIKUBE) start \
			--cpus 4 \
			--memory 6000 \
			--disk-size 20g \
			--driver docker; \
			--docker-env="DOCKER_DEFAULT_PLATFORM=linux/amd64"; \
		echo "🔌 Enabling addons..."; \
		$(MINIKUBE) addons enable metrics-server; \
		$(MINIKUBE) addons enable dashboard; \
	else \
		echo "✅ Minikube $(MINIKUBE_VERSION) cluster is already running"; \
	fi
	@echo "🎉 Minikube $(MINIKUBE_VERSION) cluster is ready!"

setup-tilt:
	@if [ ! -f "$(TILT)" ]; then \
		echo "📥 Installing Tilt $(TILT_VERSION)"; \
		mkdir -p $(TILT_DIR); \
		curl -L --fail https://github.com/tilt-dev/tilt/releases/download/$(TILT_VERSION)/tilt.$(TILT_VERSION:v%=%).$(tilt_os).$(arch).tar.gz | tar -xz -C $(TILT_DIR) || (echo "❌ Failed to install Tilt" && exit 1); \
		echo "✅ Tilt $(TILT_VERSION) installed successfully"; \
	fi

tunnel:
	$(MINIKUBE) tunnel

teardown-minikube:
	@echo "🛑 Stopping Minikube $(MINIKUBE_VERSION) cluster..."
	-$(MINIKUBE) stop
	@echo "🗑️  Deleting Minikube $(MINIKUBE_VERSION) cluster..."
	-$(MINIKUBE) delete --all
	@echo "🧹 Removing Minikube binary..."
	-rm -rf $(MINIKUBE_DIR)
	@echo "✅ Minikube $(MINIKUBE_VERSION) teardown complete"

dashboard:
	@echo "🔗 Opening Minikube Dashboard..."
	@$(MINIKUBE) dashboard

# TODO: Move from @echo to @cat
up: install-brew check-docker install-curl install-gum setup-minikube helm setup-tilt
	@echo "🚀 Starting up (may require sudo access)..."
	@mkdir -p $(DEVTOOLS_DIR)
	@echo '#!/bin/bash' > $(DEVTOOLS_DIR)/start.sh
	@echo 'set -e' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'trap "exit" INT TERM' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'trap "kill 0" EXIT' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'eval $$($(MINIKUBE) docker-env)' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo "📝 Selecting services..."' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'SERVICES=$$(./pick_services.sh)' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" $(MINIKUBE) tunnel &' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo "🔥 Starting Tilt with selected services..."' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" tilt up' >> $(DEVTOOLS_DIR)/start.sh
# @echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" tilt up -- services="$$SERVICES"' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'wait' >> $(DEVTOOLS_DIR)/start.sh
	@chmod +x $(DEVTOOLS_DIR)/start.sh
	@$(DEVTOOLS_DIR)/start.sh

down:
	@echo "🛑 Stopping all services..."
	@-pkill -f "$(MINIKUBE) tunnel" 2>/dev/null || true
	@echo "⏹️  Stopping Tilt..."
	-PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" tilt down
	@echo "🧹 Cleaning up Minikube..."
	$(MAKE) teardown-minikube
	@echo "🗑️  Removing Tilt binary and directory..."
	-rm -rf $(TILT_DIR)
	@echo "🧹 Removing temporary scripts..."
	-rm -f $(DEVTOOLS_DIR)/start.sh
	@echo "✨ All done!"

help:
	@echo "Available targets:"
	@echo "  make up          - Start the development environment"
	@echo "  make down        - Stop and clean up the environment"
	@echo "  make dashboard   - Open Minikube dashboard"
	@echo "  make help        - Show this help message"

.PHONY: helm setup-minikube setup-tilt teardown-minikube tunnel up down check-docker install-curl install-gum install-brew up down dashboard help

.DEFAULT_GOAL := up