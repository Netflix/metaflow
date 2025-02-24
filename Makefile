SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

help:
	@echo "Available targets:"
	@echo "  make up          - Start the development environment"
	@echo "  make shell       - Switch to development environment's shell"
	@echo "  make dashboard   - Open Minikube dashboard"
	@echo "  make down        - Stop and clean up the environment"
	@echo "  make help        - Show this help message"

HELM_VERSION := v3.14.0
MINIKUBE_VERSION := v1.32.0
TILT_VERSION := v0.33.11
DEVTOOLS_DIR := $(CURDIR)/.devtools
MINIKUBE_DIR := $(DEVTOOLS_DIR)/minikube
MINIKUBE := $(MINIKUBE_DIR)/minikube
TILT_DIR := $(DEVTOOLS_DIR)/tilt
TILT := $(TILT_DIR)/tilt

MINIKUBE_CPUS ?= 4
MINIKUBE_MEMORY ?= 6000
MINIKUBE_DISK_SIZE ?= 20g

ifeq ($(shell uname), Darwin)
	minikube_os = darwin
	tilt_os = mac
else
	minikube_os = linux
	tilt_os = linux
endif

ifeq ($(shell uname -m), x86_64)
	arch = amd64
	tilt_arch = x86_64
else
	arch = arm64
	tilt_arch = arm64
endif

# TODO: Move scripts to a folder

install-helm:
	@if ! command -v helm >/dev/null 2>&1; then \
		echo "📥 Installing Helm $(HELM_VERSION) (may require sudo access)..."; \
		curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | HELM_INSTALL_VERSION=$(HELM_VERSION) bash; \
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
			HOMEBREW_NO_AUTO_UPDATE=1 brew install curl; \
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

# TODO: fix gum install on linux
install-gum:
	@echo "🔍 Checking if gum is installed..."
	@if ! command -v gum >/dev/null 2>&1; then \
		echo "📥 Installing gum..."; \
		if [ "$(shell uname)" = "Darwin" ]; then \
			HOMEBREW_NO_AUTO_UPDATE=1 brew install gum; \
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
			--cpus $(MINIKUBE_CPUS) \
			--memory $(MINIKUBE_MEMORY) \
			--disk-size $(MINIKUBE_DISK_SIZE) \
			--driver docker; \
		echo "🔌 Enabling metrics-server and dashboard (quietly)..."; \
		$(MINIKUBE) addons enable metrics-server >/dev/null 2>&1; \
		$(MINIKUBE) addons enable dashboard >/dev/null 2>&1; \
	else \
		echo "✅ Minikube $(MINIKUBE_VERSION) cluster is already running"; \
	fi
	@echo "🎉 Minikube $(MINIKUBE_VERSION) cluster is ready!"

setup-tilt:
	@if [ ! -f "$(TILT)" ]; then \
		echo "📥 Installing Tilt $(TILT_VERSION)"; \
		mkdir -p $(TILT_DIR); \
		(curl -L https://github.com/tilt-dev/tilt/releases/download/$(TILT_VERSION)/tilt.$(TILT_VERSION:v%=%).$(tilt_os).$(tilt_arch).tar.gz | tar -xz -C $(TILT_DIR)) && echo "✅ Tilt $(TILT_VERSION) installed successfully" || (echo "❌ Failed to install Tilt" && exit 1); \
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
up: install-brew check-docker install-curl install-gum setup-minikube install-helm setup-tilt
	@echo "🚀 Starting up (may require sudo access)..."
	@mkdir -p $(DEVTOOLS_DIR)
	@echo '#!/bin/bash' > $(DEVTOOLS_DIR)/start.sh
	@echo 'set -e' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'trap "exit" INT TERM' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'trap "kill 0" EXIT' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'eval $$($(MINIKUBE) docker-env)' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo "📝 Selecting services..."' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'SERVICES=$$(./pick_services.sh)' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo "Selected services: $$SERVICES"' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" $(MINIKUBE) tunnel &' >> $(DEVTOOLS_DIR)/start.sh
	@echo '$(MAKE) create-dev-shell' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo "🔥 Starting Tilt with selected services..."' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" tilt up' >> $(DEVTOOLS_DIR)/start.sh
# @echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" tilt up -- services="$$SERVICES"' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'rm -f /tmp/metaflow-devshell-*' >> $(DEVTOOLS_DIR)/start.sh
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

shell: setup-tilt
	@echo "⏳ Checking if development environment is up..."
	@if ! $(TILT) get session > /dev/null 2>&1; then \
		echo "❌ Development environment is not up."; \
		echo "   Please run 'make up' in another terminal, then re-run 'make shell'."; \
		exit 1; \
	fi
	@echo "⏳ Waiting for development environment to be ready..."
	@$(TILT) wait --for=condition=Ready uiresource/generate-configs
	@echo "🔧 Starting a new shell for development environment..."
	@bash -c '\
		if [ -n "$$SHELL" ]; then \
			user_shell="$$SHELL"; \
		else \
			user_shell="$(SHELL)"; \
		fi; \
		echo "🔎 Using $$user_shell for interactive session."; \
		if [ -f "$(PWD)/.devtools/aws_config" ]; then \
			env METAFLOW_HOME="$(PWD)/.devtools" \
				METAFLOW_PROFILE=local \
				AWS_CONFIG_FILE="$(PWD)/.devtools/aws_config" \
				"$$user_shell" -i; \
		else \
			env METAFLOW_HOME="$(PWD)/.devtools" \
				METAFLOW_PROFILE=local \
				"$$user_shell" -i; \
		fi'

create-dev-shell: setup-tilt
	@bash -c '\
		SHELL_PATH=/tmp/metaflow-dev-shell-$$$$ && \
		echo "#!/bin/bash" > $$SHELL_PATH && \
		echo "set -e" >> $$SHELL_PATH && \
		echo "" >> $$SHELL_PATH && \
		echo "echo \"⏳ Checking if development environment is up...\"" >> $$SHELL_PATH && \
		echo "if ! $(TILT) get session >/dev/null 2>&1; then" >> $$SHELL_PATH && \
		echo "  echo \"❌ Development environment is not up.\"" >> $$SHELL_PATH && \
		echo "  echo \"   Please run '\''make up'\'' in another terminal, then re-run this script.\"" >> $$SHELL_PATH && \
		echo "  exit 1" >> $$SHELL_PATH && \
		echo "fi" >> $$SHELL_PATH && \
		echo "" >> $$SHELL_PATH && \
		echo "echo \"⏳ Waiting for development environment to be ready...\"" >> $$SHELL_PATH && \
		echo "$(TILT) wait --for=condition=Ready uiresource/generate-configs" >> $$SHELL_PATH && \
		echo "" >> $$SHELL_PATH && \
		echo "echo \"🔧 Starting a new shell for development environment...\"" >> $$SHELL_PATH && \
		echo "if [ -n \"\$$SHELL\" ]; then" >> $$SHELL_PATH && \
		echo "    user_shell=\"\$$SHELL\"" >> $$SHELL_PATH && \
		echo "else" >> $$SHELL_PATH && \
		echo "    user_shell=\"$(SHELL)\"" >> $$SHELL_PATH && \
		echo "fi" >> $$SHELL_PATH && \
		echo "echo \"🔎 Using \$$user_shell for interactive session.\"" >> $$SHELL_PATH && \
		echo "if [ -f \"$$(pwd)/.devtools/aws_config\" ]; then" >> $$SHELL_PATH && \
		echo "    env METAFLOW_HOME=\"$$(pwd)/.devtools\" \\" >> $$SHELL_PATH && \
		echo "        METAFLOW_PROFILE=local \\" >> $$SHELL_PATH && \
		echo "        AWS_CONFIG_FILE=\"$$(pwd)/.devtools/aws_config\" \\" >> $$SHELL_PATH && \
		echo "        \"\$$user_shell\" -i" >> $$SHELL_PATH && \
		echo "else" >> $$SHELL_PATH && \
		echo "    env METAFLOW_HOME=\"$$(pwd)/.devtools\" \\" >> $$SHELL_PATH && \
		echo "        METAFLOW_PROFILE=local \\" >> $$SHELL_PATH && \
		echo "        \"\$$user_shell\" -i" >> $$SHELL_PATH && \
		echo "fi" >> $$SHELL_PATH && \
		chmod +x $$SHELL_PATH && \
		echo "✨ Created $$SHELL_PATH" && \
		echo "🔑  Execute it from ANY directory to switch to development environment shell!" \
	'	

.PHONY: install-helm setup-minikube setup-tilt teardown-minikube tunnel up down check-docker install-curl install-gum install-brew up down dashboard shell help

.DEFAULT_GOAL := up