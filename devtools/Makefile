SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

help:
	@echo "Available targets:"
	@echo "  up            - Start the development environment"
	@echo "  shell         - Switch to development environment's shell"
	@echo "  ui            - Open Metaflow UI"
	@echo "  dashboard     - Open Minikube dashboard"
	@echo "  down          - Stop and clean up the environment"
	@echo "  all-up        - Start the development environment with all services"
	@echo "  help          - Show this help message"

HELM_VERSION := v3.14.0
MINIKUBE_VERSION := v1.32.0
TILT_VERSION := v0.33.11
GUM_VERSION := v0.15.2

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MKFILE_DIR := $(dir $(MKFILE_PATH))
DEVTOOLS_DIR := $(MKFILE_DIR).devtools
PICK_SERVICES := $(MKFILE_DIR)pick_services.sh
MINIKUBE_DIR := $(DEVTOOLS_DIR)/minikube
MINIKUBE := $(MINIKUBE_DIR)/minikube
HELM_DIR := $(DEVTOOLS_DIR)/helm
TILT_DIR := $(DEVTOOLS_DIR)/tilt
TILT := $(TILT_DIR)/tilt
TILTFILE := $(MKFILE_DIR)/Tiltfile
MAKE_CMD := $(MAKE) -f "$(MKFILE_PATH)"

MINIKUBE_CPUS ?= 4
MINIKUBE_MEMORY ?= 6144
MINIKUBE_DISK_SIZE ?= 20g
WAIT_TIMEOUT ?= 300

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
		echo "📥 Installing Helm $(HELM_VERSION)..."; \
		mkdir -p "$(HELM_DIR)"; \
		curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 \
			| HELM_INSTALL_VERSION="$(HELM_VERSION)" \
			  USE_SUDO="false" \
			  PATH="$(HELM_DIR):$$PATH" \
			  HELM_INSTALL_DIR="$(HELM_DIR)" \
			  bash; \
		chmod +x "$(HELM_DIR)/helm"; \
		echo "✅ Helm installation complete"; \
	else \
		echo "✅ Helm is already installed at $$(command -v helm)"; \
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
                docker info >/dev/null 2>&1 || (echo "❌ Docker daemon is not running." && exit 1); \
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

install-gum:
	@echo "🔍 Checking if gum is installed..."
	@if ! command -v gum >/dev/null 2>&1; then \
		echo "📥 Installing gum..."; \
		if [ "$(shell uname)" = "Darwin" ]; then \
			HOMEBREW_NO_AUTO_UPDATE=1 brew install gum|| { echo "❌ Failed to install gum via Homebrew"; exit 1; }; \
		elif command -v apt-get >/dev/null 2>&1; then \
			curl -fsSL -o /tmp/gum.deb \
			"https://github.com/charmbracelet/gum/releases/download/$(GUM_VERSION)/gum_$(GUM_VERSION:v%=%)_$(arch).deb"; \
			sudo apt-get update -qq; \
			sudo apt-get install -y /tmp/gum.deb || sudo dpkg -i /tmp/gum.deb; \
			rm -f /tmp/gum.deb; \
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
			--driver docker \
		|| { echo "❌ Failed to start Minikube (check if Docker is running)"; exit 1; }; \
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

# make shell is symlinked to metaflow-dev shell by metaflow
up: install-brew check-docker install-curl install-gum setup-minikube install-helm setup-tilt
	@echo "🚀 Starting up (may require sudo access)..."
	@mkdir -p $(DEVTOOLS_DIR)
	@echo '#!/bin/bash' > $(DEVTOOLS_DIR)/start.sh
	@echo 'set -e' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'trap "exit" INT TERM' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'trap "kill 0" EXIT' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'eval $$($(MINIKUBE) docker-env)' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'if [ -n "$$SERVICES_OVERRIDE" ]; then' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo '    echo "🌐 Using user-provided list of services: $$SERVICES_OVERRIDE"' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo '    SERVICES="$$SERVICES_OVERRIDE"' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo 'else' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo '    echo "📝 Selecting services..."' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo '    SERVICES=$$($(PICK_SERVICES))' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo 'fi' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo 'PATH="$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" $(MINIKUBE) tunnel &' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo -e "🚀 Starting Tilt with selected services..."' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'echo -e "\033[1;38;5;46m\n🔥 \033[1;38;5;196mNext Steps:\033[0;38;5;46m Use \033[3mmetaflow-dev shell\033[23m to switch to the development\n   environment'\''s shell and start executing your Metaflow flows.\n\033[0m"' >> "$(DEVTOOLS_DIR)/start.sh"
	@echo 'PATH="$(HELM_DIR):$(MINIKUBE_DIR):$(TILT_DIR):$$PATH" SERVICES="$$SERVICES" tilt up -f $(TILTFILE)' >> $(DEVTOOLS_DIR)/start.sh
	@echo 'wait' >> $(DEVTOOLS_DIR)/start.sh
	@chmod +x $(DEVTOOLS_DIR)/start.sh
	@$(DEVTOOLS_DIR)/start.sh

all-up:
	@echo "🚀 Starting up all services..."
	SERVICES_OVERRIDE=all $(MAKE_CMD) up

down:
	@echo "🛑 Stopping all services..."
	@-pkill -f "$(MINIKUBE) tunnel" 2>/dev/null || true
	@echo "⏹️  Stopping Tilt..."
	@echo "🧹 Cleaning up Minikube..."
	$(MAKE_CMD) teardown-minikube
	@echo "🗑️  Removing Tilt binary and directory..."
	-rm -rf $(TILT_DIR)
	@echo "🧹 Removing temporary scripts..."
	-rm -rf $(DEVTOOLS_DIR)
	@echo "✨ All done!"

shell: setup-tilt
	@echo "⏳ Checking if development environment is up..."
	@set -eu; \
	for i in $$(seq 1 90); do \
		if "$(TILT)" get session >/dev/null 2>&1; then \
			found_session=1; \
			break; \
		else \
			sleep 2; \
		fi; \
	done; \
	if [ -z "$${found_session:-}" ]; then \
		echo "❌ Development environment is not up."; \
		echo "   Please run 'metaflow-dev up' in another terminal, then re-run 'metaflow-dev shell'."; \
		exit 1; \
	fi
	@echo "⏳ Waiting for development environment to be ready..."
	@while true; do \
		"$(TILT)" get uiresource generate-configs >/dev/null 2>&1; \
		status=$$?; \
		if [ $$status -eq 0 ]; then \
			if ! "$(TILT)" wait --for=condition=Ready uiresource/generate-configs --timeout=300s; then \
				echo "❌ Timed out waiting for development environment to be ready."; \
				exit 1; \
			fi; \
			break; \
		elif [ $$status -eq 127 ]; then \
			echo "❌ Development environment is not up."; \
			echo "   Please run 'metaflow-dev up' in another terminal, then re-run 'metaflow-dev shell'."; \
			exit 1; \
		else \
			sleep 1; \
		fi; \
	done
	@echo "🔧 Starting a new shell for development environment..."
	@bash -c '\
		if [ -n "$$SHELL" ]; then \
			user_shell="$$SHELL"; \
		else \
			user_shell="$(SHELL)"; \
		fi; \
		echo "🔎 Using $$user_shell for interactive session."; \
		echo "🐍 If you installed Metaflow in a virtual environment, activate it now."; \
		if [ -f "$(DEVTOOLS_DIR)/aws_config" ]; then \
			env -u AWS_PROFILE \
				-u AWS_SHARED_CREDENTIALS_FILE \
				METAFLOW_HOME="$(DEVTOOLS_DIR)" \
				METAFLOW_PROFILE=local \
				AWS_CONFIG_FILE="$(DEVTOOLS_DIR)/aws_config" \
				"$$user_shell" -i; \
		else \
			env METAFLOW_HOME="$(DEVTOOLS_DIR)" \
				METAFLOW_PROFILE=local \
				"$$user_shell" -i; \
		fi'

wait-until-ready:
	@echo "Waiting for infrastructure to be ready. Timing out in $(WAIT_TIMEOUT) seconds..."
	@timeout $(WAIT_TIMEOUT) bash -c 'while [ ! -f $(DEVTOOLS_DIR)/start.sh ]; do sleep 10; done; echo "Infra is Ready"' || (echo "Waiting for infra timed out"&&exit 1)
# buffer to get the tilt api running
	@timeout 120 bash -c 'while ! $(TILT) get session; do sleep 3;done'
	@echo "Waiting for services to be ready. Timing out in $(WAIT_TIMEOUT) seconds..."
# Need to wait for Tiltfile first, as other resources return 404 otherwise
	@$(TILT) wait --for=condition=Ready "uiresource/(Tiltfile)" --timeout=$(WAIT_TIMEOUT)s
	@$(TILT) wait --for=condition=Ready uiresource/generate-configs --timeout=$(WAIT_TIMEOUT)s

# @echo '$(MAKE_CMD) create-dev-shell' >> $(DEVTOOLS_DIR)/start.sh
# @echo 'rm -f /tmp/metaflow-devshell-*' >> $(DEVTOOLS_DIR)/start.sh
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
		echo "if ! $(TILT) wait --for=condition=Ready uiresource/generate-configs --timeout=300s; then" >> $$SHELL_PATH && \
		echo "  echo \"❌ Timed out waiting for development environment to be ready.\"" >> $$SHELL_PATH && \
		echo "  exit 1" >> $$SHELL_PATH && \
		echo "fi" >> $$SHELL_PATH && \
		echo "" >> $$SHELL_PATH && \
		echo "echo \"🔧 Starting a new shell for development environment...\"" >> $$SHELL_PATH && \
		echo "if [ -n \"\$$SHELL\" ]; then" >> $$SHELL_PATH && \
		echo "    user_shell=\"\$$SHELL\"" >> $$SHELL_PATH && \
		echo "else" >> $$SHELL_PATH && \
		echo "    user_shell=\"$(SHELL)\"" >> $$SHELL_PATH && \
		echo "fi" >> $$SHELL_PATH && \
		echo "echo \"🔎 Using \$$user_shell for interactive session.\"" >> $$SHELL_PATH && \
		echo "echo \"🐍 If you installed Metaflow in a virtual environment, activate it now.\"" >> $$SHELL_PATH && \
		echo "if [ -f \"$(DEVTOOLS_DIR)/aws_config\" ]; then" >> $$SHELL_PATH && \
		echo "    env METAFLOW_HOME=\"$(DEVTOOLS_DIR)\" \\" >> $$SHELL_PATH && \
		echo "        METAFLOW_PROFILE=local \\" >> $$SHELL_PATH && \
		echo "        AWS_CONFIG_FILE=\"$(DEVTOOLS_DIR)/aws_config\" \\" >> $$SHELL_PATH && \
		echo "        AWS_SHARED_CREDENTIALS_FILE= \\" >> $$SHELL_PATH && \
		echo "        \"\$$user_shell\" -i" >> $$SHELL_PATH && \
		echo "else" >> $$SHELL_PATH && \
		echo "    env METAFLOW_HOME=\"$(DEVTOOLS_DIR)\" \\" >> $$SHELL_PATH && \
		echo "        METAFLOW_PROFILE=local \\" >> $$SHELL_PATH && \
		echo "        \"\$$user_shell\" -i" >> $$SHELL_PATH && \
		echo "fi" >> $$SHELL_PATH && \
		chmod +x $$SHELL_PATH && \
		echo "✨ Created $$SHELL_PATH" && \
		echo "🔑  Execute it from ANY directory to switch to development environment shell!" \
	'

ui: setup-tilt
	@echo "⏳ Checking if the development environment is up..."
	@if ! $(TILT) get session >/dev/null 2>&1; then \
		echo "❌ Development environment is not up."; \
		echo "   Please run 'metaflow-dev up' in another terminal, then re-run 'metaflow-dev ui'."; \
		exit 1; \
	fi
	@echo "⏳ Waiting for Metaflow UI to be ready..."
	@while true; do \
		"$(TILT)" get uiresource metaflow-ui >/dev/null 2>&1; \
		status=$$?; \
		if [ $$status -eq 0 ]; then \
			"$(TILT)" wait --for=condition=Ready uiresource/metaflow-ui; \
			break; \
		elif [ $$status -eq 127 ]; then \
			echo "❌ Development environment is not up."; \
			echo "   Please run 'metaflow-dev up' in another terminal, then re-run 'metaflow-dev shell'."; \
			exit 1; \
		else \
			sleep 1; \
		fi; \
	done
	@echo "🔗 Opening Metaflow UI at http://localhost:3000"
	@open http://localhost:3000

.PHONY: install-helm setup-minikube setup-tilt teardown-minikube tunnel up down check-docker install-curl install-gum install-brew up down dashboard shell ui all-up help

.DEFAULT_GOAL := help
