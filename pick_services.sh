#!/usr/bin/env bash

set -e

COLOR="11"

# TODO: Improve logo
LOGO="
                                                                 
                           ░░                 ░░                 
  ░▒▓▒  ░▒▓▓ ▒▓▒▒▒▒▒▓█▓▓▓░▒▓▓▓  ▒▓▒▒▒▒▒▒▓░ ░▓▓▓▓▓▒░▓▒  ░▒▓░ ░▓▒░ 
  ░▓██░░▒▓██░▓▓░░░░ ▒█░ ░▒▓░██ ░▓▓░░░░ ▓▓░░▓▓░  ▓▓░▓▓ ░▓██░░▓▓░  
 ░▓▓▒█▒▓█▒█▓░▓▓▓▓▓▒░▓▓ ░▒▓▓▒▓█░░▓▓▓▓▓▒░▓▓░▓█░  ░▓▓░▓▓░▓▓▒█░▒█▒   
 ▒▓▒░▓██░░▓▒▒▓▒░░░░░▓▓ ▒▓▒▒░▒█▒▒▓░    ▒▓▒░▓█▒░░▒▓▒ ▓▓▓█░░▓▒█▓░   
 ▒▒░ ░▒░ ░▒░▒▒▒▒▒▒░░▒░░▒▒   ░▒▒▒▒░    ▒▒▒▒▒▒▒▒▒▒░  ▒▒▓░ ░▒▒▓░    
                                                                 
"

SERVICE_OPTIONS=(
    "minio"
    "metadata-service"
    "ui"
    "argo-workflows"
    "argo-events"
)

gum style "$LOGO" \
  --foreground "$COLOR" \
  --padding "0 2" \
  --margin "0 1" \
  --align center >&2

gum style "Select services to deploy:" --foreground $COLOR >&2
# gum style "Pick the services you want to run. If you don't pick any, all will be automatically selected.\n
# Metaflow’s main components:
# • 🗃️ datastore (e.g. Minio) to store artifacts
# ...
# " \
#   --foreground "$COLOR" --bold >&2

pretty_print() {
  local items=("$@")
  
  if [ "${#items[@]}" -eq 1 ]; then
    echo "${items[0]}"
    return
  fi

  if [ "${#items[@]}" -eq 2 ]; then
    echo "${items[0]} and ${items[1]}"
    return
  fi

  local last_item="${items[-1]}"
  unset 'items[-1]'  # remove last item from array
  echo "$(IFS=,; echo "${items[*]}"), and $last_item"
}

pretty_print() {
  local items=("$@")
  local length=${#items[@]}

  if [ "$length" -eq 0 ]; then
    echo "(none)"
    return
  fi

  if [ "$length" -eq 1 ]; then
    echo "${items[0]}"
    return
  fi

  if [ "$length" -eq 2 ]; then
    echo "${items[0]} and ${items[1]}"
    return
  fi

  local last_index=$((length - 1))
  local last_item="${items[$last_index]}"
  unset 'items[last_index]'

  local joined
  IFS=","
  joined="${items[*]}"
  unset IFS
  joined="${joined//,/, }"

  echo "$joined, and $last_item"
}

SELECTED_SERVICES="$(
  gum choose "${SERVICE_OPTIONS[@]}" \
    --no-limit \
    --cursor.foreground="$COLOR" \
    --selected.foreground="$COLOR"
)"

# If nothing was chosen, default to all
if [ -z "$SELECTED_SERVICES" ]; then
  gum style "🙅 No services selected. Deploying all..." --foreground "$COLOR" >&2
  SELECTED_SERVICES=("${SERVICE_OPTIONS[@]}")
else
  read -r -a SELECTED_SERVICES <<< "$SELECTED_SERVICES"
fi

PRINTABLE="$(pretty_print "${SELECTED_SERVICES[@]}")"
gum style "✅ Deploying $PRINTABLE" --foreground "$COLOR" >&2

echo "$(IFS=,; echo "${SELECTED_SERVICES[*]}")"