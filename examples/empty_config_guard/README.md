# Empty Configuration Guard – Examples

These examples demonstrate the behavior of the **empty configuration guard**
added to `metaflow configure` (specifically the `persist_env` function).

Before this change, calling `persist_env({}, profile)` would silently overwrite
`config.json` with an empty dictionary, wiping out all prior settings. Now,
Metaflow warns the user and asks for confirmation before proceeding.

## Scenarios

| # | Script | What it shows |
|---|--------|---------------|
| 1 | `01_interactive_empty_config_confirm.py` | Interactive TTY: user sees a warning and is prompted for confirmation |
| 2 | `02_non_interactive_empty_config_abort.py` | Non-interactive (piped / CI): automatic abort with an error message |
| 3 | `03_nonempty_config_no_prompt.py` | Non-empty config is written directly – no extra prompt |

## How to run

```bash
# Scenario 1 – Interactive prompt (run in a real terminal)
python examples/empty_config_guard/01_interactive_empty_config_confirm.py

# Scenario 2 – Non-interactive abort (simulated via pipe)
echo "" | python examples/empty_config_guard/02_non_interactive_empty_config_abort.py

# Scenario 3 – Normal (non-empty) configuration
python examples/empty_config_guard/03_nonempty_config_no_prompt.py
```

## Expected output

### Scenario 1 (interactive)
```
Warning: The configuration to be saved is empty. Writing an empty
configuration will overwrite any existing settings in "/path/to/config.json".
Do you still want to save the empty configuration? [y/N]:
```
If the user types **N** (or presses Enter):
```
Operation aborted. Configuration was not modified.
```

### Scenario 2 (non-interactive)
```
Warning: The configuration to be saved is empty. ...
Aborting: refusing to write an empty configuration in a non-interactive
environment. Use an explicit configuration or the 'reset' command instead.
```

### Scenario 3 (non-empty config)
```
Configuration successfully written to "/tmp/metaflow_example/config.json"
```
