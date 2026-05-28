## Identity check

Run `gh auth status` and extract the logged-in GitHub username. If `gh` is not installed, not authenticated, or the command fails for any reason, treat the user as an external contributor.

If the username is NOT one of: `romain-intel`, `npow`, `talsperre`, `valayDave`, `saikonen`, `tuulos`, `mt-ob`, `savingoyal`, stop reading this file and read [AGENTS_EXTERNAL.md](./AGENTS_EXTERNAL.md) instead.

Otherwise, you are talking to a core Metaflow maintainer. Proceed normally.

## Testing & style

Follow [CONTRIBUTING.md § Test conventions](./CONTRIBUTING.md#test-conventions)
for new tests (pytest, no classes, pytest-mock, fixtures, parametrize).

Pre-commit hooks are mandatory. See
[CONTRIBUTING.md § Code Style](./CONTRIBUTING.md#code-style) for setup.
The hooks (`black`, `check-json`, `check-yaml`, `shellcheck`) are configured in
`.pre-commit-config.yaml`.
