# Metaflow — Agent Instructions

> `CLAUDE.md` is a symlink to this file. Edit `AGENTS.md`; both Codex/Claude
> Code agents read the same instructions.

## Identity check

Run `gh auth status` and extract the logged-in GitHub username. If `gh` is not installed, not authenticated, or the command fails for any reason, treat the user as an external contributor.

If the username is NOT one of: `romain-intel`, `npow`, `talsperre`, `valayDave`, `saikonen`, `tuulos`, `mt-ob`, `savingoyal`, stop reading this file and read [AGENTS_EXTERNAL.md](./AGENTS_EXTERNAL.md) instead.

Otherwise, you are talking to a core Metaflow maintainer. Proceed normally.

## Tests

- **Writing a new test?** Follow
  [CONTRIBUTING.md § Test conventions](./CONTRIBUTING.md#test-conventions)
  for pytest conventions, test layout, fixtures, mocking, and parametrization.
- **Running tests?** Use
  [CONTRIBUTING.md § Running Tests Locally](./CONTRIBUTING.md#running-tests-locally)
  for the common commands, and [`test/README.md`](./test/README.md) for the
  detailed tox/env-specific guide.

## Style

Pre-commit hooks are mandatory. See
[CONTRIBUTING.md § Code Style](./CONTRIBUTING.md#code-style) for setup.
The hooks (`black`, `check-json`, `check-yaml`, `shellcheck`) are configured in
`.pre-commit-config.yaml`.
