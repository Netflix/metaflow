# Contributing to Metaflow

First off, thanks for taking the time to contribute! We'd love to hear from you! Drop us a line in our [chatroom](http://chat.metaflow.org)!

## Table of Contents

- [Quick Start](#quick-start)
- [PR Requirements (READ THIS FIRST)](#pr-requirements-read-this-first)
- [Core Runtime Contributions (Higher Bar)](#core-runtime-contributions-higher-bar)
- [AI Tool Usage Policy](#ai-tool-usage-policy)
- [Testing Requirements](#testing-requirements)
- [PR Description Template](#pr-description-template)
- [Code Style](#code-style)
- [Running Tests Locally](#running-tests-locally)
- [Commit Guidelines](#commit-guidelines)
- [Development Environment Setup](#development-environment-setup)
- [Finding Issues to Work On](#finding-issues-to-work-on)
- [Types of Contributions](#types-of-contributions)
- [How to Contribute](#how-to-contribute)
- [Pull Request Review Process](#pull-request-review-process)
- [Community](#community)

## Quick Start

Get up and running in under 2 minutes:

```bash
# 1. Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/metaflow.git
cd metaflow

# 2. Install in editable mode
pip install -e .

# 3. Set up pre-commit hooks (formats code automatically)
pip install pre-commit
pre-commit install

# 4. Make your changes and add tests!

# 5. Run tests
cd test/unit
python -m pytest -v
```

**That's it!** Now read the requirements below before submitting your PR.

## PR Requirements (READ THIS FIRST)

Before you submit a pull request, make sure you understand these **non-negotiable requirements**:

### 1. Tests Are Mandatory for Bug Fixes ⚠️

If you're fixing a bug, you **MUST** include a test that:
- ✅ Reproduces the bug (fails without your fix)
- ✅ Passes with your fix applied
- ✅ Prevents regression in the future

**No test = PR will not be merged.** This is our most important requirement.

### 2. Tests Are Expected for New Features

New functionality should include appropriate test coverage. If you're unsure what tests to write, ask in the PR!

### 3. Code Must Follow Project Style

Run `pre-commit install` to automatically format your code. See [Code Style](#code-style) for details.

### 4. PR Description Must Be Comprehensive

Use the [PR Description Template](#pr-description-template) below. PRs with vague descriptions like "Fixed bug" will be sent back for revision.

### 5. Keep PRs Focused

- One PR = One logical change
- Split large changes into multiple PRs
- Don't mix unrelated changes

### Before You Start

- **Check existing issues** - Is someone already working on this?
- **Discuss major changes first** - Open an issue or chat with us for big features
- **Comment on the issue** - Let others know you're working on it

## Core Runtime Contributions (Higher Bar)

Changes touching any of the following files or directories are **Core Runtime** and have a higher acceptance bar:

| Area | Paths |
|------|-------|
| **Execution engine** | `metaflow/runtime.py`, `metaflow/task.py`, `metaflow/flowspec.py` |
| **Subprocess management** | `metaflow/runner/metaflow_runner.py`, `metaflow/runner/subprocess_manager.py`, `metaflow/runner/deployer_impl.py` |
| **CLI plumbing** | `metaflow/cli.py`, `metaflow/cli_components/`, `metaflow/runner/click_api.py` |
| **Datastore** | `metaflow/datastore/`, `metaflow/plugins/datastores/` |
| **Metadata** | `metaflow/metadata_provider/`, `metaflow/plugins/metadata_providers/` |
| **AWS client/credentials** | `metaflow/plugins/aws/aws_client.py`, `metaflow/plugins/datatools/s3/` |
| **Config/parameters** | `metaflow/metaflow_config.py`, `metaflow/parameters.py`, `metaflow/user_configs/` |
| **Logging/capture** | `metaflow/mflog/`, `metaflow/system/`, `metaflow/debug.py` |
| **Decorators (core)** | `metaflow/decorators.py` |
| **Graph/DAG** | `metaflow/graph.py` |
| **Orchestrator plugins** | `metaflow/plugins/argo/`, `metaflow/plugins/aws/batch/`, `metaflow/plugins/aws/step_functions/`, `metaflow/plugins/kubernetes/` |

If you're unsure whether your change is Core Runtime, it probably is. When in doubt, open an issue first.

### Why the higher bar?

Metaflow executes user code in subprocesses and worker processes across local, Kubernetes, Batch, and Argo runtimes. Bugs in these areas are subtle: something that "works" when tested naively (e.g., printing to stderr in the parent process) may completely fail in production where output must propagate across subprocess boundaries. We have seen multiple PRs where the test validates something different from what the fix claims to address.

**Maintainer bandwidth is limited.** We cannot provide step-by-step debugging or mentorship for Core Runtime PRs. We review contributions that are already reproducible, minimal, and defended with a correct model of Metaflow's execution semantics.

### Before you open a Core Runtime PR

**Required:**

1. **Open or link an issue** describing the user-visible problem and expected behavior.
2. **Provide a minimal reproduction** that demonstrates the failure in the real execution mode that matters (e.g., local runtime vs. Kubernetes/Batch/Argo, subprocess boundaries, worker logs).
3. **Write a short technical rationale:**
   - Root cause: what invariant was violated?
   - Why this fix is correct
   - What failure modes were considered (at least two)

PRs that don't meet these requirements may be closed without further review.

### What "tested" means for Core Runtime

"Manual testing" only counts if you specify:

- The exact command(s) run
- The runtime used (`--with kubernetes`, `--with batch`, local, etc.)
- Where the evidence shows up (parent console, task logs, UI logs, metadata)

Because Metaflow uses subprocesses and worker processes, printing to stderr inside a worker **does not necessarily appear where you expect** unless you explicitly propagate it. Tests must validate behavior across that boundary.

**Examples of good Core Runtime PRs:**
- [PR #2796](https://github.com/Netflix/metaflow/pull/2796) -- race condition in local storage: identifies the exact interleaving that causes `json.load()` to fail on partial writes, fix is a single atomic write helper, links to CI failure as evidence.
- [PR #2751](https://github.com/Netflix/metaflow/pull/2751) -- symlink traversal edge case: concrete directory structure that reproduces the bug, explains the global-vs-per-branch invariant that was violated, minimal fix.
- [PR #2714](https://github.com/Netflix/metaflow/pull/2714) -- Argo input-paths with nested conditionals: links to issue, identifies the template generation bug, scoped fix.

### Feature PRs touching Core Runtime

We only accept Core Runtime feature changes after issue discussion and maintainer alignment. Open an issue describing the problem and proposed approach first. We can then evaluate whether this belongs in core vs. an extension or plugin.

## AI Tool Usage Policy

We welcome contributions that use AI tools responsibly. However, the contributor is fully accountable for every line of code they submit.

**Requirements:**

1. **Disclose AI use** -- Check the AI disclosure box in the PR template if you used AI tools (LLMs, code generators, copilots, etc.) for any part of your contribution.
2. **Understand your code** -- You must be able to answer technical questions about your changes without referring back to an AI tool. If you cannot explain why your fix is correct or what failure modes you considered, the PR will be closed.
3. **No AI-only submissions** -- PRs must represent human judgment and understanding. Using AI to help write code is fine; submitting AI output you haven't critically reviewed is not.
4. **Test what matters** -- AI tools often generate tests that look plausible but validate the wrong thing. Ensure your tests exercise the actual failure mode, not a superficial approximation of it.

Undisclosed AI use discovered during review, or inability to explain your changes when asked, will result in PR closure. Repeated violations may result in future PRs being declined.

This policy follows the approach taken by [CPython](https://devguide.python.org/getting-started/generative-ai/), [LLVM](https://llvm.org/docs/DeveloperPolicy.html), and [scikit-learn](https://scikit-learn.org/stable/developers/contributing.html).

## Testing Requirements

**Testing is not optional.** Here's exactly what you need to know:

### When to Write Tests

| Type of Change | Testing Requirement |
|----------------|---------------------|
| **Bug fix** | **MANDATORY** - Test that reproduces the bug |
| **New feature** | **EXPECTED** - Tests covering the functionality |
| **Refactoring** | **REQUIRED** - Existing tests must pass |
| **Documentation** | Not required (unless code examples) |

### Types of Tests

Metaflow has three types of tests:

1. **Unit tests** (`test/unit/`) - Fast, isolated tests for individual components
2. **Integration tests** (`test/core/`) - Full Metaflow stack tests
3. **Data tests** (`test/data/`) - Data layer components (S3, etc.)

**For most bug fixes and features, add unit tests.**

### Writing Good Tests

✅ **A good test:**
- Has a clear name describing what it tests
- Tests one thing well
- Is reliable (not flaky)
- Runs quickly (for unit tests)
- Includes comments for complex logic

**Example:**
```python
def test_symlink_traversal_handles_circular_references():
    """Test that symlink detection works correctly when modules are
    encountered through different paths."""
    # Test case for issue #2751
    # Setup: Create circular symlink structure
    # ... test implementation
    # Assert: Verify all modules are included correctly
```

## PR Description Template

A good PR description helps reviewers and speeds up the merge process. **Use this template:**

```markdown
## Summary
Brief (1-2 sentence) description of what this PR does.

## Context / Motivation
Why is this change needed? What problem does it solve? Link to issue: Fixes #123

## Changes Made
- Bullet point list of specific changes
- Include both code changes and behavior changes
- Mention any breaking changes or deprecations

## Testing
How you tested these changes:
- Added test_feature_name() that verifies X
- Manually tested by running: python flow.py run
- Tested edge cases: empty input, large files, etc.

## Trade-offs / Design Decisions (optional)
- Why you chose this approach over alternatives
- Known limitations
- Performance implications
```

**Examples of excellent PR descriptions:**
- [PR #2796](https://github.com/Netflix/metaflow/pull/2796): Fix race condition in local storage with atomic writes
- [PR #2751](https://github.com/Netflix/metaflow/pull/2751): Fix symlink traversal edge case in packaging

**Common mistakes to avoid:**
- ❌ Empty or one-line descriptions
- ❌ No explanation of WHY the change is needed
- ❌ No testing information
- ❌ Missing issue link

## Code Style

We use automated formatting - you don't need to worry about this much!

### Python Code Formatting

We use [black](https://black.readthedocs.io/en/stable/) as our code formatter.

**Setup (do this once):**
```bash
pip install pre-commit
pre-commit install
```

This automatically formats your code when you commit. Done!

**Manual formatting (if needed):**
```bash
black .
```

### Documentation Style

We use [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html) style for docstrings:

```python
def example_function(param1, param2):
    """
    Brief description of the function.

    Parameters
    ----------
    param1 : str
        Description of param1
    param2 : int
        Description of param2

    Returns
    -------
    bool
        Description of return value

    Examples
    --------
    >>> example_function("test", 42)
    True
    """
```

### Code Quality Quick Tips

- Keep it simple - avoid over-engineering
- Remove commented-out code
- Use descriptive variable names
- Provide helpful error messages
- Use type hints where they add clarity

## Running Tests Locally

Before submitting your PR, run the relevant tests:

### Unit Tests (Most Common)

```bash
cd test/unit
python -m pytest -v
```

**Run specific test:**
```bash
python -m pytest test/unit/test_your_feature.py -v
```

### Integration Tests

```bash
cd test/core
PYTHONPATH=`pwd`/../../ python run_tests.py --debug --contexts dev-local
```

**Run specific test:**
```bash
PYTHONPATH=`pwd`/../../ python run_tests.py --debug --contexts dev-local --tests YourTestName
```

### Data/S3 Tests

```bash
cd test/data/
PYTHONPATH=`pwd`/../../ python3 -m pytest -x -s -v --benchmark-skip
```

See [test/README.md](test/README.md) for detailed testing documentation.

## Commit Guidelines

### Good Commit Messages

```
Fix symlink traversal edge case in packaging

The symlink detection was happening globally across branches, causing
some modules to be skipped when encountered through different paths.
Now tracks symlinks per-branch to handle this correctly.

Fixes #2751
```

### Commit Structure

- Use imperative mood: "Fix bug" not "Fixed bug"
- First line: summary (50-72 characters)
- Blank line, then detailed explanation
- Reference issues: `Fixes #123` or `Relates to #456`

### Multiple Commits

Multiple commits in a PR are fine! Each commit should:
- Be logical and complete
- Pass tests on its own (if possible)
- Have a clear message

We may squash commits on merge for cleaner history.

## Development Environment Setup

### Basic Setup (Sufficient for Most Contributors)

You've already done this if you followed [Quick Start](#quick-start)!

```bash
git clone https://github.com/YOUR_USERNAME/metaflow.git
cd metaflow
pip install -e .
pip install pre-commit && pre-commit install
```

### Full Local Environment (For Cloud Feature Development)

If you're working on features that interact with **S3, Kubernetes, or cloud services**, you can run a full local stack using MinIO (S3-compatible) and Minikube (local Kubernetes).

**Prerequisites:**
- Docker (must be running)
- At least 4 CPU cores and 6GB RAM available

**Setup:**
```bash
cd devtools
make up
```

This installs and configures:
- **MinIO** - S3-compatible object storage at `http://localhost:9000`
- **PostgreSQL** - For metadata service
- **Minikube** - Local Kubernetes cluster
- **Tilt** - Resource orchestration
- Optional: Argo Workflows

You'll be prompted to select services. For S3 testing, select at minimum:
- `minio` (S3-compatible storage)

**Using the development environment:**

```bash
# Start the environment (from devtools/)
make up

# In a new terminal, enter the dev shell
metaflow-dev shell

# Your flows now use local MinIO instead of AWS S3
# Access MinIO console: http://localhost:9001
# Username: rootuser, Password: rootpass123
```

**Testing with Local S3 (MinIO):**

When running, MinIO is configured with:
- **Endpoint**: `http://localhost:9000`
- **Access Key**: `rootuser`
- **Secret Key**: `rootpass123`
- **Bucket**: `metaflow-test`

Test S3-dependent changes:
```bash
# In the dev shell
cd test/data/s3
METAFLOW_S3_TEST_ROOT=s3://metaflow-test/test python -m pytest -v
```

**Stop the environment:**
```bash
cd devtools
make down
```

See [devtools/](devtools/) for advanced configuration.

## Finding Issues to Work On

### For First-Time Contributors

Look for [`good first issue`](https://github.com/Netflix/metaflow/labels/good%20first%20issue) label. These issues:
- Don't require deep codebase knowledge
- Have clear acceptance criteria
- Include guidance on where to start
- Are scoped to be completable in reasonable time

### For Experienced Contributors

Check [`help wanted`](https://github.com/Netflix/metaflow/labels/help%20wanted) label. These are:
- Ready to work on (design agreed upon)
- Important but not on critical path
- May require more system knowledge

### Before Starting Work

1. **Comment on the issue** - Let others know you're working on it
2. **Ask questions** - Clarify anything unclear upfront
3. **Check recent activity** - Ensure issue is still relevant
4. **Start small** - Especially for your first contribution

### Working on Something New?

If you want to work on something not in the issue tracker:
1. Search existing issues to avoid duplicates
2. Open an issue first to discuss your approach
3. Wait for feedback before investing significant time

## Types of Contributions

We welcome many types of contributions beyond code!

### Code Contributions
- **Bug fixes** - Fix issues you've encountered
- **New features** - Add new functionality
- **Performance improvements** - Optimize code
- **Refactoring** - Improve code structure

### Non-Code Contributions
- **Documentation** - Improve docs, fix typos, add examples, write tutorials
- **Issue triaging** - Help categorize and investigate issues
- **Code review** - Review PRs from other contributors
- **Community support** - Answer questions in [chatroom](http://chat.metaflow.org)
- **Testing** - Report bugs, test PRs, improve coverage
- **Evangelism** - Blog posts, talks, share experiences

**All contributions are valuable!** Documentation improvements and bug reports are just as important as features.

## How to Contribute

### Reporting Bugs

When filing a bug report, include:

**Required Information:**
- **Clear title** - Summarize in one line
- **Steps to reproduce** - Numbered list of exact steps
- **Expected vs actual behavior** - What should happen vs what happened
- **Environment details**:
  - OS (e.g., macOS 14.0, Ubuntu 22.04)
  - Python version: `python --version`
  - Metaflow version: `python -c "import metaflow; print(metaflow.__version__)"`
  - Relevant integrations (AWS Batch, Kubernetes, etc.)
- **Logs/error messages** - Full stack traces
- **Minimal reproduction** - Simplest code that shows the issue

**Use issue templates** when available.

### Proposing Features

For feature requests:
- **Check existing issues** - Avoid duplicates
- **Describe the problem** - What use case are you solving?
- **Explain your solution** - What would you like to see?
- **Consider alternatives** - What other approaches work?
- **Discuss major changes first** - Use [chatroom](http://chat.metaflow.org) or open a discussion

## Pull Request Review Process

### What to Expect

After submitting a PR:

1. **Automated checks run** (tests, formatting)
   - Must pass before review
   - Fix failures by pushing new commits

2. **Initial triage** (few days)
   - Maintainer reviews and may assign reviewers
   - You may be asked questions

3. **Code review** begins
   - Reviewers provide feedback
   - **Expect 2-4 business days** for initial review

4. **Iteration** - Address feedback by:
   - Pushing new commits
   - Responding to comments
   - Updating tests/docs

5. **Approval and merge**
   - Maintainer merges once approved
   - May squash commits for clean history

### Review Timeline

- **Simple fixes** (typos, small bugs): 2-3 days
- **Medium changes** (features, refactors): 3-7 days
- **Large changes** (major features): 1-2 weeks

**PR stalled?** After a week, feel free to:
- Politely ping with a comment
- Ask in [chatroom](http://chat.metaflow.org)

### You Can Help Review PRs!

**Anyone can review** - you don't need to be a maintainer!

**Focus on:**
- Does the code make sense?
- Are there tests?
- Is the PR description clear?
- Edge cases to consider?
- Follows code style?

**Be constructive:**
- Respectful and assume good intent
- Ask questions, don't demand
- Suggest alternatives when pointing out issues
- Acknowledge good work

**Benefits:**
- Learn codebase faster
- Build community reputation
- Speed up merge process
- Improve your review skills

### If Your PR Isn't Merged

Not all PRs get merged. Common reasons:
- Doesn't align with project goals
- Different approach was chosen
- PR became stale/outdated
- Breaking changes without sufficient benefit

**If closed:**
- Don't be discouraged - happens to everyone!
- Ask for feedback on why
- Consider different approach
- Your effort still contributed to discussion

## Community

Everyone is welcome in our [chatroom](http://chat.metaflow.org)!

Please maintain appropriate, professional conduct in all communication channels. We take reports of harassment or unwelcoming behavior very seriously. Report issues to [help@metaflow.org](mailto:help@metaflow.org).

## Questions?

- **Usage questions** - [Chatroom](http://chat.metaflow.org)
- **Bug reports** - [File an issue](https://github.com/Netflix/metaflow/issues)
- **Feature discussions** - [Discussions](https://github.com/Netflix/metaflow/discussions) or chatroom
- **Documentation** - [docs.metaflow.org](https://docs.metaflow.org)
- **Contributing questions** - [Chatroom](http://chat.metaflow.org) - we're happy to help!

## Additional Resources

- [Metaflow Documentation](https://docs.metaflow.org) - Learn how to use Metaflow
- [Contributing Guide (extended)](https://docs.metaflow.org/introduction/contributing-to-metaflow)
- [Test Documentation](test/README.md) - Detailed testing guide
- [Security Policy](SECURITY.md) - Security and conduct guidelines
- [Slack/Chat](http://chat.metaflow.org) - Real-time community support

## Recognition

We value all contributions! Contributors are:
- Listed in the commit history
- Mentioned in release notes for significant contributions
- Welcomed into our community of practitioners

Your contributions make Metaflow better for everyone. Thank you! 🙏

---

**Thank you for contributing to Metaflow!** 🚀
