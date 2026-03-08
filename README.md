# Metaflow — Kubernetes + Argo CI Workflow

A proof-of-concept CI pipeline that automatically validates Metaflow flows
running on Kubernetes with Argo Workflows.

## What This Does

Every time a PR touches Kubernetes or Argo-related code, this workflow:

1. **Spins up a local Kubernetes cluster** using KIND (Kubernetes IN Docker)
2. **Deploys Argo Workflows** onto the cluster
3. **Runs 3 test flows** using `--with kubernetes`
4. **Reports pass/fail** with full logs back to the PR
5. **Tears down the cluster** cleanly after the run

## Test Matrix

| Python | LinearFlow | BranchFlow | ForeachFlow |
|--------|-----------|------------|-------------|
| 3.9    | ✅        | ✅         | ✅          |
| 3.10   | ✅        | ✅         | ✅          |
| 3.11   | ✅        | ✅         | ✅          |

## Test Flows

| Flow | Tests |
|------|-------|
| `linear_flow.py` | Basic step execution, artifact passing |
| `branch_flow.py` | Parallel branches, fan-out/fan-in |
| `foreach_flow.py` | Dynamic parallelism with `foreach` |

## Trigger Conditions

The workflow runs on:
- PRs that touch `metaflow/plugins/kubernetes/**`
- PRs that touch `metaflow/plugins/argo/**`
- Any push to `master`
- Manual trigger via `workflow_dispatch`

## Local Testing

```bash
# Install KIND
brew install kind   # macOS
# or
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.22.0/kind-linux-amd64 && chmod +x ./kind

# Create the cluster
kind create cluster --name metaflow-ci --config scripts/kind-config.yaml

# Run a test flow manually
cd test_flows
python linear_flow.py run --with kubernetes
```

## Architecture

```
GitHub PR
    │
    ▼
GitHub Actions Runner (ubuntu-latest)
    │
    ├─ Install: kubectl, KIND, Argo CLI
    │
    ├─ kind create cluster  ──► ephemeral K8s cluster
    │                              │
    ├─ Deploy Argo Workflows ──────►│
    │                              │
    ├─ Run LinearFlow  ────────────►│ (pod per step)
    ├─ Run BranchFlow ─────────────►│ (parallel pods)
    └─ Run ForeachFlow ────────────►│ (dynamic pods)
                                   │
    Upload logs as artifacts ◄──────┘
    Delete cluster
```