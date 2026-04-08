# Netflix internal: vendor-token auth for `@huggingface`

This document is for **Netflix-internal** use. The open-source Metaflow repository does **not** ship the vendor-token-retrieval integration; it stays on branch `netflix-huggingface-vendor-token` (or your internal fork).

## What this adds

- `metaflow/plugins/huggingface/vendor_token_auth_provider.py` — `VendorTokenAuthProvider` (`TYPE = "vendor-token"`) that calls the internal `/hf-token` endpoint with Metatron mutual TLS.
- Registration in `metaflow/plugins/__init__.py` under `HF_AUTH_PROVIDERS_DESC`.
- Config `HUGGINGFACE_VENDOR_TOKEN_URL` in `metaflow_config.py` (default: internal vendor-token-retrieval URL).

## Dependencies

- `requests`
- `metatron` (MetatronAdapter)
- Run `metatron refresh` so credentials are valid before calling the service.

## Configuration

```bash
export METAFLOW_HUGGINGFACE_AUTH_PROVIDER=vendor-token
# Optional override:
# export METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL=https://.../hf-token
```

## Demo

With the internal branch checked out, restore the `service` mode in `run_huggingface_demo.py` / `run_huggingface_demo.sh` if removed, or set the env above and use flows that require private Hub access.

## Rebasing on OSS

After the OSS HuggingFace PR merges:

1. Fetch upstream `main`.
2. `git checkout netflix-huggingface-vendor-token`
3. `git rebase origin/main` (resolve conflicts in `__init__.py` / `metaflow_config.py` if any).
4. Re-run tests in an environment with Metatron + vendor-token service.

## Security

Do not commit real tokens. Internal service URLs are confidential.
