# HuggingFace Decorator — Read‑aloud update script

Use this as a script to read out in your meeting. The change is split into two parts; you can present Part 1 and Part 2 separately or together.

---

# Part 1 — Authentication Management

**Long version — narrative (about 1–1.5 minutes)**

> Today, users who need HuggingFace models in Metaflow often hardcode API tokens in their flow code or wire them in ad hoc. There’s no standard way to manage HuggingFace credentials, and that gets worse when we need different auth in different environments—for example our internal secrets system in production versus a simple env var locally.
>
> In the new decorator we’ve made authentication pluggable. By default we read the token from the environment: either HF_TOKEN or HUGGING_FACE_HUB_TOKEN. So for OSS or local use, you set one of those and you’re done. We’ve also defined an auth provider interface. Implementations can talk to whatever backend we want—for example our internal secrets service—and we can switch the active provider via configuration, so user code doesn’t change. The decorator calls the provider before any download; the token is never exposed in the step. So part one is: no more hardcoded tokens, a clear default for simple cases, and a pluggable path for org-specific credential management.

**Short version (about 15 seconds)**

> We made HuggingFace auth pluggable. Default is env vars—HF token or hugging face hub token. We have an auth provider interface so we can plug in our own credential source without changing user code; no tokens in the step.

**Bullets**
- Pluggable auth: env default (HF_TOKEN / HUGGING_FACE_HUB_TOKEN).
- Auth provider interface for org-specific credential backends.
- Config to select provider (HUGGINGFACE_AUTH_PROVIDER / METAFLOW_HUGGINGFACE_AUTH_PROVIDER); no tokens in user code.

---

# Part 2 — Model Download Performance & Caching

**Long version — narrative (about 1–1.5 minutes)**

> The second problem is that large model downloads can block a step for a long time, and when many users or many runs need the same model we end up re-downloading it over and over. There was no standard way to use a shared cache or to pass model paths between steps.
>
> We’ve addressed that with a pluggable cache and a single, consistent way to get model paths in the step. The decorator is at-huggingface. You declare what you need either as a list of model IDs or as a mapping—for example “llama” to “meta-llama slash Llama-2-7b at main”—so you can pin to a revision. Before the step runs, we ask the cache provider for each model: if it returns a local path we use it; if not we download from the Hub using the token from part one and then use that path. Either way, inside the step you get paths from current dot huggingface dot models: you index by the same key you used in the decorator and you get the local directory. So you use the usual HuggingFace APIs—from-pretrained with that path—and it doesn’t matter whether the path came from a shared cache or a fresh download. Same code in local dev and production. We’re not doing cache population or performance targets in this phase; the interface is there so we can add FSx, JFrog, S3, or another backend later.

**Short version (about 15 seconds)**

> We have a step decorator at-huggingface that declares which models you need. A cache provider can return a path or we download. In the step you get paths from current dot huggingface dot models and use normal from-pretrained. Same code locally and in production; we can add FSx, JFrog, or S3 later.

**Bullets**
- at-huggingface step decorator: declare models (list or alias → repo@revision mapping).
- Pluggable cache provider: return path or None; we fall back to huggingface_hub download.
- current.huggingface.models[key] returns local path; use with standard HuggingFace APIs.
- Same code in local dev and production; cache/provider can be added later (FSx, JFrog, S3).

---

# Combined (if presenting both parts in one go)

**Long version — narrative (about 2–3 minutes)**

> I’d like to give an update on the HuggingFace decorator for Metaflow. I’ll break it into two parts: authentication management, and model download performance and caching.
>
> **Part 1 — Authentication management.** Today, users who need HuggingFace models in Metaflow often hardcode API tokens in their flow code or wire them in ad hoc. There’s no standard way to manage HuggingFace credentials, and that gets worse when we need different auth in different environments—for example our internal secrets system in production versus a simple env var locally. In the new decorator we’ve made authentication pluggable. By default we read the token from the environment: either HF_TOKEN or HUGGING_FACE_HUB_TOKEN. So for OSS or local use, you set one of those and you’re done. We’ve also defined an auth provider interface. Implementations can talk to whatever backend we want—for example our internal secrets service—and we can switch the active provider via configuration, so user code doesn’t change. The decorator calls the provider before any download; the token is never exposed in the step.
>
> **Part 2 — Model download performance and caching.** The second problem is that large model downloads can block a step for a long time, and when many users or many runs need the same model we end up re-downloading it over and over. We’ve addressed that with a pluggable cache and a single, consistent way to get model paths in the step. The decorator is at-huggingface. You declare what you need either as a list of model IDs or as a mapping. Before the step runs, we ask the cache provider for each model: if it returns a local path we use it; if not we download from the Hub using the token from part one. Inside the step you get paths from current dot huggingface dot models. Same code in local dev and production. We’re not doing cache population or performance targets in this phase; the interface is there so we can add FSx, JFrog, S3, or another backend later.
>
> **Wrapping up.** The decorator is registered as a built-in step decorator and we’ve added it to Spin’s allowed list. We have unit tests for parsing and for the current-dot-huggingface behavior, plus an integration test that runs a flow with the decorator; we’ve also added an example flow and a run script for demos. We’re in a good place to try it on a real flow or to add an org-specific auth or cache provider.

**Short version (about 30 seconds)**

> Update on the HuggingFace decorator in two parts. One: authentication management. We made auth pluggable—default is env vars, HF token or hugging face hub token—and we have a provider interface so we can plug in our own credential source without changing user code. Two: model download and caching. We have a step decorator at-huggingface that declares which models you need; a cache provider can return a path or we download. In the step you get paths from current dot huggingface dot models and use normal from-pretrained. It’s wired in with tests and an example flow; next we can try it on a real flow or add an org-specific auth or cache provider.

**General bullets (both parts)**
- Decorator registered; Spin allowed; tests and example flow in repo.
