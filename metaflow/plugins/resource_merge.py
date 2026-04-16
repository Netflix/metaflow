from metaflow.exception import MetaflowException


def merge_resource_attrs(base, deco_attrs, compute_deco_attrs, supported_keys):
    """Merge @resources attrs into base, taking max for numeric values."""
    result = dict(base)
    for k, v in deco_attrs.items():
        if k not in supported_keys or v is None:
            continue
        existing = compute_deco_attrs.get(k)
        if existing is not None and v is not None:
            if k == "mounts":
                merged = dict(existing) if existing else {}
                for mk, mv in v.items():
                    if mk in merged:
                        raise MetaflowException("Duplicate mount key: %s" % mk)
                    merged[mk] = mv
                result[k] = merged
                continue
            try:
                result[k] = str(max(float(existing or 0), float(v or 0)))
            except ValueError:
                if existing != v:
                    raise MetaflowException(
                        "Conflicting values for '%s'" % k
                    )
        elif existing is not None:
            result[k] = str(existing or "0")
        else:
            result[k] = str(v or "0")
    return result


def group_resources_by_node_type(decos):
    """Group @resources decorators by node_type, raising on duplicates."""
    resources_by_node_type = {}
    for deco in decos:
        if deco.name != "resources":
            continue
        node_type = deco.attributes.get("node_type") or "default"
        if node_type in resources_by_node_type:
            raise MetaflowException(
                "Multiple @resources decorators with the same node_type '%s' "
                "on the same step. Please combine them into a single decorator."
                % node_type
            )
        resources_by_node_type[node_type] = deco
    return resources_by_node_type


def _compute_defaults_only(base_defaults, compute_deco, resource_defaults):
    """Compute resources when no @resources decorator is present."""
    result = dict(base_defaults)
    for k in resource_defaults:
        if compute_deco.attributes.get(k) is not None:
            result[k] = str(compute_deco.attributes[k] or "0")
    return {"default": result}


def compute_resource_attributes(decos, compute_deco, resource_defaults):
    """
    Compute resource values taking into account defaults, the values specified
    in the compute decorator (like @titus) directly, and resources specified
    via @resources decorator.

    Returns a Dict[str, Dict[str, str]] keyed by node_type. When there is no
    node_type on the @resources decorator, the key is "default".

    Each inner dict contains resource attr -> value (str) plus "num_replicas" if set.
    """
    assert compute_deco is not None
    supported_keys = resource_defaults.keys() | compute_deco.attributes.keys()
    base_defaults = {k: v for k, v in resource_defaults.items() if v is not None}

    resources_by_node_type = group_resources_by_node_type(decos)

    if not resources_by_node_type:
        return _compute_defaults_only(base_defaults, compute_deco, resource_defaults)

    compute_has_resources = any(
        compute_deco.attributes.get(k) is not None for k in resource_defaults
    )
    if compute_has_resources and len(resources_by_node_type) > 1:
        raise MetaflowException(
            "Cannot specify resource values on @titus when using multiple "
            "@resources decorators with different node_type values. "
            "Please specify all resources via @resources decorators."
        )

    result = {}
    for node_type, deco in resources_by_node_type.items():
        merged = merge_resource_attrs(
            base_defaults, deco.attributes, compute_deco.attributes, supported_keys
        )
        num_replicas = deco.attributes.get("num_replicas")
        if num_replicas is not None:
            merged["num_replicas"] = str(num_replicas)
        result[node_type] = merged

    return result


def select_default_resources(resources_by_node_type):
    """Select the resource dict to apply for compatibility with compute decorators.

    Uses the "default" node type if present, otherwise the first non-"head" type.
    """
    if "default" in resources_by_node_type:
        return resources_by_node_type["default"]
    non_head = [nt for nt in resources_by_node_type if nt != "head"]
    key = non_head[0] if non_head else next(iter(resources_by_node_type))
    return resources_by_node_type[key]
