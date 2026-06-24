"""Tests for parsing, validating, and resolving SecretSpecs and environment variables."""

import os
import uuid
import pytest

import metaflow.metaflow_config
from metaflow.exception import MetaflowException
from metaflow.plugins.secrets.secrets_decorator import (
    SecretSpec,
    get_secrets_backend_provider,
    validate_env_vars,
    validate_env_vars_across_secrets,
    validate_env_vars_vs_existing_env,
)


@pytest.fixture
def default_secrets_backend(mocker):
    """Fixture to establish a consistent default backend type for tests."""
    mocker.patch(
        "metaflow.metaflow_config.DEFAULT_SECRETS_BACKEND_TYPE",
        "some-default-backend-type",
    )


def test_missing_default_secrets_backend_type(mocker):
    """Test that missing a default backend type raises an exception when parsing implicit strings."""
    mocker.patch("metaflow.metaflow_config.DEFAULT_SECRETS_BACKEND_TYPE", None)
    assert metaflow.metaflow_config.DEFAULT_SECRETS_BACKEND_TYPE is None

    with pytest.raises(MetaflowException):
        SecretSpec.secret_spec_from_str("secret_id", None)


def test_secret_spec_from_str_explicit_type(default_secrets_backend):
    """Test parsing a dot-separated string into a specific backend type and id."""
    spec = SecretSpec.secret_spec_from_str("explicit-type.the_id", None)
    expected = {
        "options": {},
        "secret_id": "the_id",
        "secrets_backend_type": "explicit-type",
        "role": None,
    }
    assert spec.to_json() == expected


def test_secret_spec_from_str_implicit_type(default_secrets_backend):
    """Test parsing a raw id string falls back to the default backend type."""
    spec = SecretSpec.secret_spec_from_str("the_id", None)
    expected = {
        "options": {},
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": None,
    }
    assert spec.to_json() == expected


def test_secret_spec_from_dict_explicit_type_no_options(default_secrets_backend):
    """Test parsing a dictionary specifying a backend type."""
    spec = SecretSpec.secret_spec_from_dict(
        {"type": "explicit-type", "id": "the_id"}, None
    )
    expected = {
        "options": {},
        "secret_id": "the_id",
        "secrets_backend_type": "explicit-type",
        "role": None,
    }
    assert spec.to_json() == expected


def test_secret_spec_from_dict_implicit_type_with_options(default_secrets_backend):
    """Test parsing a dictionary inherits default type and retains options."""
    spec = SecretSpec.secret_spec_from_dict(
        {"id": "the_id", "options": {"a": "b"}}, None
    )
    expected = {
        "options": {"a": "b"},
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": None,
    }
    assert spec.to_json() == expected


def test_role_resolution_source_level_wins(default_secrets_backend):
    """Test that a role specified in the source dictionary overrides the decorator-level role."""
    spec = SecretSpec.secret_spec_from_dict(
        {"id": "the_id", "role": "source-level-role"},
        "decorator-level-role",
    )
    expected = {
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": "source-level-role",
        "options": {},
    }
    assert spec.to_json() == expected


def test_role_resolution_falls_back_to_decorator_level(default_secrets_backend):
    """Test that omitting the role in the source dict falls back to the decorator-level role."""
    spec = SecretSpec.secret_spec_from_dict(
        {"id": "the_id"},
        role="decorator-level-role",
    )
    expected = {
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": "decorator-level-role",
        "options": {},
    }
    assert spec.to_json() == expected


@pytest.mark.parametrize(
    "spec_dict",
    [
        {"type": 42, "id": "the_id"},
        {"id": 42},
        {"id": "the_id", "options": []},
        {"id": "the_id", "role": 42},
    ],
    ids=["bad_type", "bad_id", "bad_options", "bad_role"],
)
def test_secret_spec_from_dict_rejects_invalid(spec_dict, default_secrets_backend):
    """Test that malformed dictionaries raise exceptions during parsing."""
    with pytest.raises(MetaflowException):
        SecretSpec.secret_spec_from_dict(spec_dict, None)


def test_secrets_provider_resolution_unknown_backend():
    """Test that resolving an unregistered backend type raises an exception."""
    unique_backend_name = f"non_existent_{uuid.uuid4().hex}"

    with pytest.raises(MetaflowException):
        get_secrets_backend_provider(unique_backend_name)


def test_validate_env_vars_across_secrets_rejects_overlap():
    """Test that multiple secret specs returning the same env var key raise a collision exception."""
    all_secrets_env_vars = [
        (SecretSpec.secret_spec_from_str("t.1", None), {"A": "a", "B": "b"}),
        (SecretSpec.secret_spec_from_str("t.2", None), {"B": "b", "C": "c"}),
    ]
    with pytest.raises(MetaflowException):
        validate_env_vars_across_secrets(all_secrets_env_vars)


def test_validate_env_vars_vs_existing_env_rejects_collision(monkeypatch):
    """Test that env vars provided by a secret cannot overwrite existing OS environment variables."""
    # Setup a deterministic existing environment variable
    monkeypatch.setenv("EXISTING_MOCK_ENV_VAR", "some_value")

    all_secrets_env_vars = [
        (
            SecretSpec.secret_spec_from_str("t.1", None),
            {"A": "a", "EXISTING_MOCK_ENV_VAR": "secret_value"},
        ),
    ]
    with pytest.raises(MetaflowException):
        validate_env_vars_vs_existing_env(all_secrets_env_vars)


def test_validate_env_vars_accepts_typical_keys():
    """Test that standard, well-formed bash-compatible environment variable keys pass validation."""
    validate_env_vars(
        {
            "TYPICAL_KEY_1": "TYPICAL_VALUE_1",
            "_typical_key_2": "typical_value_2",
        }
    )


@pytest.mark.parametrize(
    "bad_key", [1, tuple(), b"old_school"], ids=["int_key", "tuple_key", "bytes_key"]
)
def test_validate_env_vars_rejects_mistyped_keys(bad_key):
    """Test that non-string keys raise validation errors."""
    with pytest.raises(MetaflowException):
        validate_env_vars({bad_key: "v"})


@pytest.mark.parametrize(
    "bad_value", [1, {}, b"old_school"], ids=["int_value", "dict_value", "bytes_value"]
)
def test_validate_env_vars_rejects_mistyped_values(bad_value):
    """Test that non-string values raise validation errors."""
    with pytest.raises(MetaflowException):
        validate_env_vars({"K": bad_value})


@pytest.mark.parametrize(
    "weird_key",
    [
        "1_",
        "hello world",
        "hey_arnold!",
        "I_♥_NY",
        "door-",
        "METAFLOW_SOMETHING_OR_OTHER",
    ],
    ids=[
        "starts_with_number",
        "contains_space",
        "contains_symbol",
        "contains_unicode",
        "ends_with_dash",
        "reserved_metaflow_prefix",
    ],
)
def test_validate_env_vars_rejects_weird_keys(weird_key):
    """Test that invalid bash identifier formats or reserved prefixes are rejected."""
    with pytest.raises(MetaflowException):
        validate_env_vars({weird_key: "v"})
