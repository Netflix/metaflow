import os
import time

import pytest

from metaflow.exception import MetaflowException
import metaflow.metaflow_config
from metaflow.plugins.secrets.secrets_decorator import (
    SecretSpec,
    validate_env_vars_across_secrets,
    validate_env_vars_vs_existing_env,
    validate_env_vars,
    get_secrets_backend_provider,
)


@pytest.fixture
def default_secrets_backend(mocker):
    mocker.patch(
        "metaflow.metaflow_config.DEFAULT_SECRETS_BACKEND_TYPE",
        "some-default-backend-type",
    )


def test_missing_default_secrets_backend_type(mocker):
    mocker.patch("metaflow.metaflow_config.DEFAULT_SECRETS_BACKEND_TYPE", None)
    assert metaflow.metaflow_config.DEFAULT_SECRETS_BACKEND_TYPE is None
    with pytest.raises(MetaflowException):
        SecretSpec.secret_spec_from_str("secret_id", None)


def test_secret_spec_from_str_explicit_type(default_secrets_backend):
    assert SecretSpec.secret_spec_from_str("explicit-type.the_id", None).to_json() == {
        "options": {},
        "secret_id": "the_id",
        "secrets_backend_type": "explicit-type",
        "role": None,
    }


def test_secret_spec_from_str_implicit_type(default_secrets_backend):
    assert SecretSpec.secret_spec_from_str("the_id", None).to_json() == {
        "options": {},
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": None,
    }


def test_secret_spec_from_dict_explicit_type_no_options(default_secrets_backend):
    assert SecretSpec.secret_spec_from_dict(
        {"type": "explicit-type", "id": "the_id"}, None
    ).to_json() == {
        "options": {},
        "secret_id": "the_id",
        "secrets_backend_type": "explicit-type",
        "role": None,
    }


def test_secret_spec_from_dict_implicit_type_with_options(default_secrets_backend):
    assert SecretSpec.secret_spec_from_dict(
        {"id": "the_id", "options": {"a": "b"}}, None
    ).to_json() == {
        "options": {"a": "b"},
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": None,
    }


def test_role_resolution_source_level_wins(default_secrets_backend):
    assert SecretSpec.secret_spec_from_dict(
        {"id": "the_id", "role": "source-level-role"},
        "decorator-level-role",
    ).to_json() == {
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": "source-level-role",
        "options": {},
    }


def test_role_resolution_falls_back_to_decorator_level(default_secrets_backend):
    assert SecretSpec.secret_spec_from_dict(
        {"id": "the_id"},
        role="decorator-level-role",
    ).to_json() == {
        "secret_id": "the_id",
        "secrets_backend_type": "some-default-backend-type",
        "role": "decorator-level-role",
        "options": {},
    }


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
    with pytest.raises(MetaflowException):
        SecretSpec.secret_spec_from_dict(spec_dict, None)


def test_secrets_provider_resolution_unknown_backend():
    with pytest.raises(MetaflowException):
        get_secrets_backend_provider(str(time.time()))


def test_validate_env_vars_across_secrets_rejects_overlap():
    all_secrets_env_vars = [
        (SecretSpec.secret_spec_from_str("t.1", None), {"A": "a", "B": "b"}),
        (SecretSpec.secret_spec_from_str("t.2", None), {"B": "b", "C": "c"}),
    ]
    with pytest.raises(MetaflowException):
        validate_env_vars_across_secrets(all_secrets_env_vars)


def test_validate_env_vars_vs_existing_env_rejects_collision():
    existing_os_env_k, existing_os_env_v = next(iter(os.environ.items()))
    all_secrets_env_vars = [
        (
            SecretSpec.secret_spec_from_str("t.1", None),
            {"A": "a", existing_os_env_k: existing_os_env_v},
        ),
    ]
    with pytest.raises(MetaflowException):
        validate_env_vars_vs_existing_env(all_secrets_env_vars)


def test_validate_env_vars_accepts_typical_keys():
    validate_env_vars(
        {
            "TYPICAL_KEY_1": "TYPICAL_VALUE_1",
            "_typical_key_2": "typical_value_2",
        }
    )


@pytest.mark.parametrize("bad_key", [1, tuple(), b"old_school"])
def test_validate_env_vars_rejects_mistyped_keys(bad_key):
    with pytest.raises(MetaflowException):
        validate_env_vars({bad_key: "v"})


@pytest.mark.parametrize("bad_value", [1, {}, b"old_school"])
def test_validate_env_vars_rejects_mistyped_values(bad_value):
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
)
def test_validate_env_vars_rejects_weird_keys(weird_key):
    with pytest.raises(MetaflowException):
        validate_env_vars({weird_key: "v"})
