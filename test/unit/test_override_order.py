from metaflow import FlowSpec, Parameter
from metaflow.flowspec import FlowStateItems


def test_get_parameters_full_semantics():
    """
    Comprehensive validation of FlowSpec._get_parameters() behavior.

    Validates:
    - Override resolution (child wins)
    - Multi-level inheritance
    - Multiple inheritance
    - MRO precedence
    - Definition-order behavior within each class
    - No duplicates
    - Cached/uncached consistency
    """

    # -------------------------------
    # Define inheritance hierarchy
    # -------------------------------

    class BaseA(FlowSpec):
        a = Parameter("a")

    class BaseB(FlowSpec):
        b = Parameter("b")

    class Parent(BaseA, BaseB):
        p = Parameter("p")

    class Child(Parent):
        # Override BaseA.a
        a = Parameter("a")
        c = Parameter("c")

    # -------------------------------
    # First call (uncached)
    # -------------------------------

    first = list(Child._get_parameters())
    first_names = [name for name, _ in first]
    first_dict = dict(first)

    # -------------------------------
    # Second call (cached)
    # -------------------------------

    second = list(Child._get_parameters())
    second_names = [name for name, _ in second]
    second_dict = dict(second)

    # -------------------------------
    # 1️⃣ Override correctness
    # -------------------------------

    assert first_dict["a"] is Child.a
    assert first_dict["a"] is not BaseA.a

    # -------------------------------
    # 2️⃣ Inheritance correctness
    # -------------------------------

    assert set(first_names) == {"a", "c", "p", "b"}

    # -------------------------------
    # 3️⃣ No duplicates
    # -------------------------------

    assert len(first_names) == len(set(first_names))

    # -------------------------------
    # 4️⃣ Cached consistency
    # -------------------------------

    assert first_names == second_names
    assert first_dict["a"] is second_dict["a"]

    # -------------------------------
    # 5️⃣ Ordering semantics
    # -------------------------------

    # Expected behavior:
    # - Child parameters first (definition order)
    # - Then Parent parameters (definition order)
    # - Then BaseA/BaseB parameters respecting MRO
    #
    # Child defines: a (override), c
    # Parent defines: p
    # BaseA defines: a (shadowed)
    # BaseB defines: b

    # Child parameters must come before parent parameters
    assert first_names.index("a") < first_names.index("p")
    assert first_names.index("c") < first_names.index("p")

    # Parent parameter must come before BaseB parameter
    assert first_names.index("p") < first_names.index("b")


def test_mixin_and_config_parameter_handling():
    """
    Validates:
    - Non-FlowSpec mixins defining Parameters are discovered.
    - Config parameters yielded from SET_CONFIG_PARAMETERS are not duplicated.
    - Override semantics remain correct.
    """

    # -----------------------------
    # 1️⃣ Non-FlowSpec mixin support
    # -----------------------------

    class ParamMixin:
        x = Parameter("x")

    class MixinFlow(ParamMixin, FlowSpec):
        y = Parameter("y")

    params = list(MixinFlow._get_parameters())
    param_names = [name for name, _ in params]

    assert set(param_names) == {"x", "y"}

    # -----------------------------
    # 2️⃣ Config parameter deduplication
    # -----------------------------

    class ConfigFlow(FlowSpec):
        a = Parameter("a")
        b = Parameter("b")

    # Simulate config mutation behavior:
    # Pretend parameter "a" has been converted and stored in SET_CONFIG_PARAMETERS
    config_param = ConfigFlow.a
    ConfigFlow._flow_state[FlowStateItems.SET_CONFIG_PARAMETERS] = [("a", config_param)]

    params = list(ConfigFlow._get_parameters())
    param_names = [name for name, _ in params]

    # Must contain both parameters
    assert set(param_names) == {"a", "b"}

    # Must not duplicate "a"
    assert param_names.count("a") == 1

    # Clean up state for safety (avoid leaking into other tests)
    ConfigFlow._flow_state[FlowStateItems.SET_CONFIG_PARAMETERS] = []
    ConfigFlow._flow_state[FlowStateItems.CACHED_PARAMETERS] = None
