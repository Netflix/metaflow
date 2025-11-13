# FlowSpec Inheritance Tests

This test suite comprehensively tests various inheritance patterns for Metaflow FlowSpec classes, with a focus on real-world scenarios combining multiple features.

## Test Coverage

### 1. Comprehensive Linear Inheritance (`comprehensive_linear_flow.py`)
Tests linear inheritance chain: `FlowSpec -> BaseA -> BaseB -> BaseC -> ComprehensiveLinearFlow`

**Features tested:**
- Parameters at multiple levels (alpha, beta from BaseA; gamma from BaseC; delta from final)
- Configs at multiple levels (config_b from BaseB; config_c from BaseC)
- Decorated steps (@retry on start step in BaseB)
- Multi-step flow (start -> process -> end)
- Computations using inherited parameters and config values

**Verifies:**
- All inherited parameters accessible across hierarchy
- All inherited configs accessible and usable in computations
- Decorated steps execute correctly through inheritance
- Step-to-step data flow works correctly

### 2. Mutator with Base Config (`mutator_with_base_config_flow.py`)
Tests FlowMutator using config values from base class to inject parameters.

**Structure:** `BaseA (with config) -> BaseB (with mutator) -> MutatorWithBaseConfigFlow`

**Features tested:**
- Config defined in base class (mutator_config in BaseA)
- FlowMutator in middle class that reads base config (ConfigBasedMutator in BaseB)
- Parameter injection based on config values
- Original parameters remain accessible

**Verifies:**
- Mutator can access and read config from base class
- Parameters are dynamically injected based on config content
- Injected parameters have correct default values from config
- Computations can use both original and injected parameters

### 3. Mutator with Derived Config (`mutator_with_derived_config_flow.py`)
Tests FlowMutator in base class using config values from derived classes (forward-looking).

**Structure:** `BaseA (with mutator) -> BaseB -> BaseC (with config) -> MutatorWithDerivedConfigFlow`

**Features tested:**
- FlowMutator in base class (DerivedConfigMutator in BaseA)
- Config defined in derived class (runtime_config in BaseC)
- Base mutator accesses derived config to inject parameters
- Multiple configs across hierarchy

**Verifies:**
- Base class mutator can access config defined later in hierarchy
- Parameters injected from derived config are accessible
- Multiple parameters can be injected from single config
- Feature flag pattern works (injecting feature_* parameters)

### 4. Comprehensive Diamond Inheritance (`comprehensive_diamond_flow.py`)
Tests diamond pattern: `FlowSpec -> BaseA, FlowSpec -> BaseB, BaseA + BaseB -> BaseC -> ComprehensiveDiamondFlow`

**Features tested:**
- Two branches from FlowSpec with different parameters and configs
- BaseA branch: param_a, config_a, @retry decorated start step
- BaseB branch: param_b, config_b
- Merge point (BaseC): param_c, config_c, process step
- MRO (Method Resolution Order) resolution

**Verifies:**
- Parameters from both branches accessible (param_a, param_b, param_c)
- Configs from both branches accessible (config_a, config_b, config_c)
- Step from BaseA executes correctly (not duplicated from BaseB)
- Computations can use values from both branches
- Python MRO correctly resolves diamond pattern

### 5. Comprehensive Multi-Hierarchy (`comprehensive_multi_hierarchy_flow.py`)
Tests multiple inheritance from two independent hierarchies with all features combined.

**Structure:**
- First hierarchy: `FlowSpec -> BaseA (mutator) -> BaseB (decorated step)`
- Second hierarchy: `FlowSpec -> BaseX -> BaseY (step)`
- Merge: `BaseB + BaseY -> BaseC (step override) -> ComprehensiveMultiHierarchyFlow`

**Features tested:**
- Two completely independent inheritance hierarchies
- FlowMutator in first hierarchy (LoggingMutator on BaseA)
- Decorated step in first hierarchy (@retry on BaseB.start)
- Step override at merge point (BaseC.process overrides BaseY.process)
- Parameters and configs in both hierarchies
- Cross-hierarchy computations

**Verifies:**
- Parameters from both hierarchies accessible (param_a, param_b, param_x, param_y, param_c)
- Configs from both hierarchies accessible (config_a, config_x, config_y, config_c)
- Mutator from first hierarchy executes
- Decorated step from first hierarchy works
- Step override at merge point correctly replaces base implementation
- Computations can combine values from both hierarchies

## Design Improvements

### Consolidated Test Cases
Instead of having separate, simple test cases for each feature, the new tests combine multiple features to match real-world usage patterns:

**Old approach:**
- Separate test for linear inheritance with just parameters
- Separate test for linear inheritance with just configs
- Separate test for decorators
- Separate test for mutators

**New approach:**
- Single comprehensive linear test combining parameters, configs, decorators, and multi-step flow
- Mutator tests specifically focused on config interactions (base config usage, derived config usage)
- Comprehensive diamond and multi-hierarchy tests combining all features

### Standardized Naming
All test flows now use consistent base class naming:
- `BaseA`, `BaseB`, `BaseC` for linear hierarchies
- `BaseA`/`BaseB` for diamond branches, `BaseC` for merge point
- `BaseA`/`BaseB` for first hierarchy, `BaseX`/`BaseY` for second hierarchy in multi-inheritance

This makes it easier to understand the hierarchy structure at a glance.

### New Coverage: Mutators with Configs
Two important new test cases:
1. **Mutator with Base Config**: Mutator uses config from base class to inject parameters
2. **Mutator with Derived Config**: Mutator in base class uses config from derived class (forward-looking)

These patterns are common in real flows where configuration drives parameter injection.

## Running the Tests

### Run all inheritance tests:
```bash
pytest unit/inheritance/
```

### Run a specific test class:
```bash
pytest unit/inheritance/test_inheritance.py::TestComprehensiveLinear
pytest unit/inheritance/test_inheritance.py::TestMutatorWithBaseConfig
```

### Run a specific test:
```bash
pytest unit/inheritance/test_inheritance.py::TestComprehensiveLinear::test_all_parameters_accessible
```

### Use latest runs instead of creating new ones:
```bash
pytest unit/inheritance/ --use-latest
```

### Run with verbose output:
```bash
pytest unit/inheritance/ -v
```

### Run integration tests only:
```bash
pytest unit/inheritance/test_inheritance.py::TestInheritanceIntegration
```

## Test Structure

```
unit/inheritance/
├── __init__.py
├── README.md
├── conftest.py                          # Pytest configuration and fixtures
├── inheritance_test_helpers.py          # Helper functions (if needed)
├── test_inheritance.py                  # Main test file
└── flows/                               # Flow definitions
    ├── __init__.py
    ├── comprehensive_linear_flow.py
    ├── mutator_with_base_config_flow.py
    ├── mutator_with_derived_config_flow.py
    ├── comprehensive_diamond_flow.py
    └── comprehensive_multi_hierarchy_flow.py
```

## Test Design Philosophy

Each test flow:
1. **Combines multiple features** - Tests real-world patterns where parameters, configs, decorators, and mutators work together
2. **Uses consistent naming** - BaseA, BaseB, BaseC pattern makes hierarchy clear
3. **Stores verification artifacts** - Computes values and stores them for assertion
4. **Tests cross-level interactions** - Verifies that features from different hierarchy levels work together

Each test class:
1. **Verifies flow completion** - Ensures the flow runs successfully
2. **Tests feature accessibility** - Checks that inherited features are accessible
3. **Validates computations** - Ensures computed results using inherited values are correct
4. **Tests interactions** - Verifies that features interact correctly (e.g., mutator + config)

## Key Assertions

### Flow Execution
- All flows complete successfully (`successful` and `finished`)
- All expected steps are present and execute in order
- Decorated steps execute without errors

### Parameter & Config Inheritance
- Parameters from all hierarchy levels accessible
- Configs from all hierarchy levels accessible
- Values can be used in computations across steps

### Feature Interactions
- Mutators can access configs from base classes
- Mutators can access configs from derived classes (forward-looking)
- Decorated steps work through inheritance
- Step overrides replace base implementations

### Computation Correctness
- Computed values using inherited parameters correct
- Computed values using config values correct
- Computations combining multiple hierarchy levels work

## Notes

- Tests use the `Runner` API to execute flows programmatically
- Fixtures are session-scoped to avoid re-running flows for each test
- Use `--use-latest` flag to skip flow execution and use previous runs
- All test flows include `@project` decorator for proper namespacing
- Old flow files are kept for backward compatibility but can be removed if not needed elsewhere
