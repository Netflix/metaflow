## `typeguard` Vendor Log

### 2025 JAN 31

- To resolve [this issue](https://github.com/Netflix/metaflow/issues/2235),
  updated to typeguard 4.4.1.
- Source: https://github.com/agronholm/typeguard/archive/refs/tags/4.4.1.tar.gz
- Commands:

``` sh
cp ${TYPEGUARD_DIR}/src/typeguard ${METAFLOW_DIR}/metaflow/_vendor/typeguard
cd ${METAFLOW_DIR}/metaflow/_vendor/typeguard
bash ./_patch_typeguard.sh # Updates imports, removes extraneous files.
```

