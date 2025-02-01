#!/usr/bin/env bash

rm -f _pytest_plugin.py

PATCH_FILES="
_checkers.py
_functions.py
_importhook.py
_memo.py
_suppression.py
_transformer.py
_utils.py
"

for file in $PATCH_FILES; do
    sed -i \
        -e 's/from typing_extensions/from metaflow._vendor.typing_extensions/g' \
        -e 's/import typing_extensions/from metaflow._vendor import typing_extensions/g' \
        -e 's/from typeguard/from metaflow._vendor.typeguard/g' \
        "$file"
done
