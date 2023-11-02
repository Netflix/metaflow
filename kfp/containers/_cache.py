# Copyright 2019 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the speci

import hashlib
import os
import shutil
import tempfile
from pathlib import Path


def calculate_file_hash(file_path: str):
    block_size = 64 * 1024
    sha = hashlib.sha256()
    with open(file_path, 'rb') as fp:
        while True:
            data = fp.read(block_size)
            if not data:
                break
            sha.update(data)
    return sha.hexdigest()


def calculate_recursive_dir_hash(root_dir_path: str):
    path_hashes = {}
    for dirpath, dirnames, filenames in os.walk(root_dir_path):
        for file_name in filenames:
            file_path = os.path.join(dirpath, file_name)
            rel_file_path = os.path.relpath(file_path, root_dir_path)
            file_hash = calculate_file_hash(file_path)
            path_hashes[rel_file_path] = file_hash
    binary_path_hash_lines = sorted(
        path.encode('utf-8') + b'\t' + path_hash.encode('utf-8') + b'\n'
        for path, path_hash in path_hashes.items())
    binary_path_hash_doc = b''.join(binary_path_hash_lines)

    full_hash = hashlib.sha256(binary_path_hash_doc).hexdigest()
    return full_hash


def try_read_value_from_cache(cache_type: str, key: str) -> str:
    cache_file_path = Path(tempfile.tempdir) / cache_type / key
    if cache_file_path.exists():
        return cache_file_path.read_text()
    return None


def write_value_to_cache(cache_type: str, key: str, value: str):
    cache_file_path = Path(tempfile.tempdir) / cache_type / key
    if cache_file_path.exists():
        old_value = cache_file_path.read_text()
        if value != old_value:
            import warnings
            warnings.warn(
                'Overwriting existing cache entry "{}" with value "{}" != "{}".'
                .format(key, value, old_value))
    cache_file_path.parent.mkdir(parents=True, exist_ok=True)
    cache_file_path.write_text(value)


def clear_cache(cache_type: str):
    cache_file_path = Path(tempfile.tempdir) / cache_type
    if cache_file_path.exists():
        shutil.rmtree(cache_file_path)
