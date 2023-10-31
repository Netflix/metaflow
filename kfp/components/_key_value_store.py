import hashlib
from pathlib import Path


class KeyValueStore:
    KEY_FILE_SUFFIX = '.key'
    VALUE_FILE_SUFFIX = '.value'

    def __init__(
        self,
        cache_dir: str,
    ):
        cache_dir = Path(cache_dir)
        hash_func = (
            lambda text: hashlib.sha256(text.encode('utf-8')).hexdigest())
        self.cache_dir = cache_dir
        self.hash_func = hash_func

    def store_value_text(self, key: str, text: str) -> str:
        return self.store_value_bytes(key, text.encode('utf-8'))

    def store_value_bytes(self, key: str, data: bytes) -> str:
        cache_id = self.hash_func(key)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key_file_path = self.cache_dir / (
            cache_id + KeyValueStore.KEY_FILE_SUFFIX)
        cache_value_file_path = self.cache_dir / (
            cache_id + KeyValueStore.VALUE_FILE_SUFFIX)
        if cache_key_file_path.exists():
            old_key = cache_key_file_path.read_text()
            if key != old_key:
                raise RuntimeError(
                    'Cache is corrupted: File "{}" contains existing key '
                    '"{}" != new key "{}"'.format(cache_key_file_path, old_key,
                                                  key))
            if cache_value_file_path.exists():
                old_data = cache_value_file_path.read_bytes()
                if data != old_data:
                    # TODO: Add options to raise error when overwriting the value.
                    pass
        cache_value_file_path.write_bytes(data)
        cache_key_file_path.write_text(key)
        return cache_id

    def try_get_value_text(self, key: str) -> str:
        result = self.try_get_value_bytes(key)
        if result is None:
            return None
        return result.decode('utf-8')

    def try_get_value_bytes(self, key: str) -> bytes:
        cache_id = self.hash_func(key)
        cache_value_file_path = self.cache_dir / (
            cache_id + KeyValueStore.VALUE_FILE_SUFFIX)
        if cache_value_file_path.exists():
            return cache_value_file_path.read_bytes()
        return None

    def exists(self, key: str) -> bool:
        cache_id = self.hash_func(key)
        cache_key_file_path = self.cache_dir / (
            cache_id + KeyValueStore.KEY_FILE_SUFFIX)
        return cache_key_file_path.exists()

    def keys(self):
        for cache_key_file_path in self.cache_dir.glob(
                '*' + KeyValueStore.KEY_FILE_SUFFIX):
            yield Path(cache_key_file_path).read_text()
