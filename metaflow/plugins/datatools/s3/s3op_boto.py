"""
Direct boto3 implementation of S3 batch operations.

Replaces the subprocess/multiprocessing path in s3op.py with
ThreadPoolExecutor for significantly lower per-operation overhead.
Gated behind the S3_DIRECT_BOTO3 feature flag.
"""

import errno
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import sha1

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    S3_LOG_TRANSIENT_RETRIES,
    S3_TRANSIENT_RETRY_COUNT,
    S3_WORKER_COUNT,
)
from metaflow.util import url_quote

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from .s3util import get_timestamp, read_in_chunks

RANGE_MATCH = re.compile(r"bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total>[0-9]+)")


class _S3OpTransientError(Exception):
    pass


class S3DirectClient(object):
    """
    Performs S3 list/info/get/put using boto3 directly via ThreadPoolExecutor.

    Parameters
    ----------
    s3_client : object
        Metaflow S3 client wrapper (has .client, .error, .reset_client()).
    tmpdir : str
        Temporary directory for downloaded files.
    inject_failures : int
        Failure injection rate for testing (0 = disabled).
    """

    def __init__(self, s3_client, tmpdir, inject_failures):
        self._s3_client = s3_client
        self._tmpdir = tmpdir
        self._inject_failures = inject_failures

    def _generate_local_path(self, url, range_str=None, suffix=None):
        if range_str is None:
            range_part = "whole"
        elif range_str == "whole":
            range_part = "whole"
        else:
            range_part = range_str[6:].replace("-", "_")
        quoted = url_quote(url)
        fname = quoted.split(b"/")[-1].replace(b".", b"_").replace(b"-", b"_")
        sha_hash = sha1(quoted).hexdigest()
        fname_decoded = fname.decode("utf-8")
        if len(fname_decoded) > 150:
            fname_decoded = fname_decoded[:150] + "..."
        if suffix:
            return "-".join((sha_hash, fname_decoded, range_part, suffix))
        return "-".join((sha_hash, fname_decoded, range_part))

    def _inject_failure_rate_for_mode(self, mode):
        if self._inject_failures <= 0:
            return 0
        if mode == "list":
            return 0
        if self._inject_failures >= 100:
            return 100
        return min(self._inject_failures, 90)

    def _maybe_inject_failure(self, inject_rate):
        if inject_rate > 0 and random.randint(0, 99) < inject_rate:
            raise _S3OpTransientError(
                "Injected failure for testing (rate=%d%%)" % inject_rate
            )

    def _do_batch_op(self, items, worker_fn, mode):
        if not items:
            return []

        pending = [(i, item) for i, item in enumerate(items)]
        results = {}
        base_inject_rate = self._inject_failure_rate_for_mode(mode)
        retry_count = S3_TRANSIENT_RETRY_COUNT
        if self._inject_failures > 0 and retry_count > 0:
            retry_count = max(retry_count, 100)
        last_ok_count = 0

        for attempt in range(retry_count + 1):
            if not pending:
                break

            if self._inject_failures > 0:
                max_count = len(pending)
            elif attempt == 0:
                max_count = len(pending)
            elif last_ok_count > 0:
                max_count = min(int(last_ok_count * 1.2), len(pending))
            else:
                max_count = min(2 * S3_WORKER_COUNT, len(pending))
            max_count = max(max_count, 1)

            batch = pending[:max_count]
            remaining = pending[max_count:]

            if base_inject_rate >= 100:
                inject_rate = 100
            elif base_inject_rate > 0:
                if attempt % 2 == 0:
                    inject_rate = max(1, base_inject_rate // 3)
                else:
                    inject_rate = min(int(base_inject_rate * 1.5), 90)
            else:
                inject_rate = 0

            succeeded = []
            failed = []

            client = self._s3_client.client
            error_class = self._s3_client.error

            with ThreadPoolExecutor(
                max_workers=min(S3_WORKER_COUNT, len(batch))
            ) as pool:
                future_to_idx = {}
                for idx, item in batch:
                    f = pool.submit(worker_fn, client, error_class, item, inject_rate)
                    future_to_idx[f] = (idx, item)

                for future in as_completed(future_to_idx):
                    idx, item = future_to_idx[future]
                    try:
                        result = future.result()
                        succeeded.append(idx)
                        results[idx] = result
                    except _S3OpTransientError as e:
                        if S3_LOG_TRANSIENT_RETRIES:
                            sys.stderr.write(
                                "[WARNING] S3 %s transient failure "
                                "(attempt %d): %s\n" % (mode, attempt, e)
                            )
                        failed.append((idx, item))

            last_ok_count = len(succeeded)
            pending = failed + remaining

            if pending and attempt < retry_count:
                self._s3_client.reset_client()
                if self._inject_failures > 0:
                    time.sleep(0.01)
                else:
                    time.sleep(min(2**attempt, 30) + random.randint(0, 5))

        if pending:
            from .s3 import MetaflowS3Exception

            raise MetaflowS3Exception(
                "S3 %s operation failed after %d retries for %d items"
                % (mode, retry_count, len(pending))
            )

        return [results[i] for i in sorted(results.keys())]

    def read_many(self, op, prefixes_and_ranges, **options):
        from .s3 import MetaflowS3Exception

        if op == "list":
            yield from self.list_objects(
                prefixes_and_ranges,
                recursive=options.get("recursive", False),
            )
        elif op == "info":
            yield from self.info_objects(prefixes_and_ranges)
        elif op == "get":
            if options.get("recursive", False):
                listed = list(self.list_objects(prefixes_and_ranges, recursive=True))
                url_to_prefix = {url: prefix for prefix, url, _size in listed}
                download_items = [(url, None) for _prefix, url, _size in listed]
                for prefix_or_url, url, fname in self.get_objects(
                    download_items,
                    allow_missing=options.get("allow_missing", False),
                    return_info=options.get("info", False),
                ):
                    yield (url_to_prefix.get(url, prefix_or_url), url, fname)
                return
            yield from self.get_objects(
                prefixes_and_ranges,
                allow_missing=options.get("allow_missing", False),
                return_info=options.get("info", False),
            )
        else:
            raise MetaflowS3Exception("Unknown operation: %s" % op)

    def list_objects(self, prefixes_and_ranges, recursive=False):
        from . import s3op
        from .s3 import MetaflowS3AccessDenied

        delimiter = "" if recursive else "/"

        def worker(client, error_class, item, inject_rate):
            prefix_str, _range = item
            self._maybe_inject_failure(inject_rate)

            parsed = urlparse(prefix_str)
            bucket = parsed.netloc
            path = parsed.path.lstrip("/")
            if path and not path.endswith("/"):
                path += "/"
            url_base = "s3://%s/" % bucket

            try:
                paginator = client.get_paginator("list_objects_v2")
                page_kwargs = {"Bucket": bucket, "Prefix": path}
                if delimiter:
                    page_kwargs["Delimiter"] = delimiter

                found = []
                for page in paginator.paginate(**page_kwargs):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            key_path = obj["Key"].lstrip("/")
                            url = url_base + key_path
                            found.append((prefix_str, url, str(obj["Size"])))
                    if "CommonPrefixes" in page:
                        for cp in page["CommonPrefixes"]:
                            url = url_base + cp["Prefix"]
                            found.append((prefix_str, url, ""))
                return found
            except error_class as err:
                error_code = s3op.normalize_client_error(err)
                if error_code == 404:
                    return []
                elif error_code == 403:
                    raise MetaflowS3AccessDenied(prefix_str)
                elif error_code in (500, 502, 503, 504):
                    raise _S3OpTransientError(str(err))
                else:
                    raise
            except MetaflowException:
                raise
            except Exception as e:
                raise _S3OpTransientError("transient error during list: %s" % e) from e

        items = list(prefixes_and_ranges)
        batch_results = self._do_batch_op(items, worker, "list")
        for result_list in batch_results:
            for item in result_list:
                yield item

    def info_objects(self, prefixes_and_ranges):
        from . import s3op
        from .s3 import MetaflowS3AccessDenied

        def worker(client, error_class, item, inject_rate):
            url_str, _range = item
            self._maybe_inject_failure(inject_rate)

            parsed = urlparse(url_str)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            local_name = self._generate_local_path(url_str)
            local_path = os.path.join(self._tmpdir, local_name)

            try:
                head = client.head_object(Bucket=bucket, Key=key)
                result = {
                    "error": None,
                    "size": head["ContentLength"],
                    "content_type": head.get("ContentType"),
                    "encryption": head.get("ServerSideEncryption"),
                    "metadata": head.get("Metadata", {}),
                    "last_modified": get_timestamp(head["LastModified"]),
                }
            except error_class as err:
                error_code = s3op.normalize_client_error(err)
                if error_code == 404:
                    result = {"error": s3op.ERROR_URL_NOT_FOUND}
                elif error_code == 403:
                    result = {"error": s3op.ERROR_URL_ACCESS_DENIED}
                elif error_code in (500, 502, 503, 504):
                    raise _S3OpTransientError(str(err))
                else:
                    result = {"error": error_code}
            except MetaflowException:
                raise
            except Exception as e:
                raise _S3OpTransientError("transient error during info: %s" % e) from e

            with open(local_path, "w") as f:
                json.dump(result, f)

            return (url_str, url_str, local_name)

        items = list(prefixes_and_ranges)
        results = self._do_batch_op(items, worker, "info")
        for result in results:
            yield result

    def get_objects(self, prefixes_and_ranges, allow_missing, return_info):
        from . import s3op
        from .s3 import (
            MetaflowS3AccessDenied,
            MetaflowS3InsufficientDiskSpace,
            MetaflowS3InvalidRange,
            MetaflowS3NotFound,
        )
        from boto3.s3.transfer import TransferConfig

        download_file_threshold = 2 * TransferConfig().multipart_threshold
        download_max_chunk = 2 * 1024 * 1024 * 1024 - 1

        def worker(client, error_class, item, inject_rate):
            url_str, range_str = item
            self._maybe_inject_failure(inject_rate)

            parsed = urlparse(url_str)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            local_name = self._generate_local_path(url_str, range_str)
            local_path = os.path.join(self._tmpdir, local_name)

            try:
                if range_str:
                    resp = client.get_object(Bucket=bucket, Key=key, Range=range_str)
                    range_result = resp["ContentRange"]
                    range_match = RANGE_MATCH.match(range_result)
                    if range_match is None:
                        raise RuntimeError(
                            "Wrong format for ContentRange: %s" % str(range_result)
                        )
                    range_result = {
                        x: int(range_match.group(x)) for x in ["total", "start", "end"]
                    }
                else:
                    resp = client.get_object(Bucket=bucket, Key=key)
                    range_result = None

                sz = resp["ContentLength"]
                if range_result is None:
                    range_result = {"total": sz, "start": 0, "end": sz - 1}

                if not range_str and sz > download_file_threshold:
                    resp["Body"].close()
                    client.download_file(bucket, key, local_path)
                else:
                    with open(local_path, "wb") as f:
                        read_in_chunks(f, resp["Body"], sz, download_max_chunk)

                if return_info:
                    meta = {
                        "size": (
                            range_result["total"]
                            if range_result
                            else resp["ContentLength"]
                        ),
                        "range_result": range_result,
                    }
                    if resp.get("ContentType"):
                        meta["content_type"] = resp["ContentType"]
                    if resp.get("Metadata") is not None:
                        meta["metadata"] = resp["Metadata"]
                    if resp.get("ServerSideEncryption") is not None:
                        meta["encryption"] = resp["ServerSideEncryption"]
                    if resp.get("LastModified"):
                        meta["last_modified"] = get_timestamp(resp["LastModified"])
                    with open(
                        os.path.join(self._tmpdir, "%s_meta" % local_name),
                        "w",
                    ) as f:
                        json.dump(meta, f)

                return (url_str, url_str, local_name)

            except error_class as err:
                error_code = s3op.normalize_client_error(err)
                if error_code == 404:
                    if allow_missing:
                        return (url_str, "", "")
                    raise MetaflowS3NotFound(url_str)
                elif error_code == 403:
                    raise MetaflowS3AccessDenied(url_str)
                elif error_code == 416:
                    raise MetaflowS3InvalidRange(str(err))
                elif error_code in (500, 502, 503, 504):
                    raise _S3OpTransientError(str(err))
                else:
                    raise
            except MetaflowException:
                raise
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    raise MetaflowS3InsufficientDiskSpace(
                        "Out of disk space downloading %s" % url_str
                    )
                raise _S3OpTransientError(str(e))
            except Exception as e:
                raise _S3OpTransientError("transient error during get: %s" % e) from e

        items = list(prefixes_and_ranges)
        results = self._do_batch_op(items, worker, "get")
        for result in results:
            yield result

    def put_objects(self, url_info, overwrite):
        from . import s3op
        from .s3 import MetaflowS3AccessDenied, MetaflowS3Exception

        def worker(client, error_class, item, inject_rate):
            local_path, url_str, store_info = item
            self._maybe_inject_failure(inject_rate)

            parsed = urlparse(url_str)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")

            if not overwrite:
                try:
                    client.head_object(Bucket=bucket, Key=key)
                    return None
                except error_class as err:
                    error_code = s3op.normalize_client_error(err)
                    if error_code == 404:
                        pass
                    elif error_code == 403:
                        raise MetaflowS3AccessDenied(url_str)
                    elif error_code in (500, 502, 503, 504):
                        raise _S3OpTransientError(str(err))
                    else:
                        raise

            extra_args = None
            ct = store_info.get("content_type")
            md = store_info.get("metadata")
            enc = store_info.get("encryption")
            if ct or md or enc:
                extra_args = {}
                if ct:
                    extra_args["ContentType"] = ct
                if md is not None:
                    extra_args["Metadata"] = md
                if enc is not None:
                    extra_args["ServerSideEncryption"] = enc

            try:
                client.upload_file(
                    os.path.realpath(local_path),
                    bucket,
                    key,
                    ExtraArgs=extra_args,
                )
                return url_str
            except error_class as err:
                error_code = s3op.normalize_client_error(err)
                if error_code == 403:
                    raise MetaflowS3AccessDenied(url_str)
                elif error_code in (500, 502, 503, 504):
                    raise _S3OpTransientError(str(err))
                else:
                    raise
            except MetaflowException:
                raise
            except Exception as e:
                raise _S3OpTransientError("transient error during put: %s" % e) from e

        items = list(url_info)
        results = self._do_batch_op(items, worker, "put")

        uploaded_urls = set()
        for result in results:
            if result is not None:
                uploaded_urls.add(result)

        return [(info["key"], url) for _, url, info in items if url in uploaded_urls]
