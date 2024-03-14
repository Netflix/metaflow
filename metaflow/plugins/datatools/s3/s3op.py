from __future__ import print_function

import json
import time
import math
import random
import re
import sys
import os
import traceback
from collections import namedtuple
from functools import partial, wraps
from hashlib import sha1
from tempfile import NamedTemporaryFile
from multiprocessing import Process, Queue
from itertools import starmap, chain, islice

from boto3.s3.transfer import TransferConfig

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

if __name__ == "__main__":
    # When launched standalone, point to our parent metaflow
    sys.path.insert(
        0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
    )

from metaflow._vendor import click

# we use Metaflow's parallel_imap_unordered instead of
# multiprocessing.Pool because https://bugs.python.org/issue31886
from metaflow.util import TempDir, url_quote, url_unquote
from metaflow.multicore_utils import parallel_map
from metaflow.plugins.datatools.s3.s3util import (
    aws_retry,
    read_in_chunks,
    get_timestamp,
    TRANSIENT_RETRY_LINE_CONTENT,
    TRANSIENT_RETRY_START_LINE,
)
import metaflow.tracing as tracing
from metaflow.metaflow_config import (
    S3_WORKER_COUNT,
)

DOWNLOAD_FILE_THRESHOLD = 2 * TransferConfig().multipart_threshold
DOWNLOAD_MAX_CHUNK = 2 * 1024 * 1024 * 1024 - 1

RANGE_MATCH = re.compile(r"bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total>[0-9]+)")

S3Config = namedtuple("S3Config", "role session_vars client_params")


class S3Url(object):
    def __init__(
        self,
        bucket,
        path,
        url,
        local,
        prefix,
        content_type=None,
        encryption=None,
        metadata=None,
        range=None,
        idx=None,
    ):

        self.bucket = bucket
        self.path = path
        self.url = url
        self.local = local
        self.prefix = prefix
        self.content_type = content_type
        self.metadata = metadata
        self.range = range
        self.idx = idx
        self.encryption = encryption

    def __str__(self):
        return self.url


# We use error codes instead of Exceptions, which are trickier to
# handle reliably in a multiprocess world
ERROR_INVALID_URL = 4
ERROR_NOT_FULL_PATH = 5
ERROR_URL_NOT_FOUND = 6
ERROR_URL_ACCESS_DENIED = 7
ERROR_WORKER_EXCEPTION = 8
ERROR_VERIFY_FAILED = 9
ERROR_LOCAL_FILE_NOT_FOUND = 10
ERROR_INVALID_RANGE = 11
ERROR_TRANSIENT = 12


def format_result_line(idx, prefix, url="", local=""):
    # We prefix each output with the index corresponding to the line number on the
    # initial request (ie: prior to any transient errors). This allows us to
    # properly maintain the order in which things were requested even in the presence
    # of transient retries where we do not know what succeeds and what does not.
    # Basically, when we retry an operation, we can trace it back to its original
    # position in the first request.
    return " ".join(
        [str(idx)] + [url_quote(x).decode("utf-8") for x in (prefix, url, local)]
    )


# I can't understand what's the right way to deal
# with boto errors. This function can be replaced
# with better error handling code.
def normalize_client_error(err):
    error_code = err.response["Error"]["Code"]
    try:
        return int(error_code)
    except ValueError:
        if error_code in ("AccessDenied", "AllAccessDisabled"):
            return 403
        if error_code == "NoSuchKey":
            return 404
        if error_code == "InvalidRange":
            return 416
        # We "normalize" retriable server errors to 503. These are also considered
        # transient by boto3 (see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html)
        if error_code in (
            "SlowDown",
            "RequestTimeout",
            "RequestTimeoutException",
            "PriorRequestNotComplete",
            "ConnectionError",
            "HTTPClientError",
            "Throttling",
            "ThrottlingException",
            "ThrottledException",
            "RequestThrottledException",
            "TooManyRequestsException",
            "ProvisionedThroughputExceededException",
            "TransactionInProgressException",
            "RequestLimitExceeded",
            "BandwidthLimitExceeded",
            "LimitExceededException",
            "RequestThrottled",
            "EC2ThrottledException",
        ):
            return 503
    return error_code


# S3 worker pool


@tracing.cli_entrypoint("s3op/worker")
def worker(result_file_name, queue, mode, s3config):
    # Interpret mode, it can either be a single op or something like
    # info_download or info_upload which implies:
    #  - for download: we need to return the information as well
    #  - for upload: we need to not overwrite the file if it exists
    modes = mode.split("_")
    pre_op_info = False
    if len(modes) > 1:
        pre_op_info = True
        mode = modes[1]
    else:
        mode = modes[0]

    def op_info(url):
        try:
            head = s3.head_object(Bucket=url.bucket, Key=url.path)
            to_return = {
                "error": None,
                "size": head["ContentLength"],
                "content_type": head["ContentType"],
                "encryption": head.get("ServerSideEncryption"),
                "metadata": head["Metadata"],
                "last_modified": get_timestamp(head["LastModified"]),
            }
        except client_error as err:
            error_code = normalize_client_error(err)
            if error_code == 404:
                to_return = {"error": ERROR_URL_NOT_FOUND, "raise_error": err}
            elif error_code == 403:
                to_return = {"error": ERROR_URL_ACCESS_DENIED, "raise_error": err}
            elif error_code == 416:
                to_return = {"error": ERROR_INVALID_RANGE, "raise_error": err}
            elif error_code in (500, 502, 503, 504):
                to_return = {"error": ERROR_TRANSIENT, "raise_error": err}
            else:
                to_return = {"error": error_code, "raise_error": err}
        return to_return

    with open(result_file_name, "w") as result_file:
        try:
            from metaflow.plugins.datatools.s3.s3util import get_s3_client

            s3, client_error = get_s3_client(
                s3_role_arn=s3config.role,
                s3_session_vars=s3config.session_vars,
                s3_client_params=s3config.client_params,
            )
            while True:
                url, idx = queue.get()
                if url is None:
                    break
                if mode == "info":
                    result = op_info(url)
                    orig_error = result.get("raise_error", None)
                    if orig_error:
                        del result["raise_error"]
                    with open(url.local, "w") as f:
                        json.dump(result, f)
                    result_file.write(
                        "%d %d\n"
                        % (idx, -1 * result["error"] if orig_error else result["size"])
                    )
                elif mode == "download":
                    tmp = NamedTemporaryFile(dir=".", mode="wb", delete=False)
                    try:
                        if url.range:
                            resp = s3.get_object(
                                Bucket=url.bucket, Key=url.path, Range=url.range
                            )
                            range_result = resp["ContentRange"]
                            range_result_match = RANGE_MATCH.match(range_result)
                            if range_result_match is None:
                                raise RuntimeError(
                                    "Wrong format for ContentRange: %s"
                                    % str(range_result)
                                )
                            range_result = {
                                x: int(range_result_match.group(x))
                                for x in ["total", "start", "end"]
                            }
                        else:
                            resp = s3.get_object(Bucket=url.bucket, Key=url.path)
                            range_result = None
                        sz = resp["ContentLength"]
                        if range_result is None:
                            range_result = {"total": sz, "start": 0, "end": sz - 1}
                        if not url.range and sz > DOWNLOAD_FILE_THRESHOLD:
                            # In this case, it is more efficient to use download_file as it
                            # will download multiple parts in parallel (it does it after
                            # multipart_threshold)
                            s3.download_file(url.bucket, url.path, tmp.name)
                        else:
                            read_in_chunks(tmp, resp["Body"], sz, DOWNLOAD_MAX_CHUNK)
                        tmp.close()
                        os.rename(tmp.name, url.local)
                    except client_error as err:
                        tmp.close()
                        os.unlink(tmp.name)
                        error_code = normalize_client_error(err)
                        if error_code == 404:
                            result_file.write("%d %d\n" % (idx, -ERROR_URL_NOT_FOUND))
                            continue
                        elif error_code == 403:
                            result_file.write(
                                "%d %d\n" % (idx, -ERROR_URL_ACCESS_DENIED)
                            )
                            continue
                        elif error_code == 503:
                            result_file.write("%d %d\n" % (idx, -ERROR_TRANSIENT))
                            continue
                        else:
                            raise
                        # TODO specific error message for out of disk space
                    # If we need the metadata, get it and write it out
                    if pre_op_info:
                        with open("%s_meta" % url.local, mode="w") as f:
                            # Get range information

                            args = {
                                "size": resp["ContentLength"],
                                "range_result": range_result,
                            }
                            if resp["ContentType"]:
                                args["content_type"] = resp["ContentType"]
                            if resp["Metadata"] is not None:
                                args["metadata"] = resp["Metadata"]
                            if resp.get("ServerSideEncryption") is not None:
                                args["encryption"] = resp["ServerSideEncryption"]
                            if resp["LastModified"]:
                                args["last_modified"] = get_timestamp(
                                    resp["LastModified"]
                                )
                            json.dump(args, f)
                    # Finally, we push out the size to the result_pipe since
                    # the size is used for verification and other purposes, and
                    # we want to avoid file operations for this simple process
                    result_file.write("%d %d\n" % (idx, resp["ContentLength"]))
                else:
                    # This is upload, if we have a pre_op, it means we do not
                    # want to overwrite
                    do_upload = False
                    if pre_op_info:
                        result_info = op_info(url)
                        if result_info["error"] == ERROR_URL_NOT_FOUND:
                            # We only upload if the file is not found
                            do_upload = True
                    else:
                        # No pre-op so we upload
                        do_upload = True
                    if do_upload:
                        extra = None
                        if url.content_type or url.metadata or url.encryption:
                            extra = {}
                            if url.content_type:
                                extra["ContentType"] = url.content_type
                            if url.metadata is not None:
                                extra["Metadata"] = url.metadata
                            if url.encryption is not None:
                                extra["ServerSideEncryption"] = url.encryption
                        try:
                            s3.upload_file(
                                url.local, url.bucket, url.path, ExtraArgs=extra
                            )
                            # We indicate that the file was uploaded
                            result_file.write("%d %d\n" % (idx, 0))
                        except client_error as err:
                            error_code = normalize_client_error(err)
                            if error_code == 403:
                                result_file.write(
                                    "%d %d\n" % (idx, -ERROR_URL_ACCESS_DENIED)
                                )
                                continue
                            elif error_code == 503:
                                result_file.write("%d %d\n" % (idx, -ERROR_TRANSIENT))
                                continue
                            else:
                                raise
        except:
            traceback.print_exc()
            sys.exit(ERROR_WORKER_EXCEPTION)


def start_workers(mode, urls, num_workers, inject_failure, s3config):
    # We start the minimum of len(urls) or num_workers to avoid starting
    # workers that will definitely do nothing
    num_workers = min(num_workers, len(urls))
    queue = Queue(len(urls) + num_workers)
    procs = {}
    random.seed()

    sz_results = []
    # 1. push sources and destinations to the queue
    # We only push if we don't inject a failure; otherwise, we already set the sz_results
    # appropriately with the result of the injected failure.
    for idx, elt in enumerate(urls):
        if random.randint(0, 99) < inject_failure:
            sz_results.append(-ERROR_TRANSIENT)
        else:
            sz_results.append(None)
            queue.put((elt, idx))

    # 2. push end-of-queue markers
    for i in range(num_workers):
        queue.put((None, None))

    # 3. start processes
    with TempDir() as output_dir:
        for i in range(num_workers):
            file_path = os.path.join(output_dir, str(i))
            p = Process(
                target=worker,
                args=(file_path, queue, mode, s3config),
            )
            p.start()
            procs[p] = file_path

        # 4. wait for the processes to finish; we continuously update procs
        # to remove all processes that have finished already
        while procs:
            new_procs = {}
            for proc, out_path in procs.items():
                proc.join(timeout=1)
                if proc.exitcode is not None:
                    if proc.exitcode != 0:
                        msg = "Worker process failed (exit code %d)" % proc.exitcode
                        exit(msg, proc.exitcode)
                    # Read the output file if all went well
                    with open(out_path, "r") as out_file:
                        for line in out_file:
                            line_split = line.split(" ")
                            sz_results[int(line_split[0])] = int(line_split[1])
                else:
                    # Put this process back in the processes to check
                    new_procs[proc] = out_path
            procs = new_procs
    return sz_results


def process_urls(mode, urls, verbose, inject_failure, num_workers, s3config):

    if verbose:
        print("%sing %d files.." % (mode.capitalize(), len(urls)), file=sys.stderr)

    start = time.time()
    sz_results = start_workers(mode, urls, num_workers, inject_failure, s3config)
    end = time.time()

    if verbose:
        total_size = sum(sz for sz in sz_results if sz is not None and sz > 0)
        bw = total_size / (end - start)
        print(
            "%sed %d files, %s in total, in %d seconds (%s/s)."
            % (
                mode.capitalize(),
                len(urls),
                with_unit(total_size),
                end - start,
                with_unit(bw),
            ),
            file=sys.stderr,
        )
    return sz_results


# Utility functions


def with_unit(x):
    if x > 1024**3:
        return "%.1fGB" % (x / 1024.0**3)
    elif x > 1024**2:
        return "%.1fMB" % (x / 1024.0**2)
    elif x > 1024:
        return "%.1fKB" % (x / 1024.0)
    else:
        return "%d bytes" % x


# S3Ops class is just a wrapper for get_size and list_prefix
# required by @aws_retry decorator, which needs the reset_client
# method. Otherwise they would be just stand-alone functions.
class S3Ops(object):
    def __init__(self, s3config):
        self.s3 = None
        self.s3config = s3config
        self.client_error = None

    def reset_client(self, hard_reset=False):
        from metaflow.plugins.datatools.s3.s3util import get_s3_client

        if hard_reset or self.s3 is None:
            self.s3, self.client_error = get_s3_client(
                s3_role_arn=self.s3config.role,
                s3_session_vars=self.s3config.session_vars,
                s3_client_params=self.s3config.client_params,
            )

    @aws_retry
    def get_info(self, url):
        self.reset_client()
        try:
            head = self.s3.head_object(Bucket=url.bucket, Key=url.path)
            return (
                True,
                url,
                [
                    (
                        S3Url(
                            bucket=url.bucket,
                            path=url.path,
                            url=url.url,
                            local=url.local,
                            prefix=url.prefix,
                            content_type=head["ContentType"],
                            metadata=head["Metadata"],
                            encryption=head.get("ServerSideEncryption"),
                            range=url.range,
                        ),
                        head["ContentLength"],
                    )
                ],
            )
        except self.client_error as err:
            error_code = normalize_client_error(err)
            if error_code == 404:
                return False, url, ERROR_URL_NOT_FOUND
            elif error_code == 403:
                return False, url, ERROR_URL_ACCESS_DENIED
            # Transient errors are going to be retried by the aws_retry decorator
            else:
                raise

    @aws_retry
    def list_prefix(self, prefix_url, delimiter=""):
        self.reset_client()
        url_base = "s3://%s/" % prefix_url.bucket
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            urls = []
            for page in paginator.paginate(
                Bucket=prefix_url.bucket, Prefix=prefix_url.path, Delimiter=delimiter
            ):
                # note that an url may be both a prefix and an object
                # - the trailing slash is significant in S3
                if "Contents" in page:
                    for key in page.get("Contents", []):
                        url = url_base + key["Key"]
                        urlobj = S3Url(
                            url=url,
                            bucket=prefix_url.bucket,
                            path=key["Key"],
                            local=generate_local_path(url),
                            prefix=prefix_url.url,
                        )
                        urls.append((urlobj, key["Size"]))
                if "CommonPrefixes" in page:
                    # we get CommonPrefixes if Delimiter is a non-empty string
                    for key in page.get("CommonPrefixes", []):
                        url = url_base + key["Prefix"]
                        urlobj = S3Url(
                            url=url,
                            bucket=prefix_url.bucket,
                            path=key["Prefix"],
                            local=None,
                            prefix=prefix_url.url,
                        )
                        urls.append((urlobj, None))
            return True, prefix_url, urls
        except self.s3.exceptions.NoSuchBucket:
            return False, prefix_url, ERROR_URL_NOT_FOUND
        except self.client_error as err:
            error_code = normalize_client_error(err)
            if error_code == 404:
                return False, prefix_url, ERROR_URL_NOT_FOUND
            elif error_code == 403:
                return False, prefix_url, ERROR_URL_ACCESS_DENIED
            # Transient errors are going to be retried by the aws_retry decorator
            else:
                raise


# We want to reuse an S3 client instance over multiple operations.
# This is accomplished by op_ functions below.


def op_get_info(s3config, urls):
    s3 = S3Ops(s3config)
    return [s3.get_info(url) for url in urls]


def op_list_prefix(s3config, prefix_urls):
    s3 = S3Ops(s3config)
    return [s3.list_prefix(prefix) for prefix in prefix_urls]


def op_list_prefix_nonrecursive(s3config, prefix_urls):
    s3 = S3Ops(s3config)
    return [s3.list_prefix(prefix, delimiter="/") for prefix in prefix_urls]


def exit(exit_code, url):
    if exit_code == ERROR_INVALID_URL:
        msg = "Invalid url: %s" % url.url
    elif exit_code == ERROR_NOT_FULL_PATH:
        msg = "URL not a full path: %s" % url.url
    elif exit_code == ERROR_URL_NOT_FOUND:
        msg = "URL not found: %s" % url.url
    elif exit_code == ERROR_URL_ACCESS_DENIED:
        msg = "Access denied to URL: %s" % url.url
    elif exit_code == ERROR_WORKER_EXCEPTION:
        msg = "Download failed"
    elif exit_code == ERROR_VERIFY_FAILED:
        msg = "Verification failed for URL %s, local file %s" % (url.url, url.local)
    elif exit_code == ERROR_LOCAL_FILE_NOT_FOUND:
        msg = "Local file not found: %s" % url
    elif exit_code == ERROR_TRANSIENT:
        msg = "Transient error for url: %s" % url
    else:
        msg = "Unknown error"
    print("s3op failed:\n%s" % msg, file=sys.stderr)
    sys.exit(exit_code)


def verify_results(urls, verbose=False):
    for url, expected in urls:
        if verbose:
            print("verifying %s, expected %s" % (url, expected), file=sys.stderr)
        try:
            got = os.stat(url.local).st_size
        except OSError:
            raise
        if expected != got:
            exit(ERROR_VERIFY_FAILED, url)
        if url.content_type or url.metadata or url.encryption:
            # Verify that we also have a metadata file present
            try:
                os.stat("%s_meta" % url.local)
            except OSError:
                exit(ERROR_VERIFY_FAILED, url)


def generate_local_path(url, range="whole", suffix=None):
    # this function generates a safe local file name corresponding to
    # an S3 URL. URLs may be longer than maximum file length limit on Linux,
    # so we mostly hash the URL but retain the leaf part as a convenience
    # feature to ease eyeballing
    # We also call out "range" specifically to allow multiple ranges for the same
    # file to be downloaded in parallel.
    if range is None:
        range = "whole"
    if range != "whole":
        # It will be of the form `bytes=%d-` or `bytes=-%d` or `bytes=%d-%d`
        range = range[6:].replace("-", "_")
    quoted = url_quote(url)
    fname = quoted.split(b"/")[-1].replace(b".", b"_").replace(b"-", b"_")
    sha = sha1(quoted).hexdigest()
    if suffix:
        return "-".join((sha, fname.decode("utf-8"), range, suffix))
    return "-".join((sha, fname.decode("utf-8"), range))


def parallel_op(op, lst, num_workers):
    # parallel op divides work equally amongst num_workers
    # processes. This is a good strategy if the cost is
    # uniform over the units of work, e.g. op_get_info, which
    # is a single HEAD request to S3.
    #
    # This approach is less optimal with op_list_prefix where
    # the cost of S3 listing per prefix can vary drastically.
    # We could optimize this case by using a worker model with
    # a queue, like for downloads but the difference here is
    # that we need to return a value, which would require a
    # bit more work - something to consider if this turns out
    # to be a bottleneck.
    if lst:
        num = min(len(lst), num_workers)
        batch_size = math.ceil(len(lst) / float(num))
        batches = []
        it = iter(lst)
        while True:
            batch = list(islice(it, batch_size))
            if batch:
                batches.append(batch)
            else:
                break
        it = parallel_map(op, batches, max_parallel=num)
        for x in chain.from_iterable(it):
            yield x


# CLI


def common_options(func):
    @click.option(
        "--inputs",
        type=click.Path(exists=True),
        help="Read input prefixes from the given file.",
    )
    @click.option(
        "--num-workers",
        default=S3_WORKER_COUNT,
        show_default=True,
        help="Number of concurrent connections.",
    )
    @click.option(
        "--s3role",
        default=None,
        show_default=True,
        required=False,
        help="Role to assume when getting the S3 client",
    )
    @click.option(
        "--s3sessionvars",
        default=None,
        show_default=True,
        required=False,
        help="Session vars to set when getting the S3 client",
    )
    @click.option(
        "--s3clientparams",
        default=None,
        show_default=True,
        required=False,
        help="Client parameters to set when getting the S3 client",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def non_lst_common_options(func):
    @click.option(
        "--verbose/--no-verbose",
        default=True,
        show_default=True,
        help="Print status information on stderr.",
    )
    @click.option(
        "--listing/--no-listing",
        default=False,
        show_default=True,
        help="Print S3 URL -> local file mapping on stdout.",
    )
    @click.option(
        "--inject-failure",
        default=0,
        show_default=True,
        type=int,
        help="Simulate transient failures -- percentage (int) of injected failures",
        hidden=True,
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group()
def cli():
    pass


@tracing.cli_entrypoint("s3op/list")
@cli.command("list", help="List S3 objects")
@click.option(
    "--recursive/--no-recursive",
    default=False,
    show_default=True,
    help="List prefixes recursively.",
)
@common_options
@click.argument("prefixes", nargs=-1)
def lst(
    prefixes,
    inputs=None,
    num_workers=None,
    recursive=None,
    s3role=None,
    s3sessionvars=None,
    s3clientparams=None,
):

    s3config = S3Config(
        s3role,
        json.loads(s3sessionvars) if s3sessionvars else None,
        json.loads(s3clientparams) if s3clientparams else None,
    )

    urllist = []
    to_iterate, _ = _populate_prefixes(prefixes, inputs)
    for _, prefix, url, _ in to_iterate:
        src = urlparse(url)
        url = S3Url(
            url=url,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=None,
            prefix=prefix,
        )
        if src.scheme != "s3":
            exit(ERROR_INVALID_URL, url)
        urllist.append(url)

    op = (
        partial(op_list_prefix, s3config)
        if recursive
        else partial(op_list_prefix_nonrecursive, s3config)
    )
    urls = []
    for success, prefix_url, ret in parallel_op(op, urllist, num_workers):
        if success:
            urls.extend(ret)
        else:
            exit(ret, prefix_url)

    for idx, (url, size) in enumerate(urls):
        if size is None:
            print(format_result_line(idx, url.prefix, url.url))
        else:
            print(format_result_line(idx, url.prefix, url.url, str(size)))


@tracing.cli_entrypoint("s3op/put")
@cli.command(help="Upload files to S3")
@click.option(
    "--file",
    "files",
    type=(click.Path(exists=True), str),
    multiple=True,
    help="Local file->S3Url pair to upload. Can be specified multiple times.",
)
@click.option(
    "--filelist",
    type=click.Path(exists=True),
    help="Read local file -> S3 URL mappings from the given file. Use --inputs instead",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=True,
    show_default=True,
    help="Overwrite key if it already exists in S3.",
)
@common_options
@non_lst_common_options
def put(
    files=None,
    filelist=None,
    inputs=None,
    num_workers=None,
    verbose=None,
    overwrite=True,
    listing=None,
    s3role=None,
    s3sessionvars=None,
    s3clientparams=None,
    inject_failure=0,
):
    if inputs is not None and filelist is not None:
        raise RuntimeError("Cannot specify inputs and filelist at the same time")
    if inputs is not None and filelist is None:
        filelist = inputs

    is_transient_retry = False

    def _files():
        nonlocal is_transient_retry
        line_idx = 0
        for local, url in files:
            local_file = url_unquote(local)
            if not os.path.exists(local_file):
                exit(ERROR_LOCAL_FILE_NOT_FOUND, local_file)
            yield line_idx, local_file, url_unquote(url), None, None
            line_idx += 1
        if filelist:
            # NOTE: We are assuming that the idx is properly set. This is only used
            # by the transient failure retry mechanism and users should not use it
            # directly. This will not work, for example, if only some lines have
            # an idx specified (in some cases)
            for line in open(filelist, mode="rb"):
                r = json.loads(line)
                input_line_idx = r.get("idx")
                if input_line_idx is not None:
                    # We only have input indices if we have a transient retry.
                    is_transient_retry = True
                else:
                    input_line_idx = line_idx
                line_idx += 1
                local = r["local"]
                url = r["url"]
                content_type = r.get("content_type", None)
                metadata = r.get("metadata", None)
                encryption = r.get("encryption", None)
                if not os.path.exists(local):
                    exit(ERROR_LOCAL_FILE_NOT_FOUND, local)
                yield input_line_idx, local, url, content_type, metadata, encryption

    def _make_url(idx, local, user_url, content_type, metadata, encryption):
        src = urlparse(user_url)
        url = S3Url(
            url=user_url,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=local,
            prefix=None,
            content_type=content_type,
            metadata=metadata,
            idx=idx,
            encryption=encryption,
        )
        if src.scheme != "s3":
            exit(ERROR_INVALID_URL, url)
        if not src.path:
            exit(ERROR_NOT_FULL_PATH, url)
        return url

    s3config = S3Config(
        s3role,
        json.loads(s3sessionvars) if s3sessionvars else None,
        json.loads(s3clientparams) if s3clientparams else None,
    )

    urls = list(starmap(_make_url, _files()))
    ul_op = "upload"
    if not overwrite:
        ul_op = "info_upload"
    sz_results = process_urls(
        ul_op, urls, verbose, inject_failure, num_workers, s3config
    )
    retry_lines = []
    out_lines = []
    denied_url = None
    for url, sz in zip(urls, sz_results):
        # sz is None if the file wasn't uploaded (no overwrite), 0 if uploaded OK
        # or the error code if not (error code here will only be
        # ERROR_TRANSIENT or ERROR_URL_ACCESS_DENIED
        if sz is None:
            if listing:
                # We keep a position for it in our out list in case of retries
                out_lines.append("%d %s\n" % (url.idx, TRANSIENT_RETRY_LINE_CONTENT))
            continue
        elif listing and sz == 0:
            out_lines.append(format_result_line(url.idx, url.url) + "\n")
        elif sz == -ERROR_TRANSIENT:
            retry_lines.append(
                json.dumps(
                    {
                        "idx": url.idx,
                        "url": url.url,
                        "local": url.local,
                        "content_type": url.content_type,
                        "metadata": url.metadata,
                        "encryption": url.encryption,
                    }
                )
                + "\n"
            )
            # Output something to get a total count the first time around
            if not is_transient_retry:
                out_lines.append("%d %s\n" % (url.idx, TRANSIENT_RETRY_LINE_CONTENT))
        elif sz == -ERROR_URL_ACCESS_DENIED:
            # We do NOT break because we want to be able to accurately report all
            # the files uploaded after retries.
            denied_url = url
    if denied_url is not None:
        exit(ERROR_URL_ACCESS_DENIED, denied_url)

    if out_lines:
        sys.stdout.writelines(out_lines)
        sys.stdout.flush()

    if retry_lines:
        sys.stderr.write("%s\n" % TRANSIENT_RETRY_START_LINE)
        sys.stderr.writelines(retry_lines)
        sys.stderr.flush()
        sys.exit(ERROR_TRANSIENT)


def _populate_prefixes(prefixes, inputs):
    # Returns a tuple: first element is the prefix index, the second element is the
    # prefix and the third element is the optional range (or None if the entire prefix
    # is requested).
    # We again assume that the indices, if provided, are correct. This is again only
    # used for the transient error retry so users should not use this directly.
    is_transient_retry = False
    if prefixes:
        prefixes = [(idx, url_unquote(p), None) for idx, p in enumerate(prefixes)]
    else:
        prefixes = []
    if inputs:
        with open(inputs, mode="rb") as f:
            for idx, l in enumerate(f, start=len(prefixes)):
                s = l.split(b" ")
                if len(s) == 1:
                    url = url_unquote(s[0].strip())
                    prefixes.append((idx, url, url, None))
                elif len(s) == 2:
                    url = url_unquote(s[0].strip())
                    prefixes.append((idx, url, url, url_unquote(s[1].strip())))
                else:
                    is_transient_retry = True
                    if len(s) == 3:
                        prefix = url = url_unquote(s[1].strip())
                        range_info = url_unquote(s[2].strip())
                    else:
                        # Special case when we have both prefix and URL -- this is
                        # used in recursive gets for example
                        prefix = url_unquote(s[1].strip())
                        url = url_unquote(s[2].strip())
                        range_info = url_unquote(s[3].strip())
                    if range_info == "<norange>":
                        range_info = None
                    prefixes.append(
                        (int(url_unquote(s[0].strip())), prefix, url, range_info)
                    )
    return prefixes, is_transient_retry


@tracing.cli_entrypoint("s3op/get")
@cli.command(help="Download files from S3")
@click.option(
    "--recursive/--no-recursive",
    default=False,
    show_default=True,
    help="Download prefixes recursively.",
)
@click.option(
    "--verify/--no-verify",
    default=True,
    show_default=True,
    help="Verify that files were loaded correctly.",
)
@click.option(
    "--info/--no-info",
    default=True,
    show_default=True,
    help="Return user tags and content-type",
)
@click.option(
    "--allow-missing/--no-allow-missing",
    default=False,
    show_default=True,
    help="Do not exit if missing files are detected. Implies --verify.",
)
@common_options
@non_lst_common_options
@click.argument("prefixes", nargs=-1)
def get(
    prefixes,
    recursive=None,
    num_workers=None,
    inputs=None,
    verify=None,
    info=None,
    allow_missing=None,
    verbose=None,
    listing=None,
    s3role=None,
    s3sessionvars=None,
    s3clientparams=None,
    inject_failure=0,
):

    s3config = S3Config(
        s3role,
        json.loads(s3sessionvars) if s3sessionvars else None,
        json.loads(s3clientparams) if s3clientparams else None,
    )

    # Construct a list of URL (prefix) objects
    urllist = []
    to_iterate, is_transient_retry = _populate_prefixes(prefixes, inputs)
    for idx, prefix, url, r in to_iterate:
        src = urlparse(url)
        url = S3Url(
            url=url,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=generate_local_path(url, range=r),
            prefix=prefix,
            range=r,
            idx=idx,
        )
        if src.scheme != "s3":
            exit(ERROR_INVALID_URL, url)
        if not recursive and not src.path:
            exit(ERROR_NOT_FULL_PATH, url)
        urllist.append(url)
    # Construct a URL->size mapping and get content-type and metadata if needed
    op = None
    dl_op = "download"
    if recursive:
        op = partial(op_list_prefix, s3config)
    if verify or verbose or info:
        dl_op = "info_download"
    if op:
        if is_transient_retry:
            raise RuntimeError("--recursive not allowed for transient retries")
        urls = []
        # NOTE - we must retain the order of prefixes requested
        # and the listing order returned by S3
        for success, prefix_url, ret in parallel_op(op, urllist, num_workers):
            if success:
                urls.extend(ret)
            elif ret == ERROR_URL_NOT_FOUND and allow_missing:
                urls.append((prefix_url, None))
            else:
                exit(ret, prefix_url)
        # We re-index here since we may have pulled in a bunch more stuff. On a transient
        # retry, we never have recursive so we would not re-index
        for idx, (url, _) in enumerate(urls):
            url.idx = idx
    else:
        # pretend zero size since we don't need it for anything.
        # it can't be None though, to make sure the listing below
        # works correctly (None denotes a missing file)
        urls = [(prefix_url, 0) for prefix_url in urllist]

    # exclude the non-existent files from loading
    to_load = [url for url, size in urls if size is not None]
    sz_results = process_urls(
        dl_op, to_load, verbose, inject_failure, num_workers, s3config
    )
    # We check if there is any access denied
    retry_lines = []
    out_lines = []
    denied_url = None
    missing_url = None
    verify_info = []
    idx_in_sz = 0
    for url, _ in urls:
        sz = None
        # to_load contains an ordered subset of urls
        if idx_in_sz != len(to_load) and url.url == to_load[idx_in_sz].url:
            sz = sz_results[idx_in_sz]
            idx_in_sz += 1
        if listing and sz is None:
            out_lines.append(format_result_line(url.idx, url.url) + "\n")
        elif listing and sz >= 0:
            out_lines.append(
                format_result_line(url.idx, url.prefix, url.url, url.local) + "\n"
            )
            if verify:
                verify_info.append((url, sz))
        elif sz == -ERROR_URL_ACCESS_DENIED:
            denied_url = url
            break
        elif sz == -ERROR_URL_NOT_FOUND:
            if missing_url is None:
                missing_url = url
            if not allow_missing:
                break
            out_lines.append(format_result_line(url.idx, url.url) + "\n")
        elif sz == -ERROR_TRANSIENT:
            retry_lines.append(
                " ".join(
                    [
                        str(url.idx),
                        url_quote(url.prefix).decode(encoding="utf-8"),
                        url_quote(url.url).decode(encoding="utf-8"),
                        url_quote(url.range).decode(encoding="utf-8")
                        if url.range
                        else "<norange>",
                    ]
                )
                + "\n"
            )
            # First time around, we output something to indicate the total length
            if not is_transient_retry:
                out_lines.append("%d %s\n" % (url.idx, TRANSIENT_RETRY_LINE_CONTENT))

    if denied_url is not None:
        exit(ERROR_URL_ACCESS_DENIED, denied_url)

    if not allow_missing and missing_url is not None:
        exit(ERROR_URL_NOT_FOUND, missing_url)

    # Postprocess
    if verify:
        verify_results(verify_info, verbose=verbose)

    if out_lines:
        sys.stdout.writelines(out_lines)
        sys.stdout.flush()

    if retry_lines:
        sys.stderr.write("%s\n" % TRANSIENT_RETRY_START_LINE)
        sys.stderr.writelines(retry_lines)
        sys.stderr.flush()
        sys.exit(ERROR_TRANSIENT)


@cli.command(help="Get info about files from S3")
@common_options
@non_lst_common_options
@click.argument("prefixes", nargs=-1)
def info(
    prefixes,
    num_workers=None,
    inputs=None,
    verbose=None,
    listing=None,
    s3role=None,
    s3sessionvars=None,
    s3clientparams=None,
    inject_failure=0,
):

    s3config = S3Config(
        s3role,
        json.loads(s3sessionvars) if s3sessionvars else None,
        json.loads(s3clientparams) if s3clientparams else None,
    )

    # Construct a list of URL (prefix) objects
    urllist = []
    to_iterate, is_transient_retry = _populate_prefixes(prefixes, inputs)
    for idx, prefix, url, _ in to_iterate:
        src = urlparse(url)
        url = S3Url(
            url=url,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=generate_local_path(url, suffix="info"),
            prefix=prefix,
            range=None,
            idx=idx,
        )
        if src.scheme != "s3":
            exit(ERROR_INVALID_URL, url)
        urllist.append(url)

    sz_results = process_urls(
        "info", urllist, verbose, inject_failure, num_workers, s3config
    )

    retry_lines = []
    out_lines = []
    for idx, sz in enumerate(sz_results):
        url = urllist[idx]
        if listing and sz != -ERROR_TRANSIENT:
            out_lines.append(
                format_result_line(url.idx, url.prefix, url.url, url.local) + "\n"
            )
        else:
            retry_lines.append(
                "%d %s <norange>\n"
                % (url.idx, url_quote(url.url).decode(encoding="utf-8"))
            )
            if not is_transient_retry:
                out_lines.append("%d %s\n" % (url.idx, TRANSIENT_RETRY_LINE_CONTENT))

    if out_lines:
        sys.stdout.writelines(out_lines)
        sys.stdout.flush()

    if retry_lines:
        sys.stderr.write("%s\n" % TRANSIENT_RETRY_START_LINE)
        sys.stderr.writelines(retry_lines)
        sys.stderr.flush()
        sys.exit(ERROR_TRANSIENT)


if __name__ == "__main__":
    cli(auto_envvar_prefix="S3OP")
