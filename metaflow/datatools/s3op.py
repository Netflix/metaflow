from __future__ import print_function

import json
import time
import math
import re
import sys
import os
import traceback
from collections import namedtuple
from functools import partial
from hashlib import sha1
from tempfile import NamedTemporaryFile
from multiprocessing import Process, Queue
from itertools import starmap, chain, islice

from boto3.s3.transfer import TransferConfig

try:
    # python2
    from urlparse import urlparse
    from Queue import Full as QueueFull
except:
    # python3
    from urllib.parse import urlparse
    from queue import Full as QueueFull

if __name__ == "__main__":
    # When launched standalone, point to our parent metaflow
    sys.path.insert(
        0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    )

from metaflow._vendor import click

# we use Metaflow's parallel_imap_unordered instead of
# multiprocessing.Pool because https://bugs.python.org/issue31886
from metaflow.util import TempDir, url_quote, url_unquote
from metaflow.multicore_utils import parallel_map
from metaflow.datatools.s3util import aws_retry, read_in_chunks, get_timestamp

NUM_WORKERS_DEFAULT = 64

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
        metadata=None,
        range=None,
    ):

        self.bucket = bucket
        self.path = path
        self.url = url
        self.local = local
        self.prefix = prefix
        self.content_type = content_type
        self.metadata = metadata
        self.range = range

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


def format_triplet(prefix, url="", local=""):
    return " ".join(url_quote(x).decode("utf-8") for x in (prefix, url, local))


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
    return error_code


# S3 worker pool


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
            else:
                to_return = {"error": error_code, "raise_error": err}
        return to_return

    with open(result_file_name, "w") as result_file:
        try:
            from metaflow.datatools.s3util import get_s3_client

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
                        if url.content_type or url.metadata:
                            extra = {}
                            if url.content_type:
                                extra["ContentType"] = url.content_type
                            if url.metadata is not None:
                                extra["Metadata"] = url.metadata
                        s3.upload_file(url.local, url.bucket, url.path, ExtraArgs=extra)
                        # We indicate that the file was uploaded
                        result_file.write("%d %d\n" % (idx, 0))
        except:
            traceback.print_exc()
            sys.exit(ERROR_WORKER_EXCEPTION)


def start_workers(mode, urls, num_workers, s3config):
    # We start the minimum of len(urls) or num_workers to avoid starting
    # workers that will definitely do nothing
    num_workers = min(num_workers, len(urls))
    queue = Queue(len(urls) + num_workers)
    procs = {}

    # 1. push sources and destinations to the queue
    for idx, elt in enumerate(urls):
        queue.put((elt, idx))

    # 2. push end-of-queue markers
    for i in range(num_workers):
        queue.put((None, None))

    # 3. Prepare the result structure
    sz_results = [None] * len(urls)

    # 4. start processes
    with TempDir() as output_dir:
        for i in range(num_workers):
            file_path = os.path.join(output_dir, str(i))
            p = Process(
                target=worker,
                args=(file_path, queue, mode, s3config),
            )
            p.start()
            procs[p] = file_path

        # 5. wait for the processes to finish; we continuously update procs
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


def process_urls(mode, urls, verbose, num_workers, s3config):

    if verbose:
        print("%sing %d files.." % (mode.capitalize(), len(urls)), file=sys.stderr)

    start = time.time()
    sz_results = start_workers(mode, urls, num_workers, s3config)
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
        from metaflow.datatools.s3util import get_s3_client

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
            if err.response["Error"]["Code"] in ("AccessDenied", "AllAccessDisabled"):
                return False, prefix_url, ERROR_URL_ACCESS_DENIED
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
            exit(ERROR_VERIFY_FAILED, url)
        if expected != got:
            exit(ERROR_VERIFY_FAILED, url)
        if url.content_type or url.metadata:
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


@click.group()
def cli():
    pass


@cli.command("list", help="List S3 objects")
@click.option(
    "--inputs",
    type=click.Path(exists=True),
    help="Read input prefixes from the given file.",
)
@click.option(
    "--num-workers",
    default=NUM_WORKERS_DEFAULT,
    show_default=True,
    help="Number of concurrent connections.",
)
@click.option(
    "--recursive/--no-recursive",
    default=False,
    show_default=True,
    help="Download prefixes recursively.",
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
    for prefix, _ in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(
            url=prefix,
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

    for url, size in urls:
        if size is None:
            print(format_triplet(url.prefix, url.url))
        else:
            print(format_triplet(url.prefix, url.url, str(size)))


@cli.command(help="Upload files to S3")
@click.option(
    "--file",
    "files",
    type=(click.Path(exists=True), str),
    multiple=True,
    help="Local file->S3Url pair to upload. " "Can be specified multiple times.",
)
@click.option(
    "--filelist",
    type=click.Path(exists=True),
    help="Read local file -> S3 URL mappings from the given file.",
)
@click.option(
    "--num-workers",
    default=NUM_WORKERS_DEFAULT,
    show_default=True,
    help="Number of concurrent connections.",
)
@click.option(
    "--verbose/--no-verbose",
    default=True,
    show_default=True,
    help="Print status information on stderr.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=True,
    show_default=True,
    help="Overwrite key if it already exists in S3.",
)
@click.option(
    "--listing/--no-listing",
    default=False,
    show_default=True,
    help="Print S3 URLs upload to on stdout.",
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
def put(
    files=None,
    filelist=None,
    num_workers=None,
    verbose=None,
    overwrite=True,
    listing=None,
    s3role=None,
    s3sessionvars=None,
    s3clientparams=None,
):
    def _files():
        for local, url in files:
            yield url_unquote(local), url_unquote(url), None, None
        if filelist:
            for line in open(filelist, mode="rb"):
                r = json.loads(line)
                local = r["local"]
                url = r["url"]
                content_type = r.get("content_type", None)
                metadata = r.get("metadata", None)
                if not os.path.exists(local):
                    exit(ERROR_LOCAL_FILE_NOT_FOUND, local)
                yield local, url, content_type, metadata

    def _make_url(local, user_url, content_type, metadata):
        src = urlparse(user_url)
        url = S3Url(
            url=user_url,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=local,
            prefix=None,
            content_type=content_type,
            metadata=metadata,
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
    sz_results = process_urls(ul_op, urls, verbose, num_workers, s3config)
    urls = [url for url, sz in zip(urls, sz_results) if sz is not None]
    if listing:
        for url in urls:
            print(format_triplet(url.url))


def _populate_prefixes(prefixes, inputs):
    # Returns a tuple: first element is the prefix and second element
    # is the optional range (or None if the entire prefix is requested)
    if prefixes:
        prefixes = [(url_unquote(p), None) for p in prefixes]
    else:
        prefixes = []
    if inputs:
        with open(inputs, mode="rb") as f:
            for l in f:
                s = l.split(b" ")
                if len(s) > 1:
                    prefixes.append(
                        (url_unquote(s[0].strip()), url_unquote(s[1].strip()))
                    )
                else:
                    prefixes.append((url_unquote(s[0].strip()), None))
    return prefixes


@cli.command(help="Download files from S3")
@click.option(
    "--recursive/--no-recursive",
    default=False,
    show_default=True,
    help="Download prefixes recursively.",
)
@click.option(
    "--num-workers",
    default=NUM_WORKERS_DEFAULT,
    show_default=True,
    help="Number of concurrent connections.",
)
@click.option(
    "--inputs",
    type=click.Path(exists=True),
    help="Read input prefixes from the given file.",
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
    help="Do not exit if missing files are detected. " "Implies --verify.",
)
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
):

    s3config = S3Config(
        s3role,
        json.loads(s3sessionvars) if s3sessionvars else None,
        json.loads(s3clientparams) if s3clientparams else None,
    )

    # Construct a list of URL (prefix) objects
    urllist = []
    for prefix, r in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(
            url=prefix,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=generate_local_path(prefix, range=r),
            prefix=prefix,
            range=r,
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
    else:
        # pretend zero size since we don't need it for anything.
        # it can't be None though, to make sure the listing below
        # works correctly (None denotes a missing file)
        urls = [(prefix_url, 0) for prefix_url in urllist]

    # exclude the non-existent files from loading
    to_load = [url for url, size in urls if size is not None]
    sz_results = process_urls(dl_op, to_load, verbose, num_workers, s3config)
    # We check if there is any access denied
    is_denied = [sz == -ERROR_URL_ACCESS_DENIED for sz in sz_results]
    if any(is_denied):
        # Find the first one to return that as an error
        for i, b in enumerate(is_denied):
            if b:
                exit(ERROR_URL_ACCESS_DENIED, to_load[i])
    if not allow_missing:
        is_missing = [sz == -ERROR_URL_NOT_FOUND for sz in sz_results]
        if any(is_missing):
            # Find the first one to return that as an error
            for i, b in enumerate(is_missing):
                if b:
                    exit(ERROR_URL_NOT_FOUND, to_load[i])
    # Postprocess
    if verify:
        # Verify only results with an actual size (so actual files)
        verify_results(
            [
                (url, sz)
                for url, sz in zip(to_load, sz_results)
                if sz != -ERROR_URL_NOT_FOUND
            ],
            verbose=verbose,
        )

    idx_in_sz = 0
    if listing:
        for url, _ in urls:
            sz = None
            if idx_in_sz != len(to_load) and url.url == to_load[idx_in_sz].url:
                sz = sz_results[idx_in_sz] if sz_results[idx_in_sz] >= 0 else None
                idx_in_sz += 1
            if sz is None:
                # This means that either the initial url had a None size or
                # that after loading, we found a None size
                print(format_triplet(url.url))
            else:
                print(format_triplet(url.prefix, url.url, url.local))


@cli.command(help="Get info about files from S3")
@click.option(
    "--num-workers",
    default=NUM_WORKERS_DEFAULT,
    show_default=True,
    help="Number of concurrent connections.",
)
@click.option(
    "--inputs",
    type=click.Path(exists=True),
    help="Read input prefixes from the given file.",
)
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
):

    s3config = S3Config(
        s3role,
        json.loads(s3sessionvars) if s3sessionvars else None,
        json.loads(s3clientparams) if s3clientparams else None,
    )

    # Construct a list of URL (prefix) objects
    urllist = []
    for prefix, _ in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(
            url=prefix,
            bucket=src.netloc,
            path=src.path.lstrip("/"),
            local=generate_local_path(prefix, suffix="info"),
            prefix=prefix,
            range=None,
        )
        if src.scheme != "s3":
            exit(ERROR_INVALID_URL, url)
        urllist.append(url)

    process_urls("info", urllist, verbose, num_workers, s3config)

    if listing:
        for url in urllist:
            print(format_triplet(url.prefix, url.url, url.local))


if __name__ == "__main__":
    cli(auto_envvar_prefix="S3OP")
