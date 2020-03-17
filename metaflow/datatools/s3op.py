from __future__ import print_function

import time
import math
import string
import sys
import os
import traceback
from hashlib import sha1
from tempfile import NamedTemporaryFile
from multiprocessing import Process, Queue
from collections import namedtuple
from itertools import starmap, chain, islice

try:
    # python2
    from urlparse import urlparse
    from Queue import Full as QueueFull
except:
    # python3
    from urllib.parse import urlparse
    from queue import Full as QueueFull

import click

# s3op can be launched as a stand-alone script. We must set
# PYTHONPATH for the parent Metaflow explicitly.
sys.path.insert(0,\
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
# we use Metaflow's parallel_imap_unordered instead of
# multiprocessing.Pool because https://bugs.python.org/issue31886
from metaflow.util import url_quote, url_unquote
from metaflow.multicore_utils import parallel_map
from metaflow.datastore.util.s3util import aws_retry

NUM_WORKERS_DEFAULT = 64

S3Url = namedtuple('S3Url', ['bucket', 'path', 'url', 'local', 'prefix'])

# We use error codes instead of Exceptions, which are trickier to
# handle reliably in a multi-process world
ERROR_INVALID_URL = 4
ERROR_NOT_FULL_PATH = 5
ERROR_URL_NOT_FOUND = 6
ERROR_URL_ACCESS_DENIED = 7
ERROR_WORKER_EXCEPTION = 8
ERROR_VERIFY_FAILED = 9
ERROR_LOCAL_FILE_NOT_FOUND = 10

def format_triplet(prefix, url='', local=''):
    return u' '.join(url_quote(x).decode('utf-8') for x in (prefix, url, local))

# I can't understand what's the right way to deal
# with boto errors. This function can be replaced
# with better error handling code.
def normalize_client_error(err):
    error_code = err.response['Error']['Code']
    try:
        return int(error_code)
    except ValueError:
        if error_code == 'AccessDenied':
            return 403
    return error_code

# S3 worker pool

def worker(queue, mode):
    try:
        from metaflow.datastore.util.s3util import get_s3_client
        s3, _ = get_s3_client()
        while True:
            url = queue.get()
            if url is None:
                break

            if mode == 'download':
                tmp = NamedTemporaryFile(dir='.', delete=False)
                try:
                    s3.download_file(url.bucket, url.path, tmp.name)
                    os.rename(tmp.name, url.local)
                except:
                    # TODO specific error message for out of disk space
                    os.unlink(tmp.name)
                    raise
            else:
                s3.upload_file(url.local, url.bucket, url.path)

    except:
        traceback.print_exc()
        sys.exit(ERROR_WORKER_EXCEPTION)

def start_workers(mode, urls, num_workers):

    queue = Queue(len(urls) + num_workers)
    procs = {}

    # 1. push sources and destinations to the queue
    for url, _ in urls:
        queue.put(url)

    # 2. push end-of-queue markers
    for i in range(num_workers):
        queue.put(None)

    # 3. start processes
    for i in range(num_workers):
        p = Process(target=worker, args=(queue, mode))
        p.start()
        procs[p] = True

    # 4. wait for the processes to finish
    while any(procs.values()):
        for proc, is_alive in procs.items():
            if is_alive:
                proc.join(timeout=1)
                if proc.exitcode is not None:
                    if proc.exitcode == 0:
                        procs[proc] = False
                    else:
                        msg = 'Worker process failed (exit code %d)'\
                               % proc.exitcode
                        exit(msg, proc.exitcode)

def process_urls(mode, urls, verbose, num_workers):

    if verbose:
        print('%sing %d files..' % (mode.capitalize(), len(urls)),
              file=sys.stderr)

    start = time.time()
    start_workers(mode, urls, num_workers)
    end = time.time()

    if verbose:
        total_size = sum(size for url, size in urls)
        bw = total_size / (end - start)
        print('%sed %d files, %s in total, in %d seconds (%s/s).'\
              % (mode.capitalize(),
                 len(urls),
                 with_unit(total_size),
                 end - start,
                 with_unit(bw)),
              file=sys.stderr)

# Utility functions

def with_unit(x):
    if x > 1024**3:
        return '%.1fGB' % (x / 1024.**3)
    elif x > 1024**2:
        return '%.1fMB' % (x / 1024.**2)
    elif x > 1024:
        return '%.1fKB' % (x / 1024.)
    else:
        return '%d bytes' % x

# S3Ops class is just a wrapper for get_size and list_prefix
# required by @aws_retry decorator, which needs the reset_client
# method. Otherwise they would be just stand-alone functions.
class S3Ops(object):

    def __init__(self):
        self.s3 = None

    def reset_client(self, hard_reset=False):
        from metaflow.datastore.util.s3util import get_s3_client
        if hard_reset or self.s3 is None:
            self.s3, _ = get_s3_client()

    @aws_retry
    def get_size(self, url):
        self.reset_client()
        try:
            head = self.s3.head_object(Bucket=url.bucket, Key=url.path)
            return True, url, [(url, head['ContentLength'])]
        except ClientError as err:
            error_code = normalize_client_error(err)
            if error_code == 404:
                return False, url, ERROR_URL_NOT_FOUND
            elif error_code == 403:
                return False, url, ERROR_URL_ACCESS_DENIED
            else:
                raise

    @aws_retry
    def list_prefix(self, prefix_url, delimiter=''):
        self.reset_client()
        url_base = 's3://%s/' % prefix_url.bucket
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            urls = []
            for page in paginator.paginate(Bucket=prefix_url.bucket,
                                           Prefix=prefix_url.path,
                                           Delimiter=delimiter):
                # note that an url may be both a prefix and an object
                # - the trailing slash is significant in S3
                if 'Contents' in page:
                    for key in page.get('Contents', []):
                        url = url_base + key['Key']
                        urlobj = S3Url(url=url,
                                       bucket=prefix_url.bucket,
                                       path=key['Key'],
                                       local=generate_local_path(url),
                                       prefix=prefix_url.url)
                        urls.append((urlobj, key['Size']))
                if 'CommonPrefixes' in page:
                    # we get CommonPrefixes if Delimiter is a non-empty string
                    for key in page.get('CommonPrefixes', []):
                        url = url_base + key['Prefix']
                        urlobj = S3Url(url=url,
                                       bucket=prefix_url.bucket,
                                       path=key['Prefix'],
                                       local=None,
                                       prefix=prefix_url.url)
                        urls.append((urlobj, None))
            return True, prefix_url, urls
        except self.s3.exceptions.NoSuchBucket:
            return False, prefix_url, ERROR_URL_NOT_FOUND
        except ClientError as err:
            if err.response['Error']['Code'] == 'AccessDenied':
                return False, prefix_url, ERROR_URL_ACCESS_DENIED
            else:
                raise

# We want to reuse an s3 client instance over multiple operations.
# This is accomplished by op_ functions below.

def op_get_size(urls):
    s3 = S3Ops()
    return [s3.get_size(url) for url in urls]

def op_list_prefix(prefix_urls):
    s3 = S3Ops()
    return [s3.list_prefix(prefix) for prefix in prefix_urls]

def op_list_prefix_nonrecursive(prefix_urls):
    s3 = S3Ops()
    return [s3.list_prefix(prefix, delimiter='/') for prefix in prefix_urls]

def exit(exit_code, url):
    if exit_code == ERROR_INVALID_URL:
        msg = 'Invalid url: %s' % url.url
    elif exit_code == ERROR_NOT_FULL_PATH:
        msg = 'URL not a full path: %s' % url.url
    elif exit_code == ERROR_URL_NOT_FOUND:
        msg = 'URL not found: %s' % url.url
    elif exit_code == ERROR_URL_ACCESS_DENIED:
        msg = 'Access denied to URL: %s' % url.url
    elif exit_code == ERROR_WORKER_EXCEPTION:
        msg = 'Download failed'
    elif exit_code == ERROR_VERIFY_FAILED:
        msg = 'Verification failed for URL %s, local file %s'\
              % (url.url, url.local)
    elif exit_code == ERROR_LOCAL_FILE_NOT_FOUND:
        msg = 'Local file not found: %s' % url
    else:
        msg = 'Unknown error'
    print('s3op failed:\n%s' % msg, file=sys.stderr)
    sys.exit(exit_code)

def verify_results(urls, verbose=False):
    for url, expected in urls:
        if verbose:
            print('verifying %s, expected %s' % (url, expected),
                  file=sys.stderr)
        try:
            got = os.stat(url.local).st_size
        except OSError:
            raise
            exit(ERROR_VERIFY_FAILED, url)
        if expected != got:
            exit(ERROR_VERIFY_FAILED, url)

def generate_local_path(url):
    # this function generates a safe local file name corresponding to
    # an S3 URL. URLs may be longer than maximum file length limit on Linux,
    # so we mostly hash the URL but retain the leaf part as a convenience
    # feature to ease eyeballing
    quoted = url_quote(url)
    fname = quoted.split(b'/')[-1].replace(b'.', b'_').replace(b'-', b'_')
    sha = sha1(quoted).hexdigest()
    return u'-'.join((sha, fname.decode('utf-8')))

def parallel_op(op, lst, num_workers):
    # parallel op divides work equally amongst num_workers
    # processes. This is a good strategy if the cost is
    # uniform over the units of work, e.g. op_get_size, which
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

@cli.command('list', help='List S3 objects')
@click.option('--inputs',
              type=click.Path(exists=True),
              help='Read input prefixes from the given file.')
@click.option('--num-workers',
              default=NUM_WORKERS_DEFAULT,
              show_default=True,
              help='Number of concurrent connections.')
@click.option('--recursive/--no-recursive',
              default=False,
              show_default=True,
              help='Download prefixes recursively.')
@click.argument('prefixes', nargs=-1)
def lst(prefixes,
        inputs=None,
        num_workers=None,
        recursive=None):

    urllist = []
    for prefix in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(url=prefix,
                    bucket=src.netloc,
                    path=src.path.lstrip('/'),
                    local=None,
                    prefix=prefix)
        if src.scheme != 's3':
            exit(ERROR_INVALID_URL, url)
        urllist.append(url)

    op = op_list_prefix if recursive else op_list_prefix_nonrecursive
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

@cli.command(help='Upload files to S3')
@click.option('--file',
              'files',
              type=(click.Path(exists=True), str),
              multiple=True,
              help='Local file->S3Url pair to upload. '
                   'Can be specified multiple times.')
@click.option('--filelist',
              type=click.Path(exists=True),
              help='Read local file -> S3 URL mappings from the given file.')
@click.option('--num-workers',
              default=NUM_WORKERS_DEFAULT,
              show_default=True,
              help='Number of concurrent connections.')
@click.option('--verbose/--no-verbose',
              default=True,
              show_default=True,
              help='Print status information on stderr.')
@click.option('--overwrite/--no-overwrite',
              default=True,
              show_default=True,
              help='Overwrite key if it already exists in S3.')
@click.option('--listing/--no-listing',
              default=False,
              show_default=True,
              help='Print S3 URLs upload to on stdout.')
def put(files=None,
        filelist=None,
        num_workers=None,
        verbose=None,
        overwrite=True,
        listing=None):

    def _files():
        for local, url in files:
            yield url_unquote(local), url_unquote(url)
        if filelist:
            for line in open(filelist, mode='rb'):
                local, url = map(url_unquote, line.split())
                if not os.path.exists(local):
                    exit(ERROR_LOCAL_FILE_NOT_FOUND, local)
                yield local, url

    def _make_url(local, user_url):
        src = urlparse(user_url)
        url = S3Url(url=user_url,
                    bucket=src.netloc,
                    path=src.path.lstrip('/'),
                    local=local,
                    prefix=None)
        if src.scheme != 's3':
            exit(ERROR_INVALID_URL, url)
        if not src.path:
            exit(ERROR_NOT_FULL_PATH, url)
        return url, os.stat(local).st_size

    urls = list(starmap(_make_url, _files()))
    if not overwrite:
        new_urls = set()
        for success, prefix_url, ret in parallel_op(op_get_size, list(list(zip(*urls))[0]), num_workers):
            if ret == ERROR_URL_NOT_FOUND:
                new_urls.add(prefix_url)
        urls = [(url, size) for url, size in urls if url in new_urls]
    process_urls('upload', urls, verbose, num_workers)
    if listing:
        for url, _ in urls:
            print(format_triplet(url.url))

def _populate_prefixes(prefixes, inputs):
    if not prefixes:
        prefixes = []
    if inputs:
        with open(inputs, mode='rb') as f:
            prefixes.extend(l.strip() for l in f)
    return list(map(url_unquote, prefixes))

@cli.command(help='Download files from S3')
@click.option('--recursive/--no-recursive',
              default=False,
              show_default=True,
              help='Download prefixes recursively.')
@click.option('--num-workers',
              default=NUM_WORKERS_DEFAULT,
              show_default=True,
              help='Number of concurrent connections.')
@click.option('--inputs',
              type=click.Path(exists=True),
              help='Read input prefixes from the given file.')
@click.option('--verify/--no-verify',
              default=True,
              show_default=True,
              help='Verify that files were loaded correctly.')
@click.option('--allow-missing/--no-allow-missing',
              default=False,
              show_default=True,
              help='Do not exit if missing files are detected. '\
                   'Implies --verify.')
@click.option('--verbose/--no-verbose',
              default=True,
              show_default=True,
              help='Print status information on stderr.')
@click.option('--listing/--no-listing',
              default=False,
              show_default=True,
              help='Print S3 URL -> local file mapping on stdout.')
@click.argument('prefixes', nargs=-1)
def get(prefixes,
        recursive=None,
        num_workers=None,
        inputs=None,
        verify=None,
        allow_missing=None,
        verbose=None,
        listing=None):

    if allow_missing:
        verify = True

    # Construct a list of URL (prefix) objects
    urllist = []
    for prefix in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(url=prefix,
                    bucket=src.netloc,
                    path=src.path.lstrip('/'),
                    local=generate_local_path(prefix),
                    prefix=prefix)
        if src.scheme != 's3':
            exit(ERROR_INVALID_URL, url)
        if not recursive and not src.path:
            exit(ERROR_NOT_FULL_PATH, url)
        urllist.append(url)

    # Construct a url->size mapping
    op = None
    if recursive:
        op = op_list_prefix
    elif verify or verbose:
        op = op_get_size
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
    to_load = [(url, size) for url, size in urls if size is not None]
    process_urls('download', to_load, verbose, num_workers)
    # Postprocess
    if verify:
        verify_results(to_load, verbose=verbose)

    if listing:
        for url, size in urls:
            if size is None:
                print(format_triplet(url.url))
            else:
                print(format_triplet(url.prefix, url.url, url.local))

if __name__ == '__main__':
    from botocore.exceptions import ClientError
    cli(auto_envvar_prefix='S3OP')
