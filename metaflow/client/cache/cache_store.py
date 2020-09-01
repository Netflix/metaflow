import os
import time
import shutil
import hashlib
import tempfile

from collections import OrderedDict

TIMESTAMP_FOR_DELETABLE = 1
TIMESTAMP_FOR_DISPOSABLE = 10
GC_MARKER_QUARANTINE = 60

class CacheFullException(Exception):
    pass

def makedirs(path):
    # This is for python2 compatibility.
    # Python3 has os.makedirs(exist_ok=True).
    try:
        os.makedirs(path)
    except OSError as x:
        if x.errno == 17:
            return
        else:
            raise

def key_filename(key):
    return hashlib.sha1(key.encode('utf-8')).hexdigest()

def object_path(root, key):
    hsh = key_filename(key)
    prefix = hsh[:2]
    return os.path.join(root, prefix, hsh)

def stream_path(root, key):
    return object_path(root, key) + '.stream'

def is_safely_readable(path):
    try:
        tstamp = os.stat(path).st_mtime
        return tstamp != TIMESTAMP_FOR_DELETABLE
    except:
        return False

def filesize(path):
    return os.stat(path).st_size

class CacheStore(object):

    def __init__(self, root, max_size, echo, fill_factor=0.8):
        self.root = os.path.abspath(root)
        self.tmproot = self._init_temp(self.root)
        self.echo = echo
        self.active_streams = {}

        # GC state
        self.objects_queue = OrderedDict()
        self.disposables_queue = OrderedDict()
        self.gc_queue = OrderedDict()

        self.total_size = 0
        self.max_size = max_size
        self.gc_watermark = max_size * fill_factor

        self._init_gc(self.root)

    def object_path(self, key):
        return object_path(self.root, key)

    def warn(self, ex, msg):
        self.echo("IO ERROR: (%s) %s" % (ex, msg))

    def _init_temp(self, root):
        tmproot = os.path.join(root, 'tmp')
        if os.path.exists(tmproot):
            self.safe_fileop(shutil.rmtree, tmproot)
        makedirs(tmproot)
        return tmproot

    def _init_gc(self, root):
        objects = []
        for dirr, dirs, files in os.walk(root):
            for fname in files:
                path = os.path.join(dirr, fname)
                if os.path.islink(path):
                    self.safe_fileop(os.unlink, path)
                else:
                    stat_res = self.safe_fileop(os.stat, path)
                    if stat_res:
                        _, info = stat_res
                        if info.st_mtime == TIMESTAMP_FOR_DELETABLE:
                            self.safe_fileop(os.unlink, path)
                        elif info.st_mtime == TIMESTAMP_FOR_DISPOSABLE:
                            self.disposables_queue[path] = info.st_size
                        else:
                            objects.append((info.st_mtime, (path, info.st_size)))

        self.objects_queue.update(x for _, x in sorted(objects))
        self.total_size = sum(self.disposables_queue.values()) +\
                          sum(self.objects_queue.values())

        # It is possible that the datastore contains more than gc_watermark
        # bytes. To ensure that we start below the gc_watermark, we run the GC:
        self._gc_objects()
        # the above method may have marked objects for deletion. We assume that
        # no clients are accessing the objects since the server hasn't started
        # yet, so we can safely delete the marked objects without a quarantine:
        self._gc_objects(quarantine=-1)

        self.echo("Cache initialized with %d permanents objects, "\
                  "%d disposable objects, totaling %d bytes."\
                  % (len(self.objects_queue),
                     len(self.disposables_queue),
                     self.total_size))

    def _gc_objects(self, quarantine=GC_MARKER_QUARANTINE):

        def mark_for_deletion(path, size):
            self.safe_fileop(os.utime, path, (TIMESTAMP_FOR_DELETABLE,
                                              TIMESTAMP_FOR_DELETABLE))
            self.gc_queue[path] = (time.time(), size)

        # 1) delete marked objects that are past their quarantine period
        limit = time.time() - quarantine
        deleted = []
        for path in list(self.gc_queue):
            tstamp, size = self.gc_queue[path]
            if tstamp < limit:
                if self.safe_fileop(os.unlink, path):
                    del self.gc_queue[path]
                    self.total_size -= size
            else:
                break

        # if there are still objects marked for deletion, we can just wait
        # for them to age past quarantine. Without this check, we could GC
        # objects too eagerly during the quarantine period.
        if not self.gc_queue:

            # 2) if we are still using too much space, mark disposable
            # objects for deletion
            unmarked_size = self.total_size
            while self.disposables_queue and unmarked_size > self.gc_watermark:
                path, size = self.disposables_queue.popitem(last=False)
                mark_for_deletion(path, size)
                unmarked_size -= size

            # 3) after we have exhausted all disposables, we need to start
            # marking non-disposable objects for deletion, starting from the
            # oldest.
            while self.objects_queue and unmarked_size > self.gc_watermark:
                path, size = self.objects_queue.popitem(last=False)
                mark_for_deletion(path, size)
                unmarked_size -= size

    def ensure_path(self, path):
        dirr = os.path.dirname(path)
        if not os.path.isdir(dirr):
            try:
                makedirs(dirr)
            except Exception as ex:
                self.warn(ex, "Could not create dir: %s" % path)

    def open_tempdir(self, token, action_name, stream_key):
        self._gc_objects()
        if self.total_size > self.max_size:
            raise CacheFullException("Cache full! Used %d bytes, max %s bytes"\
                                     % (self.total_size, self.max_size))
        else:
            try:
                tmp = tempfile.mkdtemp(prefix='cache_action_%s.' % token,
                                       dir=self.tmproot)
            except Exception as ex:
                msg = "Could not create a temp directory for request %s" % token
                self.warn(ex, msg)
            else:
                if stream_key:
                    src = stream_path(self.root, stream_key)
                    dst = os.path.join(tmp, key_filename(stream_key))
                    # make sure that the new symlink points at a valid (empty!)
                    # file by creating a dummy destination file
                    self.ensure_path(src)
                    open_res = self.safe_fileop(open, dst, 'w')
                    if open_res:
                        _, f = open_res
                        f.close()
                        try:
                            os.symlink(dst, src)
                        except Exception as ex:
                            # two actions may be streaming the same object
                            # simultaneously. We don't consider an existing
                            # symlink (errno 17) to be an error.
                            if ex.errno != 17:
                                err = "Could not create a symlink %s->%s"\
                                      % (src, dst)
                                self.warn(ex, err)
                        else:
                            self.active_streams[tmp] = src
                        return tmp
                else:
                    return tmp

    def safe_fileop(self, fun, *args, **kwargs):
        try:
            return True, fun(*args, **kwargs)
        except Exception as ex:
            self.warn(ex, "File operation %s(%s) failed" % (fun.__name__, args))
            return False

    def close_tempdir(self, tempdir):
        stream = self.active_streams.pop(tempdir, None)
        if stream:
            self.safe_fileop(os.unlink, stream)
        self.safe_fileop(shutil.rmtree, tempdir)

    def commit(self, tempdir, keys, stream_key, disposable_keys):

        def _insert(queue, key, value):
            # we want to update the object's location in the
            # OrderedDict, so we will need to delete any possible
            # previous entry first
            queue.pop(key, None)
            queue[key] = value

        disposables = frozenset(disposable_keys)
        missing = []

        # note that by including stream_key in the commit list we
        # will invalidate the corresponding symlink that points at
        # a file in the temp directory. The dangling symlink will get
        # removed in close_tempdir.
        #
        # The cache client is expected to fall back to the object_path
        # when it detects an invalid symlink.
        for key in keys + ([stream_key] if stream_key else []):
            src = os.path.join(tempdir, key_filename(key))
            if os.path.exists(src):
                dst = object_path(self.root, key)
                self.ensure_path(dst)
                stat_res = self.safe_fileop(os.stat, src)
                if stat_res and self.safe_fileop(os.rename, src, dst):
                    _, info = stat_res
                    size = info.st_size
                    if key in disposables:
                        # we proceed even if we fail to mark the object as
                        # disposable. It just means that during a possible
                        # restart the object is treated as a non-disposable
                        # object
                        tstamp = TIMESTAMP_FOR_DISPOSABLE
                        self.safe_fileop(os.utime, dst, (tstamp, tstamp))
                        _insert(self.disposables_queue, dst, size)
                    else:
                        _insert(self.objects_queue, dst, size)
                    self.total_size += size
            else:
                missing.append(key)

        self._gc_objects()
        return missing

