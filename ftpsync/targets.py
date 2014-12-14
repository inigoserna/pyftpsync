# -*- coding: iso-8859-1 -*-
"""
(c) 2012-2014 Martin Wendt; see https://github.com/mar10/pyftpsync
Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
"""

from __future__ import print_function

from datetime import datetime
import io
import os
from posixpath import join as join_url, normpath as normurl
import shutil
import sys
import json
import time
from ftpsync._version import __version__


try:
    from urllib.parse import urlparse
except ImportError:
    # Python 2
    from urlparse import urlparse



DEFAULT_CREDENTIAL_STORE = "pyftpsync.pw"
DRY_RUN_PREFIX = "(DRY-RUN) "
IS_REDIRECTED = (os.fstat(0) != os.fstat(1))



def get_stored_credentials(filename, url):
    """Parse a file in the user's home directory, formatted like:
    
    URL = user:password
    """
    home_path = os.path.expanduser("~")
    file_path = os.path.join(home_path, filename)
    if os.path.isfile(file_path):
        with open(file_path, "rt") as f:
            for line in f:
                line = line.strip()
                if not "=" in line or line.startswith("#") or line.startswith(";"):
                    continue
                u, creds = line.split("=", 1)
                if not creds or u.strip().lower() != url:
                    continue
                creds = creds.strip()
                return creds.split(":", 1)
    return None




#===============================================================================
# make_target
#===============================================================================
def make_target(url, connect=True, debug=1, allow_stored_credentials=True):
    """Factory that creates _Target obejcts from URLs."""
    parts = urlparse(url, allow_fragments=False)
    # scheme is case-insensitive according to http://tools.ietf.org/html/rfc3986
    if parts.scheme.lower() == "ftp":
        creds = parts.username, parts.password
        if not parts.username and allow_stored_credentials:
            sc = get_stored_credentials(DEFAULT_CREDENTIAL_STORE, parts.netloc)
            if sc:
                creds = sc
        from ftpsync import ftp_target
        target = ftp_target.FtpTarget(parts.path, parts.hostname, 
                                      creds[0], creds[1], connect, debug)
    else:
        target = FsTarget(url)

    return target


def to_binary(s):
    """Convert unicode (text strings) to binary data on Python 2 and 3."""
    if sys.version_info[0] < 3:
        # Python 2
        if type(s) is not str:
            s = s.encode("utf8") 
    elif type(s) is str:
        # Python 3
        s = bytes(s, "utf8")
    return s 
    
#def to_text(s):
#    """Convert binary data to unicode (text strings) on Python 2 and 3."""
#    if sys.version_info[0] < 3:
#        # Python 2
#        if type(s) is not str:
#            s = s.encode("utf8") 
#    elif type(s) is str:
#        # Python 3
#        s = bytes(s, "utf8")
#    return s 
    
#===============================================================================
# LogginFileWrapper
# Wrapper around a file for writing to write a hash sign every block.
#===============================================================================
#class LoggingFileWrapper(object):
#    def __init__(self, fp, callback=None):
#        self.fp = fp
#        self.callback = callback or self.default_callback
#        self.bytes = 0
#    
#    def __enter__(self):
#        return self
#
#    def __exit__(self, type, value, tb):
#        self.close()
#
#    @staticmethod
#    def default_callback(wrapper, data):
#        print("#", end="")
#        sys.stdout.flush()
#        
#    def write(self, data):
#        self.bytes += len(data)
#        self.fp.write(data)
#        self.callback(self, data)
#    
#    def close(self):
#        self.fp.close()


#===============================================================================
# DirMetadata
#===============================================================================
class DirMetadata(object):
    
    META_FILE_NAME = "_pyftpsync-meta.json"
#     SNAPSHOT_FILE_NAME = "_pyftpsync-snap-%(remote)s.json"
    VERBOSE = True # Reduce meta file size to 35% (3759 -> 1375 bytes)
    
    def __init__(self, target):
        self.target = target
        self.list = {}
        self.dir = {"files": self.list}
        self.filename = self.META_FILE_NAME
        self.modified_list = False
        self.modified_sync = False
#        self.readonly = False
        self.was_read = False
        
    def set_mtime(self, filename, mtime, size):
        self.list[filename] = {#"name": filename,
                               "size": size,
#                                "uploaded": time.mktime(time.gmtime()),
                               "uploaded": time.time(), # UTC time stamp
                               "mtime": mtime}
        if self.VERBOSE:
            self.list[filename].update({
                "uploaded_str": time.ctime(),
                "mtime_str": time.ctime(mtime),
                })
        self.modified_list = True
    
    def set_last_sync(self, peer_target):
        """Store time when this target was last synced with remote."""
        ls = self.dir.setdefault("last_sync", {})
        ls[peer_target.get_id()] = {"sync": time.time(), "sync_str": time.ctime()}
        self.modified_sync = True
        
    def get_last_sync(self, peer_target):
        """."""
        try:
            return self.dir["last_sync"][peer_target.get_id()]["sync"]
        except KeyError:
            return None
        
    def remove(self, filename):
        self.list.pop(filename, None)
        self.modified_list = True

    def match_filename(self, filename):
        return filename == self.filename
    
    @staticmethod
    def is_meta_filename(filename):
        return filename.startswith("_pyftpsync-") and filename.endswith(".json")
    
    def read(self):
        try:
            s = self.target.read_text(self.filename)
            self.was_read = True # True, if exists (even invalid)
            self.dir = json.loads(s)
            self.list = self.dir["files"]
            self.modified_list = False
            self.modified_sync = False
#              print("DirMetadata: read(%s)" % (self.filename, ), self.dir)
        except Exception as e:
            print("Could not read meta info: %s" % e, file=sys.stderr)

    def flush(self):
        # We DO write even on read-only targets, but not in dry-run mode
#         if self.target.readonly:
#             print("DirMetadata.flush(%s): read-only; nothing to do" % self.target)
#             return
        if self.target.dry_run:
            print("DirMetadata.flush(%s): dry-run; nothing to do" % self.target)
            return
#        print("DirMetadata.flush(%s), %s" % (self.filename, self.target))
        if len(self.list) > 0 or self.modified_sync:
            if not self.modified_list and not self.modified_sync:
                print("DirMetadata.flush(%s): unmodified; nothing to do" % self.target)
                return
            self.dir["_disclaimer"] = "Generated by https://github.com/mar10/pyftpsync"
            self.dir["_time_str"] = "%s" % time.ctime()
            self.dir["_version"] = __version__
            self.dir["_time"] = time.mktime(time.gmtime())
            if self.VERBOSE:
                s = json.dumps(self.dir, indent=4, sort_keys=True)
            else:
                s = json.dumps(self.dir)
            print("DirMetadata.flush(%s)" % (self.target, ))#, s)
            self.target.write_text(self.filename, s)
        elif self.was_read:
            print("DirMetadata.flush(%s): DELETE" % self.target)
            self.target.remove_file(self.filename)
        else:
            print("DirMetadata.flush(%s): nothing to do" % self.target)

        self.modified_list = False
        self.modified_sync = False


#===============================================================================
# _Resource
#===============================================================================

class _Resource(object):
    def __init__(self, target, rel_path, name, size, mtime, unique):
        """
        
        @param target
        @param rel_path
        @param name base name
        @param size file size in bytes
        @param mtime modification time as UTC stamp
        @param uniqe string
        """
        self.target = target
        self.rel_path = rel_path
        self.name = name
        self.size = size
        self.mtime = mtime 
        self.dt_modified = datetime.fromtimestamp(self.mtime)
        self.unique = unique
        self.meta = None

    def __str__(self):
        return "%s('%s', size:%s, modified:%s)" % (self.__class__.__name__, 
                                                   os.path.join(self.rel_path, self.name), 
                                                   self.size, self.dt_modified) #+ " ## %s, %s" % (self.mtime, time.asctime(time.gmtime(self.mtime)))

    def __eq__(self, other):
        raise NotImplementedError

    def get_rel_path(self):
        return normurl(join_url(self.rel_path, self.name))
    
    def is_file(self):
        return False
    
    def is_dir(self):
        return False


#===============================================================================
# FileEntry
#===============================================================================
class FileEntry(_Resource):
    EPS_TIME = 0.1 # 2 seconds difference is considered equal
    
    def __init__(self, target, rel_path, name, size, mtime, unique):
        super(FileEntry, self).__init__(target, rel_path, name, size, mtime, unique)

    @staticmethod
    def _eps_compare(date_1, date_2):
        res = date_1 - date_2
        if abs(res) <= FileEntry.EPS_TIME: # '<=',so eps == 0 works as expected
#             print("DTC: %s, %s => %s" % (date_1, date_2, res))
            return 0
        elif res < 0:
            return -1
        return 1
        
    def __eq__(self, other):
#        if other.get_adjusted_mtime() == self.get_adjusted_mtime() and other.mtime != self.mtime:
#            print("*** Adjusted time match", self, other)
        same_time = self._eps_compare(self.get_adjusted_mtime(), other.get_adjusted_mtime()) == 0
        return (other and other.__class__ == self.__class__ 
                and other.name == self.name and other.size == self.size 
                and same_time)

    def __gt__(self, other):
        time_greater = self._eps_compare(self.get_adjusted_mtime(), other.get_adjusted_mtime()) > 0
        return (other and other.__class__ == self.__class__ 
                and other.name == self.name 
                and time_greater)

    def get_adjusted_mtime(self):
        try:
            res = float(self.meta["mtime"])
#            print("META: %s reporting %s instead of %s" % (self.name, time.ctime(res), time.ctime(self.mtime)))
            return res
        except Exception:
            return self.mtime
        
    def is_file(self):
        return True


#===============================================================================
# DirectoryEntry
#===============================================================================
class DirectoryEntry(_Resource):
    def __init__(self, target, rel_path, name, size, mtime, unique):
        super(DirectoryEntry, self).__init__(target, rel_path, name, size, mtime, unique)

    def is_dir(self):
        return True


#===============================================================================
# _Target
#===============================================================================
class _Target(object):

    def __init__(self, root_dir):
        self.readonly = False
        self.dry_run = False
        self.host = None
        self.root_dir = root_dir.rstrip("/")
        self.cur_dir = None
        self.connected = False
        self.save_mode = True
        self.case_sensitive = None # TODO: don't know yet
        self.time_ofs = None # TODO: don't know yet
        self.support_set_time = None # TODO: don't know yet
        self.cur_dir_meta = DirMetadata(self)
        self.meta_stack = []
        
    def __del__(self):
        self.close()
        
    def open(self):
        self.connected = True
    
    def close(self):
        self.connected = False
    
    def check_write(self, name):
        """Raise exception if writing cur_dir/name is not allowed."""
        if self.readonly and name != DirMetadata.META_FILE_NAME:
            raise RuntimeError("target is read-only: %s + %s / " % (self, name))

    def get_id(self):
        return self.root_dir

    def cwd(self, dir_name):
        raise NotImplementedError
    
    def push_meta(self):
        self.meta_stack.append( self.cur_dir_meta)
        self.cur_dir_meta = None
    
    def pop_meta(self):
        self.cur_dir_meta = self.meta_stack.pop()
        
    def flush_meta(self):
        """Write additional meta information for current directory."""
        if self.cur_dir_meta:
            self.cur_dir_meta.flush()

    def pwd(self, dir_name):
        raise NotImplementedError
    
    def mkdir(self, dir_name):
        raise NotImplementedError

    def rmdir(self, dir_name):
        """Remove cur_dir/name."""
        raise NotImplementedError

    def get_dir(self):
        """Return a list of _Resource entries."""
        raise NotImplementedError

    def open_readable(self, name):
        """Return file-like object opened in binary mode for cur_dir/name."""
        raise NotImplementedError

    def read_text(self, name):
        """Read text string from cur_dir/name using open_readable()."""
        with self.open_readable(name) as fp:
            res = fp.read()  # StringIO or file object
#             try:
#                 res = fp.getvalue()  # StringIO returned by FtpTarget
#             except AttributeError:
#                 res = fp.read()  # file object returned by FsTarget
            res = res.decode("utf8")
            return res

    def write_file(self, name, fp_src, blocksize=8192, callback=None):
        """Write binary data from file-like to cur_dir/name."""
        raise NotImplementedError

    def write_text(self, name, s):
        """Write string data to cur_dir/name using write_file()."""
        buf = io.BytesIO(to_binary(s))
        self.write_file(name, buf)

    def remove_file(self, name):
        """Remove cur_dir/name."""
        raise NotImplementedError

    def set_mtime(self, name, mtime, size):
        raise NotImplementedError


#===============================================================================
# FsTarget
#===============================================================================
class FsTarget(_Target):
    def __init__(self, root_dir):
        root_dir = os.path.expanduser(root_dir)
        root_dir = os.path.abspath(root_dir)
        if not os.path.isdir(root_dir):
            raise ValueError("%s is not a directory" % root_dir)
        super(FsTarget, self).__init__(root_dir)
        self.open()

    def __str__(self):
        return "<FS:%s + %s>" % (self.root_dir, os.path.relpath(self.cur_dir, self.root_dir))

    def open(self):
        self.connected = True
        self.cur_dir = self.root_dir

    def close(self):
        self.connected = False
        
    def cwd(self, dir_name):
        path = normurl(join_url(self.cur_dir, dir_name))
        if not path.startswith(self.root_dir):
            raise RuntimeError("Tried to navigate outside root %r: %r" % (self.root_dir, path))
        self.cur_dir_meta = None
        self.cur_dir = path
        return self.cur_dir

    def pwd(self):
        return self.cur_dir

    def mkdir(self, dir_name):
        self.check_write(dir_name)
        path = normurl(join_url(self.cur_dir, dir_name))
        os.mkdir(path)

    def rmdir(self, dir_name):
        """Remove cur_dir/name."""
        self.check_write(dir_name)
        path = normurl(join_url(self.cur_dir, dir_name))
#         print("REMOVE %r" % path)
        shutil.rmtree(path)

    def flush_meta(self):
        """Write additional meta information for current directory."""
        if self.cur_dir_meta:
            self.cur_dir_meta.flush()

    def get_dir(self):
        res = []
#        self.cur_dir_meta = None
        self.cur_dir_meta = DirMetadata(self)
        for name in os.listdir(self.cur_dir):
            path = os.path.join(self.cur_dir, name)
            stat = os.lstat(path)
#            print(name)
#            print("    mt : %s" % stat.st_mtime)
#            print("    lc : %s" % (time.localtime(stat.st_mtime),))
#            print("       : %s" % time.asctime(time.localtime(stat.st_mtime)))
#            print("    gmt: %s" % (time.gmtime(stat.st_mtime),))
#            print("       : %s" % time.asctime(time.gmtime(stat.st_mtime)))
#
#            utc_stamp = st_mtime_to_utc(stat.st_mtime)
#            print("    utc: %s" % utc_stamp)
#            print("    diff: %s" % ((utc_stamp - stat.st_mtime) / (60*60)))
            # stat.st_mtime is returned as UTC
            mtime = stat.st_mtime
            if os.path.isdir(path):
                res.append(DirectoryEntry(self, self.cur_dir, name, stat.st_size, 
                                          mtime, 
                                          str(stat.st_ino)))
            elif os.path.isfile(path):
                if name == DirMetadata.META_FILE_NAME:
                    self.cur_dir_meta.read()
                else:
                    res.append(FileEntry(self, self.cur_dir, name, stat.st_size, 
                                         mtime, 
                                         str(stat.st_ino)))
        return res

    def open_readable(self, name):
        fp = open(os.path.join(self.cur_dir, name), "rb")
        return fp
        
    def write_file(self, name, fp_src, blocksize=8192, callback=None):
        self.check_write(name)
        with open(os.path.join(self.cur_dir, name), "wb") as fp_dst:
            while True:
                data = fp_src.read(blocksize)
                if data is None or not len(data):
                    break
                fp_dst.write(data)
                if callback:
                    callback(data)
        return
        
    def remove_file(self, name):
        """Remove cur_dir/name."""
        self.check_write(name)
        path = os.path.join(self.cur_dir, name)
        os.remove(path)

    def set_mtime(self, name, mtime, size):
        self.check_write(name)
        os.utime(os.path.join(self.cur_dir, name), (-1, mtime))
