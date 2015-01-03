# -*- coding: iso-8859-1 -*-
"""
(c) 2012-2015 Martin Wendt; see https://github.com/mar10/pyftpsync
Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
"""

from __future__ import print_function

import io
import os
from posixpath import join as join_url, normpath as normpath_url
import shutil
import sys
import json
import time
from ftpsync._version import __version__
from ftpsync.resources import DirectoryEntry, FileEntry


try:
    from urllib.parse import urlparse
except ImportError:
    # Python 2
    from urlparse import urlparse



DEFAULT_CREDENTIAL_STORE = "pyftpsync.pw"
DRY_RUN_PREFIX = "(DRY-RUN) "
IS_REDIRECTED = (os.fstat(0) != os.fstat(1))
DEFAULT_BLOCKSIZE = 8 * 1024
# DEFAULT_BLOCKSIZE = 32 * 1024


def get_stored_credentials(filename, url):
    """Parse a file in the user's home directory, formatted like:
    
    URL = user:password
    """
    # TODO: 
    # We should support configuring by host or URL prefix.
    # (Exact URLs still override common settings) 
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
    PRETTY = True # False: Reduce meta file size to 35% (3759 -> 1375 bytes)
    VERSION = 1 # Increment if format changes. Old files will be discarded then.
    
    def __init__(self, target):
        self.target = target
        self.path = target.cur_dir
        self.list = {}
        self.peer_sync = {}
        self.dir = {"files": self.list,
                    "peer_sync": self.peer_sync,
                    }
        self.filename = self.META_FILE_NAME
        self.modified_list = False
        self.modified_sync = False
#        self.readonly = False
        self.was_read = False
        
    def set_mtime(self, filename, mtime, size):
        """Store real file mtime in meta data.
        
        This is needed, because FTP targets don't allow to set file mtime, but 
        use to the upload time instead.
        We also record size and upload time, so we can detect if the file was
        changed by other means and we have to discard our meta data.
        """
        ut = time.time()  # UTC time stamp
        self.list[filename] = {"mtime": mtime,
                               "size": size,
                               "uploaded": ut,
                               }
        if self.PRETTY:
            self.list[filename].update({
                "mtime_str": time.ctime(mtime),
                "uploaded_str": time.ctime(ut),
                })
        self.modified_list = True
    
    def set_sync_info(self, filename, mtime, size):
        """Store mtime/size when local and remote file was last synchronized.
        
        This is stored in the local file's folder as meta data.
        The information is used to detect conflicts, i.e. if both source and
        remote had been modified by other means since last synchronization.
        """
        assert self.target.is_local()
        remote_target = self.target.peer
        ps = self.dir["peer_sync"].setdefault(remote_target.get_id(), {})
        pse = ps[filename] = {"mtime": mtime, 
                              "size": size,
                              }
        if self.PRETTY:
            pse["mtime_str"] = time.ctime(mtime) if mtime else "(directory)"
        self.modified_sync = True
        
    def remove(self, filename):
        if self.list.pop(filename, None):
            self.modified_list = True
        if self.target.is_local():
            remote_target = self.target.peer
            self.modified_sync = self.dir["peer_sync"][remote_target.get_id()].pop(filename, None)
        return

    def read(self):
        assert self.path == self.target.cur_dir
        try:
            s = self.target.read_text(self.filename)
            self.target.synchronizer._inc_stat("meta_bytes_read", len(s))
            self.was_read = True # True, if exists (even invalid)
            self.dir = json.loads(s)
            if self.dir.get("_file_version", 0) < self.VERSION:
                raise RuntimeError("Invalid meta data version: %s (expected %s)" % (self.dir.get("_file_version"), self.VERSION))
            self.list = self.dir["files"]
            self.peer_sync = self.dir["peer_sync"] 
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
        assert self.path == self.target.cur_dir
        if self.target.dry_run:
#             print("DirMetadata.flush(%s): dry-run; nothing to do" % self.target)
            pass
        
        elif self.was_read and len(self.list) == 0 and len(self.peer_sync) == 0:
#             print("DirMetadata.flush(%s): DELETE" % self.target)
            self.target.remove_file(self.filename)

        elif not self.modified_list and not self.modified_sync:
#             print("DirMetadata.flush(%s): unmodified; nothing to do" % self.target)
            pass

        else:        
            self.dir["_disclaimer"] = "Generated by https://github.com/mar10/pyftpsync"
            self.dir["_time_str"] = "%s" % time.ctime()
            self.dir["_file_version"] = self.VERSION
            self.dir["_version"] = __version__
            self.dir["_time"] = time.mktime(time.gmtime())
            if self.PRETTY:
                s = json.dumps(self.dir, indent=4, sort_keys=True)
            else:
                s = json.dumps(self.dir)
#             print("DirMetadata.flush(%s)" % (self.target, ))#, s)
            self.target.write_text(self.filename, s)
            self.target.synchronizer._inc_stat("meta_bytes_written", len(s))

        self.modified_list = False
        self.modified_sync = False


#===============================================================================
# _Target
#===============================================================================
class _Target(object):

    def __init__(self, root_dir):
        self.readonly = False
        self.dry_run = False
        self.host = None
        self.root_dir = root_dir.rstrip("/")
        self.synchronizer = None # Set by BaseSynchronizer.__init__()
        self.peer = None
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

    def get_base_name(self):
        return "%s" % self.root_dir

    def is_local(self):
        return self.synchronizer.local is self
          
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

    def get_sync_info(self, name):
        """Get mtime/size when this target's current dir was last synchronized with remote."""
        peer_target = self.peer
        if self.is_local():
            info = self.cur_dir_meta.dir["peer_sync"].get(peer_target.get_id())
        else:
            info = peer_target.cur_dir_meta.dir["peer_sync"].get(self.get_id())
        if name is not None:
            info = info.get(name) if info else None
        return info

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

    def set_sync_info(self, name, mtime, size):
        """Store mtime/size when this resource was last synchronized with remote."""
        if not self.is_local():
            return self.peer.set_sync_info(name, mtime, size)
        return self.cur_dir_meta.set_sync_info(name, mtime, size)

    def remove_sync_info(self, name):
        if not self.is_local():
            return self.peer.remove_sync_info(name)
        return self.cur_dir_meta.remove(name)


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
        path = normpath_url(join_url(self.cur_dir, dir_name))
        if not path.startswith(self.root_dir):
            raise RuntimeError("Tried to navigate outside root %r: %r" % (self.root_dir, path))
        self.cur_dir_meta = None
        self.cur_dir = path
        return self.cur_dir

    def pwd(self):
        return self.cur_dir

    def mkdir(self, dir_name):
        self.check_write(dir_name)
        path = normpath_url(join_url(self.cur_dir, dir_name))
        os.mkdir(path)

    def rmdir(self, dir_name):
        """Remove cur_dir/name."""
        self.check_write(dir_name)
        path = normpath_url(join_url(self.cur_dir, dir_name))
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
        
    def write_file(self, name, fp_src, blocksize=DEFAULT_BLOCKSIZE, callback=None):
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
        """Set modification time on file."""
        self.check_write(name)
        os.utime(os.path.join(self.cur_dir, name), (-1, mtime))
