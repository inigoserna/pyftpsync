# -*- coding: iso-8859-1 -*-
"""
(c) 2012-2014 Martin Wendt; see https://github.com/mar10/pyftpsync
Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
"""
from __future__ import print_function

import calendar
import ftplib
import io
from posixpath import join as join_url, normpath as normpath_url, relpath as relpath_url
import sys
import time

from ftpsync import targets
from ftpsync.targets import _Target, DirMetadata
from ftpsync.resources import DirectoryEntry, FileEntry

DEFAULT_BLOCKSIZE = targets.DEFAULT_BLOCKSIZE

#===============================================================================
# FtpTarget
#===============================================================================
class FtpTarget(_Target):
    
    def __init__(self, path, host, username=None, password=None, connect=True, debug=0):
        path = path or "/"
        super(FtpTarget, self).__init__(path)
        self.ftp = ftplib.FTP()
        self.ftp.debug(debug)
        self.host = host
        self.username = username
        self.password = password
        if connect:
            self.open()

    def __str__(self):
        return "<ftp:%s%s + %s>" % (self.host, self.root_dir, relpath_url(self.cur_dir, self.root_dir))

    def get_base_name(self):
        return "ftp:%s%s" % (self.host, self.root_dir)

    def open(self):
        self.ftp.connect(self.host)
        if self.username:
            self.ftp.login(self.username, self.password)
        # TODO: case sensitivity?
#        resp = self.ftp.sendcmd("system")
#        self.is_unix = "unix" in resp.lower()
        self.ftp.cwd(self.root_dir)
        pwd = self.ftp.pwd()
        if pwd != self.root_dir:
            raise RuntimeError("Unable to navigate to working directory %r" % self.root_dir)
        self.cur_dir = pwd
        self.connected = True

    def close(self):
        if self.connected:
            self.ftp.quit()
        self.connected = False
        
    def get_id(self):
        return self.host + self.root_dir

    def cwd(self, dir_name):
        path = normpath_url(join_url(self.cur_dir, dir_name))
        if not path.startswith(self.root_dir):
            # paranoic check to prevent that our sync tool goes berserk
            raise RuntimeError("Tried to navigate outside root %r: %r" 
                               % (self.root_dir, path))
        self.ftp.cwd(dir_name)
        self.cur_dir = path
        self.cur_dir_meta = None
        return self.cur_dir

    def pwd(self):
        return self.ftp.pwd()

    def mkdir(self, dir_name):
        self.check_write(dir_name)
        self.ftp.mkd(dir_name)

    def _rmdir_impl(self, dir_name, keep_root=False):
        # FTP does not support deletion of non-empty directories.
#        print("rmdir(%s)" % dir_name)
        self.check_write(dir_name)
        names = self.ftp.nlst(dir_name)
#        print("rmdir(%s): %s" % (dir_name, names))
        # Skip ftp.cwd(), if dir is empty
        names = [ n for n in names if n not in (".", "..") ]
        if len(names) > 0:
            self.ftp.cwd(dir_name)
            try:
                for name in names:
                    try:
                        # try to delete this as a file
                        self.ftp.delete(name)
                    except ftplib.all_errors as _e:
#                        print("    ftp.delete(%s) failed: %s, trying rmdir()..." % (name, _e))
                        # assume <name> is a folder
                        self.rmdir(name)
            finally:
                if dir_name != ".":
                    self.ftp.cwd("..")
#        print("ftp.rmd(%s)..." % (dir_name, ))
        if not keep_root:
            self.ftp.rmd(dir_name)
        return

    
    def rmdir(self, dir_name):
        return self._rmdir_impl(dir_name)


    def get_dir(self):
        entry_list = []
        entry_map = {}
        local_res = {"has_meta": False} # pass local variables outside func scope 
        
        def _addline(line):
            data, _, name = line.partition("; ")
            res_type = size = mtime = unique = None
            fields = data.split(";")
            # http://tools.ietf.org/html/rfc3659#page-23
            # "Size" / "Modify" / "Create" / "Type" / "Unique" / "Perm" / "Lang"
            #   / "Media-Type" / "CharSet" / os-depend-fact / local-fact
            for field in fields:
                field_name, _, field_value = field.partition("=")
                field_name = field_name.lower()
                if field_name == "type":
                    res_type = field_value
                elif field_name in ("sizd", "size"):
                    size = int(field_value)
                elif field_name == "modify":
                    # Use calendar.timegm() instead of time.mktime(), because
                    # the date was returned as UTC
                    mtime = calendar.timegm(time.strptime(field_value, "%Y%m%d%H%M%S"))
#                    print("MLST modify: ", field_value, "mtime", mtime, "ctime", time.ctime(mtime))
                elif field_name == "unique":
                    unique = field_value
                    
            entry = None
            if res_type == "dir":
                entry = DirectoryEntry(self, self.cur_dir, name, size, mtime, unique)
            elif res_type == "file":
                if name == DirMetadata.META_FILE_NAME:
                    # the meta-data file is silently ignored
                    local_res["has_meta"] = True
                else:
                    entry = FileEntry(self, self.cur_dir, name, size, mtime, unique)
            elif res_type in ("cdir", "pdir"):
                pass
            else:
                raise NotImplementedError

            if entry:
                entry_map[name] = entry
                entry_list.append(entry)
                
        # raises error_perm, if command is not supported
        self.ftp.retrlines("MLSD", _addline)

        # load stored meta data if present
        self.cur_dir_meta = DirMetadata(self)

        if local_res["has_meta"]:
            try:
                self.cur_dir_meta.read()
            except Exception as e:
                print("Could not read meta info: %s" % e, file=sys.stderr)

            meta_files = self.cur_dir_meta.list

            # Adjust file mtime from meta-data if present
            missing = []
            for n in meta_files:
                meta = meta_files[n]
                if n in entry_map:
#                    if entry_map[n].size == meta["size"] and entry_map[n].mtime <= last_upload_time:
                    upload_time = meta.get("uploaded", 0)
                    # ???
                    # TODO: sollten wir pr�fen. ob meta.mtime (nicht meta.upload_time) ??
                    # ??? 
                    if entry_map[n].size == meta.get("size") and entry_map[n].mtime <= upload_time:
                        entry_map[n].meta = meta
                    else:
                        # Discard stored meta-data if 
                        #   1. the the mtime reported by the FTP server is later
                        #      than the stored upload time
                        #      or
                        #   2. the reported files size is different than the
                        #      size we stored in the meta-data 
#                        print("META: Removing outdated meta entry %s" % n, meta)
                        missing.append(n)
                else:
#                     print("META: Removing missing meta entry %s" % n)
                    missing.append(n)
            # Remove missing files from cur_dir_meta 
            for n in missing:
                self.cur_dir_meta.remove(n)

        return entry_list

    def open_readable(self, name):
        """Open cur_dir/name for reading."""
        out = io.BytesIO()
        self.ftp.retrbinary("RETR %s" % name, out.write)
        out.flush()
        out.seek(0)
        return out

    def write_file(self, name, fp_src, blocksize=DEFAULT_BLOCKSIZE, callback=None):
        self.check_write(name)
        self.ftp.storbinary("STOR %s" % name, fp_src, blocksize, callback)
        # TODO: check result
        
    def remove_file(self, name):
        """Remove cur_dir/name."""
        self.check_write(name)
#         self.cur_dir_meta.remove(name)
        self.ftp.delete(name)
        self.remove_sync_info(name)

    def set_mtime(self, name, mtime, size):
        self.check_write(name)
#         print("META set_mtime(%s): %s" % (name, time.ctime(mtime)))
        # We cannot set the mtime on FTP servers, so we store this as additional
        # meta data in the same directory
        # TODO: try "SITE UTIME", "MDTM (set version)", or "SRFT" command
        self.cur_dir_meta.set_mtime(name, mtime, size)
