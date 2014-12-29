# -*- coding: iso-8859-1 -*-
"""
(c) 2012-2014 Martin Wendt; see https://github.com/mar10/pyftpsync
Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
"""

from __future__ import print_function

import fnmatch
import sys
import time

from ftpsync.targets import FileEntry, DirectoryEntry, IS_REDIRECTED, \
    DRY_RUN_PREFIX, DirMetadata


#===============================================================================
# BaseSynchronizer
#===============================================================================
class BaseSynchronizer(object):
    """Synchronizes two target instances in dry_run mode (also base class for other synchonizers)."""
    DEFAULT_EXCLUDES = [".DS_Store",
                        ".git",
                        ".hg",
                        ".svn",
                        DirMetadata.META_FILE_NAME,
                        ]

    def __init__(self, local, remote, options):
        self.local = local
        self.remote = remote
        #TODO: check for self-including paths
        self.options = options or {}
        self.verbose = self.options.get("verbose", 3) 
        self.dry_run = self.options.get("dry_run", True)

        self.include_files = self.options.get("include_files")
        if self.include_files:
            self.include_files = [ pat.strip() for pat in self.include_files.split(",") ]

        self.omit = self.options.get("omit")
        if self.omit:
            self.omit = [ pat.strip() for pat in self.omit.split(",") ]
        
        if self.dry_run:
            self.local.readonly = True
            self.remote.readonly = True
        
        self._stats = {"local_files": 0,
                       "local_dirs": 0,
                       "remote_files": 0,
                       "remote_dirs": 0,
                       "files_created": 0,
                       "files_deleted": 0,
                       "files_written": 0,
                       "dirs_created": 0,
                       "dirs_deleted": 0,
                       "bytes_written": 0,
                       "entries_seen": 0,
                       "entries_touched": 0,
                       "elap_str": None,
                       "elap_secs": None,
                       }
    
    def get_stats(self):
        return self._stats
    
    def _inc_stat(self, name, ofs=1):
        self._stats[name] = self._stats.get(name, 0) + ofs

    def _match(self, entry):
        name = entry.name
        if name == DirMetadata.META_FILE_NAME:
            return False
#        if name in self.DEFAULT_EXCLUDES:
#            return False
        ok = True
        if entry.is_file() and self.include_files:
            ok = False
            for pat in self.include_files:
                if fnmatch.fnmatch(name, pat):
                    ok = True
                    break
        if ok and self.omit:
            for pat in self.omit:
                if fnmatch.fnmatch(name, pat):
                    ok = False
                    break
        return ok
    
    def run(self):
        start = time.time()
        
        res = self._sync_dir()
        
        stats = self._stats
        stats["elap_secs"] = time.time() - start
        stats["elap_str"] = "%0.2f sec" % stats["elap_secs"]

        def _add(rate, size, time):
            if stats.get(time) and stats.get(size):
#                stats[rate] = "%0.2f MB/sec" % (.000001 * stats[size] / stats[time])
                stats[rate] = "%0.2f kb/sec" % (.001 * stats[size] / stats[time])
        _add("upload_rate_str", "upload_bytes_written", "upload_write_time")
        _add("download_rate_str", "download_bytes_written", "download_write_time")
        return res
    
    def _copy_file(self, src, dest, file_entry):
        # TODO: save replace:
        # 1. remove temp file
        # 2. copy to target.temp
        # 3. use loggingFile for feedback
        # 4. rename target.temp
#        print("_copy_file(%s, %s --> %s)" % (file_entry, src, dest))
        assert isinstance(file_entry, FileEntry)
        self._inc_stat("files_written")
        self._inc_stat("entries_touched")
        self._tick()
        if self.dry_run:
            return self._dry_run_action("copy file (%s, %s --> %s)" % (file_entry, src, dest))
        elif dest.readonly:
            raise RuntimeError("target is read-only: %s" % dest)

        is_upload = (dest is self.remote)
        start = time.time()
        def __block_written(data):
#            print(">(%s), " % len(data))
            self._inc_stat("bytes_written", len(data))
            if is_upload:
                self._inc_stat("upload_bytes_written", len(data))
            else:
                self._inc_stat("download_bytes_written", len(data))

        with src.open_readable(file_entry.name) as fp_src:
            dest.write_file(file_entry.name, fp_src, callback=__block_written)

        dest.set_mtime(file_entry.name, file_entry.mtime, file_entry.size)

        elap = time.time() - start
        self._inc_stat("write_time", elap)
        if is_upload:
            self._inc_stat("upload_write_time", elap)
        else:
            self._inc_stat("download_write_time", elap)
        return
    
    def _copy_recursive(self, src, dest, dir_entry):
#        print("_copy_recursive(%s, %s --> %s)" % (dir_entry, src, dest))
        assert isinstance(dir_entry, DirectoryEntry)
        self._inc_stat("entries_touched")
        self._inc_stat("dirs_created")
        self._tick()
        if self.dry_run:
            return self._dry_run_action("copy directory (%s, %s --> %s)" % (dir_entry, src, dest))
        elif dest.readonly:
            raise RuntimeError("target is read-only: %s" % dest)
        
        src.push_meta()
        dest.push_meta()
        
        src.cwd(dir_entry.name)
        dest.mkdir(dir_entry.name)
        dest.cwd(dir_entry.name)
        dest.cur_dir_stamps = DirMetadata(dest)
        for entry in src.get_dir():
            # the outer call was already accompanied by an increment, but not recursions
            self._inc_stat("entries_seen")
            if entry.is_dir():
                self._copy_recursive(src, dest, entry)
            else:
                self._copy_file(src, dest, entry)
        src.flush_meta()
        dest.flush_meta()
        src.cwd("..")
        dest.cwd("..")
        
        src.pop_meta()
        dest.pop_meta()
        return

    def _remove_file(self, file_entry):
        # TODO: honor backup
#        print("_remove_file(%s)" % (file_entry, ))
        assert isinstance(file_entry, FileEntry)
        self._inc_stat("entries_touched")
        self._inc_stat("files_deleted")
        if self.dry_run:
            return self._dry_run_action("delete file (%s)" % (file_entry,))
        elif file_entry.target.readonly:
            raise RuntimeError("target is read-only: %s" % file_entry.target)
        file_entry.target.remove_file(file_entry.name)

    def _remove_dir(self, dir_entry):
        # TODO: honor backup
        assert isinstance(dir_entry, DirectoryEntry)
        self._inc_stat("entries_touched")
        self._inc_stat("dirs_deleted")
        if self.dry_run:
            return self._dry_run_action("delete directory (%s)" % (dir_entry,))
        elif dir_entry.target.readonly:
            raise RuntimeError("target is read-only: %s" % dir_entry.target)
        dir_entry.target.rmdir(dir_entry.name)

    def _log_call(self, msg, min_level=5):
        if self.verbose >= min_level: 
            print(msg)
        
    def _log_action(self, action, status, symbol, entry, min_level=3):
        if self.verbose < min_level:
            return
        prefix = "" 
        if self.dry_run:
            prefix = DRY_RUN_PREFIX
        if action and status:
            tag = ("%s %s" % (action, status)).upper()
        else:
            tag = ("%s%s" % (action, status)).upper()
        name = entry.get_rel_path()
        if entry.is_dir():
            name = "[%s]" % name
        print("%s%-16s %-2s %s" % (prefix, tag, symbol, name))
        
    def _dry_run_action(self, action):
        """"Called in dry-run mode after call to _log_action() and before exiting function."""
#        print("dry-run", action)
        return
    
    def _test_match_or_print(self, entry):
        """Return True if entry matches filter. Otherwise print 'skip' and return False ."""
        if not self._match(entry):
            self._log_action("skip", "unmatched", "-", entry, min_level=4)
            return False
        return True
    
    def _tick(self):
        """Write progress info and move cursor to beginning of line."""
        if (self.verbose >= 3 and not IS_REDIRECTED) or self.options.get("progress"):
            stats = self.get_stats()
            prefix = DRY_RUN_PREFIX if self.dry_run else ""
            sys.stdout.write("%sTouched %s/%s entries in %s dirs...\r" 
                % (prefix,
                   stats["entries_touched"], stats["entries_seen"], 
                   stats["local_dirs"]))
        sys.stdout.flush()
        return
    
    def _before_sync(self, entry):
        """Called by the synchronizer for each entry. 
        Return False to prevent the synchronizer's default action.
        """
        self._inc_stat("entries_seen")
        self._tick()
        return True
    
    def _sync_dir(self):
        """Traverse the local folder structure and remote peers.
        
        This is the core algorithm that generates calls to self.sync_XXX() 
        handler methods.
        _sync_dir() is called by self.run().
        """
        local_entries = self.local.get_dir()
        local_entry_map = dict(map(lambda e: (e.name, e), local_entries))
        local_files = [e for e in local_entries if isinstance(e, FileEntry)]
        local_directories = [e for e in local_entries if isinstance(e, DirectoryEntry)]
        
        remote_entries = self.remote.get_dir()
        # convert into a dict {name: FileEntry, ...}
        remote_entry_map = dict(map(lambda e: (e.name, e), remote_entries))
        
        # 1. Loop over all local files and classify the relationship to the
        #    peer entries.
        for local_file in local_files:
            self._inc_stat("local_files")
            if not self._before_sync(local_file):
                # TODO: currently, if a file is skipped, it will not be
                # considered for deletion on the peer target
                continue
            # TODO: case insensitive?
            # We should use os.path.normcase() to convert to lowercase on windows
            # (i.e. if the FTP server is based on Windows)
            remote_file = remote_entry_map.get(local_file.name)

            if remote_file is None:
                self.sync_missing_remote_file(local_file)
            elif local_file == remote_file:
                self.sync_equal_file(local_file, remote_file)
            # TODO: renaming could be triggered, if we find an existing
            # entry.unique with a different entry.name
#            elif local_file.key in remote_keys:
#                self._rename_file(local_file, remote_file)
            elif local_file > remote_file:
                self.sync_newer_local_file(local_file, remote_file)
            elif local_file < remote_file:
                self.sync_older_local_file(local_file, remote_file)
            else:
                self._sync_error("file with identical date but different otherwise", 
                                 local_file, remote_file)

        # 2. Handle all local directories that do NOT exist on remote target.
        for local_dir in local_directories:
            self._inc_stat("local_dirs")
            if not self._before_sync(local_dir):
                continue
            remote_dir = remote_entry_map.get(local_dir.name)
            if not remote_dir:
                self.sync_missing_remote_dir(local_dir)

        # 3. Handle all remote entries that do NOT exist on the local target.
        for remote_entry in remote_entries:
            if isinstance(remote_entry, DirectoryEntry):
                self._inc_stat("remote_dirs")
            else:
                self._inc_stat("remote_files")
                
            if not self._before_sync(remote_entry):
                continue
            if not remote_entry.name in local_entry_map:
                if isinstance(remote_entry, DirectoryEntry):
                    self.sync_missing_local_dir(remote_entry)
                else:  
                    self.sync_missing_local_file(remote_entry)
        
        # 4. Let the target provider write its meta data for the files in the 
        #    current directory.
        self.local.cur_dir_stamps.set_last_sync(self.remote)
        self.local.flush_meta()
        self.remote.flush_meta()

        # 5. Finally visit all local sub-directories recursively that also 
        #    exist on the remote target.
        for local_dir in local_directories:
            if not self._before_sync(local_dir):
                continue
            remote_dir = remote_entry_map.get(local_dir.name)
            if remote_dir:
                res = self.sync_equal_dir(local_dir, remote_dir)
                if res is not False:
                    self.local.cwd(local_dir.name)
                    self.remote.cwd(local_dir.name)
                    self._sync_dir()
                    self.local.cwd("..")
                    self.remote.cwd("..")

        return
        
    def _sync_error(self, msg, local_file, remote_file):
        print(msg, local_file, remote_file, file=sys.stderr)
    
    def sync_equal_file(self, local_file, remote_file):
        self._log_call("sync_equal_file(%s, %s)" % (local_file, remote_file))
        self._log_action("", "equal", "=", local_file, min_level=4)
    
    def sync_equal_dir(self, local_dir, remote_dir):
        """Return False to prevent visiting of children"""
        self._log_call("sync_equal_dir(%s, %s)" % (local_dir, remote_dir))
        self._log_action("", "equal", "=", local_dir, min_level=4)
        return True
    
    def sync_newer_local_file(self, local_file, remote_file):
        self._log_call("sync_newer_local_file(%s, %s)" % (local_file, remote_file))
        self._log_action("", "modified", ">", local_file)
    
    def sync_older_local_file(self, local_file, remote_file):
        self._log_call("sync_older_local_file(%s, %s)" % (local_file, remote_file))
        self._log_action("", "modified", "<", local_file)
    
    def sync_missing_local_file(self, remote_file):
        self._log_call("sync_missing_local_file(%s)" % remote_file)
        self._log_action("", "missing", "<", remote_file)
    
    def sync_missing_local_dir(self, remote_dir):
        """Return False to prevent visiting of children"""
        self._log_call("sync_missing_local_dir(%s)" % remote_dir)
        self._log_action("", "missing", "<", remote_dir)
    
    def sync_missing_remote_file(self, local_file):
        self._log_call("sync_missing_remote_file(%s)" % local_file)
        self._log_action("", "new", ">", local_file)
    
    def sync_missing_remote_dir(self, local_dir):
        self._log_call("sync_missing_remote_dir(%s)" % local_dir)
        self._log_action("", "new", ">", local_dir)


#===============================================================================
# UploadSynchronizer
#===============================================================================
class UploadSynchronizer(BaseSynchronizer):
    def __init__(self, local, remote, options):
        super(UploadSynchronizer, self).__init__(local, remote, options)
        local.readonly = True
        # don't set target.readonly to True, because it might have been set to
        # False by a caller to enforce security
#        remote.readonly = False

    def _check_del_unmatched(self, remote_entry):
        """Return True if entry is NOT matched (i.e. excluded by filter).
        
        If --delete-unmatched is on, remove the remote resource. 
        """
        if not self._match(remote_entry):
            if self.options.get("delete_unmatched"):
                self._log_action("delete", "unmatched", ">", remote_entry)
                if remote_entry.is_dir():
                    self._remove_dir(remote_entry)
                else:
                    self._remove_file(remote_entry)
            else:
                self._log_action("skip", "unmatched", "-", remote_entry, min_level=4)
            return True
        return False

    def sync_equal_file(self, local_file, remote_file):
        self._log_call("sync_equal_file(%s, %s)" % (local_file, remote_file))
        self._log_action("", "equal", "=", local_file, min_level=4)
        self._check_del_unmatched(remote_file)
    
    def sync_equal_dir(self, local_dir, remote_dir):
        """Return False to prevent visiting of children"""
        self._log_call("sync_equal_dir(%s, %s)" % (local_dir, remote_dir))
        if self._check_del_unmatched(remote_dir):
            return False
        self._log_action("", "equal", "=", local_dir, min_level=4)
        return True

    def sync_newer_local_file(self, local_file, remote_file):
        self._log_call("sync_newer_local_file(%s, %s)" % (local_file, remote_file))
        if self._check_del_unmatched(remote_file):
            return False
        self._log_action("copy", "modified", ">", local_file)
        self._copy_file(self.local, self.remote, local_file)
#        if not self._match(remote_file) and self.options.get("delete_unmatched"):
#            self._log_action("delete", "unmatched", ">", remote_file)
#            self._remove_file(remote_file)
#        elif self._test_match_or_print(local_file):
#            self._log_action("copy", "modified", ">", local_file)
#            self._copy_file(self.local, self.remote, local_file)

    def sync_older_local_file(self, local_file, remote_file):
        self._log_call("sync_older_local_file(%s, %s)" % (local_file, remote_file))
        if self._check_del_unmatched(remote_file):
            return False
        elif self.options.get("force"):
            self._log_action("restore", "older", ">", local_file)
            self._copy_file(self.local, self.remote, remote_file)
        else:
            self._log_action("skip", "older", "?", local_file, 4)
#        if not self._match(remote_file) and self.options.get("delete_unmatched"):
#            self._log_action("delete", "unmatched", ">", remote_file)
#            self._remove_file(remote_file)
#        elif self.options.get("force"):
#            self._log_action("restore", "older", ">", local_file)
#            self._copy_file(self.local, self.remote, remote_file)
#        else:
#            self._log_action("skip", "older", "?", local_file, 4)

    def sync_missing_local_file(self, remote_file):
        self._log_call("sync_missing_local_file(%s)" % remote_file)
        # If a file exists locally, but does not match the filter, this will be
        # handled by sync_newer_file()/sync_older_file()
        if self._check_del_unmatched(remote_file):
            return False
        elif not self._test_match_or_print(remote_file):
            return
        elif self.options.get("delete"):
            self._log_action("delete", "missing", ">", remote_file)
            self._remove_file(remote_file)
        else:
            self._log_action("skip", "missing", "?", remote_file, 4)
#        if not self._test_match_or_print(remote_file):
#            return
#        elif self.options.get("delete"):
#            self._log_action("delete", "missing", ">", remote_file)
#            self._remove_file(remote_file)
#        else:
#            self._log_action("skip", "missing", "?", remote_file, 4)

    def sync_missing_local_dir(self, remote_dir):
        self._log_call("sync_missing_local_dir(%s)" % remote_dir)
        if self._check_del_unmatched(remote_dir):
            return False
        elif not self._test_match_or_print(remote_dir):
            return False
        elif self.options.get("delete"):
            self._log_action("delete", "missing", ">", remote_dir)
            self._remove_dir(remote_dir)
        else:
            self._log_action("skip", "missing", "?", remote_dir, 4)
    
    def sync_missing_remote_file(self, local_file):
        self._log_call("sync_missing_remote_file(%s)" % local_file)
        if self._test_match_or_print(local_file):
            self._log_action("copy", "new", ">", local_file)
            self._copy_file(self.local, self.remote, local_file)
    
    def sync_missing_remote_dir(self, local_dir):
        self._log_call("sync_missing_remote_dir(%s)" % local_dir)
        if self._test_match_or_print(local_dir):
            self._log_action("copy", "new", ">", local_dir)
            self._copy_recursive(self.local, self.remote, local_dir)
    

#===============================================================================
# DownloadSynchronizer
#===============================================================================
class DownloadSynchronizer(UploadSynchronizer):
    """
    This download syncronize is implemented as an UploadSynchronizer with
    swapped local and remote targets. 
    """
    def __init__(self, local, remote, options):
        # swap local and remote target
        temp = local
        local = remote
        remote = temp
        # behave like an UploadSynchronizer otherwise
        super(DownloadSynchronizer, self).__init__(local, remote, options)

    def _log_action(self, action, status, symbol, entry, min_level=3):
        if symbol == "<":
            symbol = ">"
        elif symbol == ">":
            symbol = "<"
        super(DownloadSynchronizer, self)._log_action(action, status, symbol, entry, min_level)


#===============================================================================
# BiDirSynchronizer
#===============================================================================
class BiDirSynchronizer(UploadSynchronizer):
    """
    This bi-directional synchronize is implemented as an UploadSynchronizer
    with some adjustments. 
    
    - newer files override unmodified older files
    - When both files are newer -> conflict!
      --force will use the newer version
    - When a file is missing: check if it existed in the past.
      If so, delete it. Otherwise copy it.
    
    In order to know if a file was modified, deleted, or created since last sync,
    we store a snapshot of the directory in the local directory.
    """
    def __init__(self, local, remote, options):
        super(BiDirSynchronizer, self).__init__(local, remote, options)
        # TODO: remove this:
        local.readonly = False
        # remote.readonly = True    


#     def _check_del_unmatched(self, remote_entry):
#         """Return True if entry is NOT matched (i.e. excluded by filter).
#         
#         If --delete-unmatched is on, remove the remote resource. 
#         """
#         if not self._match(remote_entry):
#             if self.options.get("delete_unmatched"):
#                 self._log_action("delete", "unmatched", ">", remote_entry)
#                 if remote_entry.is_dir():
#                     self._remove_dir(remote_entry)
#                 else:
#                     self._remove_file(remote_entry)
#             else:
#                 self._log_action("skip", "unmatched", "-", remote_entry, min_level=4)
#             return True
#         return False

    # def sync_equal_file(self, local_file, remote_file):
    #     self._log_call("sync_equal_file(%s, %s)" % (local_file, remote_file))
    #     self._log_action("", "equal", "=", local_file, min_level=4)
    #     self._check_del_unmatched(remote_file)
    
    # def sync_equal_dir(self, local_dir, remote_dir):
    #     """Return False to prevent visiting of children"""
    #     self._log_call("sync_equal_dir(%s, %s)" % (local_dir, remote_dir))
    #     if self._check_del_unmatched(remote_dir):
    #         return False
    #     self._log_action("", "equal", "=", local_dir, min_level=4)
    #     return True

#     def sync_newer_local_file(self, local_file, remote_file):
#         self._log_call("sync_newer_local_file(%s, %s)" % (local_file, remote_file))
#         if self._check_del_unmatched(remote_file):
#             return False
#         self._log_action("copy", "modified", ">", local_file)
#         self._copy_file(self.local, self.remote, local_file)
# #        if not self._match(remote_file) and self.options.get("delete_unmatched"):
# #            self._log_action("delete", "unmatched", ">", remote_file)
# #            self._remove_file(remote_file)
# #        elif self._test_match_or_print(local_file):
# #            self._log_action("copy", "modified", ">", local_file)
# #            self._copy_file(self.local, self.remote, local_file)

    def sync_older_local_file(self, local_file, remote_file):
        self._log_call("sync_older_local_file(%s, %s)" % (local_file, remote_file))
        if self._check_del_unmatched(local_file):
            return False
        self._log_action("copy", "modified", "<", remote_file)
        self._copy_file(self.remote, self.local, remote_file)

    def sync_missing_local_file(self, remote_file):
        self._log_call("sync_missing_local_file(%s)" % remote_file)
        # If a file exists locally, but does not match the filter, this will be
        # handled by sync_newer_file()/sync_older_file()
        if self._check_del_unmatched(remote_file):
            return False
        elif not self._test_match_or_print(remote_file):
            return
        elif self.options.get("delete"):
            self._log_action("delete", "missing", ">", remote_file)
            self._remove_file(remote_file)
        else:
            self._log_action("skip", "missing", "?", remote_file, 4)

    def sync_missing_local_dir(self, remote_dir):
        self._log_call("sync_missing_local_dir(%s)" % remote_dir)
        if self._check_del_unmatched(remote_dir):
            return False
        elif not self._test_match_or_print(remote_dir):
            return False
        elif self.options.get("delete"):
            self._log_action("delete", "missing", ">", remote_dir)
            self._remove_dir(remote_dir)
        else:
            self._log_action("skip", "missing", "?", remote_dir, 4)
    
    # def sync_missing_remote_file(self, local_file):
    #     self._log_call("sync_missing_remote_file(%s)" % local_file)
    #     if self._test_match_or_print(local_file):
    #         self._log_action("copy", "new", ">", local_file)
    #         self._copy_file(self.local, self.remote, local_file)
    
    # def sync_missing_remote_dir(self, local_dir):
    #     self._log_call("sync_missing_remote_dir(%s)" % local_dir)
    #     if self._test_match_or_print(local_dir):
    #         self._log_action("copy", "new", ">", local_dir)
    #         self._copy_recursive(self.local, self.remote, local_dir)
