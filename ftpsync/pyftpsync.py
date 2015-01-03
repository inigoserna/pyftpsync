# -*- coding: iso-8859-1 -*-
"""
Simple folder synchronization using FTP.

(c) 2012-2015 Martin Wendt; see https://github.com/mar10/pyftpsync
Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php

Usage examples:
  > pyftpsync.py --help
  > pyftpsync.py upload . ftp://example.com/myfolder
"""
from __future__ import print_function

from pprint import pprint

from ftpsync._version import __version__
from ftpsync.targets import make_target, FsTarget

from ftpsync.synchronizers import UploadSynchronizer, \
    DownloadSynchronizer, BiDirSynchronizer, DEFAULT_OMIT


#def disable_stdout_buffering():
#    """http://stackoverflow.com/questions/107705/python-output-buffering"""
#    # Appending to gc.garbage is a way to stop an object from being
#    # destroyed.  If the old sys.stdout is ever collected, it will
#    # close() stdout, which is not good.
#    gc.garbage.append(sys.stdout)
#    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
#disable_stdout_buffering()
try:
    import argparse
except ImportError:
    print("argparse missing (requires 2.7+, 3.2+ or easy_install)")
    raise




def namespace_to_dict(o):
    """Convert an argparse namespace object to a dictionary."""
    d = {}
    for k, v in o.__dict__.items():
        if not callable(v):
            d[k] = v
    return d


#===============================================================================
# upload_command
#===============================================================================

#def upload_command(parser, args):
#    opts = namespace_to_dict(args)
#    pprint(opts)
#    exit(2)
#    s = UploadSynchronizer(args.local_target, args.remote_target, opts)
#    s.run()

#===============================================================================
# run
#===============================================================================
def run():
    parser = argparse.ArgumentParser(
        description="Synchronize folders over FTP.",
        epilog="See also https://github.com/mar10/pyftpsync"
        )
    parser.add_argument("--verbose", "-v", action="count", default=3,
                        help="increment verbosity by one (default: %(default)s, range: 0..5)")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="decrement verbosity by one")
    parser.add_argument("--version", action="version", version="%s" % (__version__))
    parser.add_argument("--progress", "-p", 
                        action="store_true",
                        default=False, 
                        help="show progress info, even if redirected or verbose < 3")
    
    subparsers = parser.add_subparsers(help="sub-command help")
    
    def __add_common_sub_args(parser):
        pass
    
    # create the parser for the "upload" command
    upload_parser = subparsers.add_parser("upload", 
                                          help="copy new and modified files to remote folder")
    upload_parser.add_argument("local", 
                               metavar="LOCAL",
#                             required=True,
                               default=".",
                               help="path to local folder (default: %(default)s)")      
    upload_parser.add_argument("remote", 
                               metavar="REMOTE",
#                             required=True,
#                             default=".",
                               help="path to remote folder")
    upload_parser.add_argument("--force", 
                               action="store_true",
                               help="overwrite different remote files, even if the target is newer")
    upload_parser.add_argument("--delete", 
                               action="store_true",
                               help="remove remote files if they don't exist locally")
    upload_parser.add_argument("--delete-unmatched", 
                               action="store_true",
                               help="remove remote files if they don't exist locally "
                               "or don't match the current filter (implies '--delete' option)")
#    upload_parser.add_argument("--dry-run", 
#                             action="store_true",
#                             help="just simulate and log results; don't change anything")
    upload_parser.add_argument("-x", "--execute", 
                               action="store_false", dest="dry_run", default=True,
                               help="turn off the dry-run mode (which is ON by default), "
                               "that would just print status messages but does "
                               "not change anything")
    upload_parser.add_argument("-f", "--include-files", 
                               help="wildcard for file names (default: all, "
                               "separate multiple values with ',')")
    upload_parser.add_argument("-o", "--omit", 
                               help="wildcard of files and directories to exclude (applied after --include)")

#    upload_parser.set_defaults(handler=upload_command)
    upload_parser.set_defaults(command="upload")
    

    # create the parser for the "download" command
    download_parser = subparsers.add_parser("download", 
            help="copy new and modified files from remote folder to local target")
    download_parser.add_argument("local", 
                                 metavar="LOCAL",
                                 default=".",
                                 help="path to local folder (default: %(default)s)")
    download_parser.add_argument("remote", 
                                 metavar="REMOTE",
                                 help="path to remote folder")
    download_parser.add_argument("--force", 
                                 action="store_true",
                                 help="overwrite different local files, even if the target is newer")
    download_parser.add_argument("--delete", 
                                 action="store_true",
                                 help="remove local files if they don't exist on remote target")
    download_parser.add_argument("--delete-unmatched", 
                                 action="store_true",
                                 help="remove local files if they don't exist on remote target "
                                 "or don't match the current filter (implies '--delete' option)")
    download_parser.add_argument("-x", "--execute", 
                                 action="store_false", dest="dry_run", default=True,
                                 help="turn off the dry-run mode (which is ON by default), "
                                 "that would just print status messages but does "
                                 "not change anything")
    download_parser.add_argument("-f", "--include-files", 
                                 help="wildcard for file names (default: all, "
                                 "separate multiple values with ',')")
    download_parser.add_argument("-o", "--omit", 
                                 help="wildcard of files and directories to exclude (applied after --include)")
    download_parser.set_defaults(command="download")
    
    # create the parser for the "sync" command
    sync_parser = subparsers.add_parser("sync", 
            help="synchronize new and modified files between remote folder and local target")
    sync_parser.add_argument("local", 
                             metavar="LOCAL",
                             default=".",
                             help="path to local folder (default: %(default)s)")
    sync_parser.add_argument("remote", 
                             metavar="REMOTE",
                             help="path to remote folder")
#     sync_parser.add_argument("--force", 
#                              action="store_true",
#                              help="overwrite conflicted files with newer version")
#     sync_parser.add_argument("--delete", 
#                              action="store_true",
#                              help="remove local files if they don't exist on remote target")
#     sync_parser.add_argument("--delete-unmatched", 
#                              action="store_true",
#                              help="remove local files if they don't exist on remote target "
#                              "or don't match the current filter (implies '--delete' option)")
    sync_parser.add_argument("-x", "--execute", 
                             action="store_false", dest="dry_run", default=True,
                             help="turn off the dry-run mode (which is ON by default), "
                             "that would just print status messages but does "
                             "not change anything")
    sync_parser.add_argument("-f", "--include-files", 
                             help="wildcard for file names (default: all, "
                             "separate multiple values with ',')")
    sync_parser.add_argument("-o", "--omit", 
#                             nargs="?",
#                             default=",".join(DEFAULT_OMIT),
                             help="wildcard of files and directories to exclude (applied after --include)")
    sync_parser.set_defaults(command="synchronize")
    
    # Parse command line
    args = parser.parse_args()
#    print("args %s %s" % (args.omit, args))
#    return
    # Post-process and check arguments
    args.verbose -= args.quiet
    del args.quiet
    if hasattr(args, "delete_unmatched") and args.delete_unmatched:
        args.delete = True
    if args.remote == ".":
        parser.error("'.' is expected to be the local target")

    ftp_debug = 0
    if args.verbose >= 5:
        ftp_debug = 1 
    args.local_target = make_target(args.local, debug=ftp_debug)
    args.remote_target = make_target(args.remote, debug=ftp_debug)
    if not isinstance(args.local_target, FsTarget) and isinstance(args.remote_target, FsTarget):
        parser.error("a file system target is expected to be local")

    # Let the command handler do its thing
#    args.handler(parser, args)
    opts = namespace_to_dict(args)
    if args.command == "upload":
        s = UploadSynchronizer(args.local_target, args.remote_target, opts)
    elif args.command == "download":
        s = DownloadSynchronizer(args.local_target, args.remote_target, opts)
    elif args.command == "synchronize":
        s = BiDirSynchronizer(args.local_target, args.remote_target, opts)
    else:
        parser.error("unknown command %s" % args.command)
    
    s.run()

    stats = s.get_stats()
    if args.verbose >= 4:
        pprint(stats)
    elif args.verbose >= 1:
        if args.dry_run:
            print("(DRY-RUN) ", end="")
        print("Wrote %s/%s files in %s dirs. Elap: %s" 
              % (stats["files_written"], stats["local_files"], stats["local_dirs"], stats["elap_str"]))
    

# Script entry point
if __name__ == "__main__":
    run()
