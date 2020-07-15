
# Erik Husby; Polar Geospatial Center, University of Minnesota; 2020


from __future__ import division
import lib.script_utils as su

PYTHON_VERSION_ACCEPTED_MIN = "2.7"  # supports multiple dot notation
if su.PYTHON_VERSION < su.VersionString(PYTHON_VERSION_ACCEPTED_MIN):
    raise su.VersionError("Python version ({}) is below accepted minimum ({})".format(
        su.PYTHON_VERSION, PYTHON_VERSION_ACCEPTED_MIN))


import argparse
import copy
import glob
import os
import sys
import traceback


##############################

### Core globals ###

SCRIPT_VERSION_NUM = su.VersionString('1.0')

# Script paths and execution
SCRIPT_FILE = os.path.abspath(os.path.realpath(__file__))
SCRIPT_FNAME = os.path.basename(SCRIPT_FILE)
SCRIPT_NAME, SCRIPT_EXT = os.path.splitext(SCRIPT_FNAME)
SCRIPT_DIR = os.path.dirname(SCRIPT_FILE)
SCRIPT_RUNCMD = ' '.join(sys.argv)+'\n'
PYTHON_EXE = 'python -u'

##############################

### Argument globals ("ARGSTR_") ###

## Argument strings (positional first, then optional with '--' prefix)
ARGSTR_SRC, ARGBRV_SRC = '--src', '-s'
ARGSTR_SRCLIST = '--srclist'
ARGBRV_SRCLIST = '-sl'
ARGSTR_SRCLIST_ROOTED = '--srclist-rooted'
ARGBRV_SRCLIST_ROOTED = '-slr'
ARGSTR_DST = '--dst'
ARGBRV_DST = '-d'
ARGSTR_DSTDIR_GLOBAL = '--dstdir-global'
ARGBRV_DSTDIR_GLOBAL = '-dg'
ARGSTR_COPY_METHOD = '--copy-method'
ARGBRV_COPY_METHOD = '-cm'
ARGSTR_OVERWRITE = '--overwrite'
ARGBRV_OVERWRITE = '-o'
ARGSTR_MINDEPTH = '--mindepth'
ARGBRV_MINDEPTH = '-d0'
ARGSTR_MAXDEPTH = '--maxdepth'
ARGBRV_MAXDEPTH = '-d1'
ARGSTR_SYNC_TREE = '--sync-tree'
ARGBRV_SYNC_TREE = '-st'
ARGSTR_TRANSPLANT_TREE = '--transplant-tree'
ARGBRV_TRANSPLANT_TREE = '-tt'
ARGSTR_COLLAPSE_TREE = '--collapse-tree'
ARGBRV_COLLAPSE_TREE = '-ct'
ARGSTR_SRCLIST_DELIM = '--srclist-delim'
ARGSTR_SRCLIST_NOGLOB = '--srclist-noglob'
ARGSTR_SILENT = '--silent'
ARGBRV_SILENT = '-s'
ARGSTR_DEBUG = '--debug'
ARGBRV_DEBUG = '-db'
ARGSTR_DRYRUN = '--dryrun'
ARGBRV_DRYRUN = '-dr'

## Argument choices (declare "ARGCHO_{ARGSTR}_{option}" options followed by list of all options as "ARGCHO_{ARGSTR}")
ARGCHO_COPY_METHOD_COPY = 'copy'
ARGCHO_COPY_METHOD_MOVE = 'move'
ARGCHO_COPY_METHOD_LINK = 'link'
ARGCHO_COPY_METHOD_SYMLINK = 'symlink'
ARGCHO_COPY_METHOD = [
    ARGCHO_COPY_METHOD_COPY,
    ARGCHO_COPY_METHOD_MOVE,
    ARGCHO_COPY_METHOD_LINK,
    ARGCHO_COPY_METHOD_SYMLINK
]
# Argument choice object mapping (dict of "ARGCHO_" argument options)
COPY_METHOD_FUNCTION_DICT = {
    ARGCHO_COPY_METHOD_COPY: su.COPY_METHOD_COPY_DEFAULT,
    ARGCHO_COPY_METHOD_MOVE: su.COPY_METHOD_MOVE,
    ARGCHO_COPY_METHOD_LINK: su.COPY_METHOD_HARDLINK,
    ARGCHO_COPY_METHOD_SYMLINK: su.COPY_METHOD_SYMLINK
}

## Argument modes ("ARGMOD_", used for mutually exclusive arguments that control the same mechanic)
# (It's generally better to use a single argument with multiple choices, but sometimes we want
#  to use multiple `action='store_true'` arguments instead.)
ARGMOD_SYNC_MODE_NULL = 0
ARGMOD_SYNC_MODE_SYNC_TREE = 1
ARGMOD_SYNC_MODE_TRANSPLANT_TREE = 2

## Segregation of argument choices (lists of related argument choices)

## Argument choice settings

## Argument groups ("ARGGRP_" lists of "ARGSTR_" argument strings)
ARGGRP_SRC = [ARGSTR_SRC, ARGSTR_SRCLIST, ARGSTR_SRCLIST_ROOTED]
ARGGRP_DST = [ARGSTR_DST, ARGSTR_DSTDIR_GLOBAL]
ARGGRP_SYNC_MODE = [ARGSTR_SYNC_TREE, ARGSTR_TRANSPLANT_TREE]
ARGGRP_OUTDIR = su.ARGGRP_OUTDIR + []

## Argument collections ("ARGCOL_" lists of "ARGGRP_" argument strings)
ARGCOL_MUT_EXCL = [ARGGRP_DST, ARGGRP_SYNC_MODE]

## Argument defaults ("ARGDEF_")
ARGDEF_MINDEPTH = 0
ARGDEF_MAXDEPTH = su.ARGNUM_POS_INF
ARGDEF_BUNDLEDIR = os.path.join(os.path.expanduser('~'), 'scratch', 'task_bundles')
ARGDEF_SRCLIST_DELIM = ','
ARGDEF_JOB_ABBREV = 'Copy'
ARGDEF_JOB_WALLTIME_HR = 1
ARGDEF_JOB_MEMORY_GB = 5

## Argument help info ("ARGHLP_", only when needed outside of argparse)
ARGHLP_SRCLIST_FORMAT = None  # set globally in pre_argparse()
ARGHLP_SRCLIST_ROOTED_FORMAT = None  # set globally in pre_argparse()

##############################

### Scheduler settings ###

JOBSCRIPT_DIR = os.path.join(SCRIPT_DIR, 'jobscripts')
JOBSCRIPT_INIT = os.path.join(JOBSCRIPT_DIR, 'init.sh')
BUNDLE_TASK_ARGSTRS = [ARGSTR_SRC, ARGSTR_DST]
BUNDLE_LIST_ARGSTR = ARGSTR_SRCLIST
BUNDLE_LIST_DESCR = 'srclist'

##############################

### Custom globals ###

PATH_SEPARATORS_LIST = ['/', '\\']
PATH_SEPARATORS_CAT = ''.join(PATH_SEPARATORS_LIST)

SYNC_MODE_GLOBAL = None

PATH_TYPE_UNKNOWN = 0
PATH_TYPE_FILE = 1
PATH_TYPE_DIR = 2
PATH_TYPE_DNE = 3

##############################


class MetaReadError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)


def pre_argparse():
    global ARGHLP_SRCLIST_FORMAT, ARGHLP_SRCLIST_ROOTED_FORMAT

    provided_srclist_delimiter = su.get_script_arg_values(ARGSTR_SRCLIST_DELIM)
    srclist_delimiter = provided_srclist_delimiter if provided_srclist_delimiter is not None else ARGDEF_SRCLIST_DELIM

    ARGHLP_SRCLIST_FORMAT = ' '.join([
        "\n(1) All 'src_path' line items (only when argument {} directory is provided)".format(ARGSTR_DST),
        "\n(2) A single 'src_path{}dst_dir' line at top followed by all 'src_path' line items".format(srclist_delimiter),
        "(where 'dst_dir' is a directory used for all items in the list)",
        "\n(3) All 'src_path{}dst_path' line items".format(srclist_delimiter)
    ])

    ARGHLP_SRCLIST_ROOTED_FORMAT = ' '.join([
        "A header line is expected containing 'src_rootdir[{}dst_rootdir]' to signify that".format(srclist_delimiter),
        "the folder structure from 'src_rootdir' down to 'src_path' for each 'src_path[{}dst_rootdir]'".format(srclist_delimiter),
        "line item will be replicated within the destination root directory.",
        "\nIf 'src_path' items are absolute paths, each is expected to start with the",
        "'src_rootdir' path EXACTLY as it appears in the header. If a 'src_path' item does not",
        "start with 'src_rootdir', the the 'src_path' item will be treated as a relative path",
        "within 'src_rootdir'.",
    ])


def argparser_init():
    global ARGHLP_SRCLIST_FORMAT, ARGHLP_SRCLIST_ROOTED_FORMAT

    parser = argparse.ArgumentParser(
        formatter_class=su.RawTextArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
        description=' '.join([
            "Copy(/link/move) a single file, whole file tree, or list of files.",
        ])
    )

    ## Positional arguments


    ## Optional arguments

    parser.add_argument(
        ARGBRV_SRC, ARGSTR_SRC,
        type=su.ARGTYPE_PATH(argstr=ARGSTR_SRC,
            existcheck_fn=os.path.exists,
            existcheck_reqval=True,
            accesscheck_reqtrue=os.R_OK),
        nargs='+',
        action='append',
        help=' '.join([
            "Path to source file or directory to be copied.",
        ])
    )

    parser.add_argument(
        ARGBRV_SRCLIST, ARGSTR_SRCLIST,
        type=su.ARGTYPE_PATH(argstr=ARGSTR_SRCLIST,
            existcheck_fn=os.path.isfile,
            existcheck_reqval=True,
            accesscheck_reqtrue=os.R_OK),
        nargs='+',
        action='append',
        help=' '.join([
            "Path to textfile list of 'src_path[{}dst_path]' copy tasks to be performed.".format(ARGSTR_SRCLIST_DELIM),
            ARGHLP_SRCLIST_FORMAT,
        ])
    )

    parser.add_argument(
        ARGBRV_SRCLIST_ROOTED, ARGSTR_SRCLIST_ROOTED,
        type=su.ARGTYPE_PATH(argstr=ARGSTR_SRCLIST_ROOTED,
            existcheck_fn=os.path.isfile,
            existcheck_reqval=True,
            accesscheck_reqtrue=os.R_OK),
        nargs='+',
        action='append',
        help=' '.join([
            "Path to textfile list of 'src_path[{}dst_rootdir]' copy tasks to be performed.".format(ARGSTR_SRCLIST_DELIM),
            ARGHLP_SRCLIST_ROOTED_FORMAT,
        ])
    )

    parser.add_argument(
        ARGBRV_DST, ARGSTR_DST,
        type=su.ARGTYPE_PATH(argstr=ARGSTR_DST,
            accesscheck_reqtrue=os.W_OK,
            accesscheck_parent_if_dne=True),
        help=' '.join([
            "Path to output file copy, or directory in which copies of source files will be created.",
            "To provide a destination directory that overrides all desination paths in source lists,",
            "use the {} argument instead of this argument.".format(ARGSTR_DSTDIR_GLOBAL)
        ])
    )
    parser.add_argument(
        ARGBRV_DSTDIR_GLOBAL, ARGSTR_DSTDIR_GLOBAL,
        type=su.ARGTYPE_PATH(argstr=ARGSTR_DSTDIR_GLOBAL,
            existcheck_fn=os.path.isfile,
            existcheck_reqval=False,
            accesscheck_reqtrue=os.W_OK,
            accesscheck_parent_if_dne=True),
        help=' '.join([
            "Path to output directory in which copies of source files will be created.",
            "This destination directory will override all desination paths in source lists.",
        ])
    )

    parser.add_argument(
        ARGBRV_COPY_METHOD, ARGSTR_COPY_METHOD,
        type=str,
        choices=ARGCHO_COPY_METHOD,
        default=ARGCHO_COPY_METHOD_LINK,
        help=' '.join([
            "Which copy method to use when performing all file transfers.",
        ])
    )
    parser.add_argument(
        ARGBRV_OVERWRITE, ARGSTR_OVERWRITE,
        action='store_true',
        help="[write me]"
    )

    parser.add_argument(
        ARGBRV_MINDEPTH, ARGSTR_MINDEPTH,
        type=su.ARGTYPE_NUM(argstr=ARGSTR_MINDEPTH,
            numeric_type=int, allow_neg=False, allow_zero=True, allow_inf=True),
        default=ARGDEF_MINDEPTH,
        help=' '.join([
            "Minimum depth of recursive search into source directories for files to copy.",
            "\nThe depth of a source directory's immediate contents is 1.",
        ])
    )

    parser.add_argument(
        ARGBRV_MAXDEPTH, ARGSTR_MAXDEPTH,
        type=su.ARGTYPE_NUM(argstr=ARGSTR_MAXDEPTH,
            numeric_type=int, allow_neg=False, allow_zero=True, allow_inf=True),
        default=ARGDEF_MAXDEPTH,
        help=' '.join([
            "Maximum depth of recursive search into source directories for files to copy.",
            "\nThe depth of a source directory's immediate contents is 1.",
        ])
    )

    parser.add_argument(
        ARGBRV_SYNC_TREE, ARGSTR_SYNC_TREE,
        action='store_true',
        help=' '.join([
            "Copy contents of source directories directly into destination directories."
            "\nIf neither the {} nor {} options are specified, source directory paths".format(ARGSTR_SYNC_TREE, ARGSTR_TRANSPLANT_TREE),
            "that end with '{}' are automatically treated this way.".format(PATH_SEPARATORS_LIST),
        ])
    )

    parser.add_argument(
        ARGBRV_TRANSPLANT_TREE, ARGSTR_TRANSPLANT_TREE,
        action='store_true',
        help=' '.join([
            "Copy contents of source directories into a folder under destination directories",
            "bearing their name (i.e. 'dstdir/srcdir_name/').",
            "\nIf neither the {} nor {} options are specified, source directory paths".format(ARGSTR_SYNC_TREE, ARGSTR_TRANSPLANT_TREE),
            "that do not end with '{}' are automatically treated this way.".format(PATH_SEPARATORS_LIST),
        ])
    )

    parser.add_argument(
        ARGSTR_SRCLIST_DELIM,
        type=str,
        default=ARGDEF_SRCLIST_DELIM,
        help=' '.join([
            "Delimiter used to separate source and destination paths in {} and {} textfiles.".format(ARGSTR_SRCLIST, ARGSTR_SRCLIST_ROOTED),
        ])
    )
    parser.add_argument(
        ARGSTR_SRCLIST_NOGLOB,
        action='store_true',
        help=' '.join([
            "Do not interpret '*' character as a wildcard for path-globbing in {} and {} textfiles.".format(ARGSTR_SRCLIST, ARGSTR_SRCLIST_ROOTED),
        ])
    )

    su.add_scheduler_arguments(parser,
        ARGDEF_JOB_ABBREV,
        ARGDEF_JOB_WALLTIME_HR,
        ARGDEF_JOB_MEMORY_GB,
        JOBSCRIPT_DIR,
        ARGDEF_BUNDLEDIR,
    )

    parser.add_argument(
        ARGBRV_SILENT, ARGSTR_SILENT,
        action='store_true',
        help="[write me]"
    )
    parser.add_argument(
        ARGBRV_DEBUG, ARGSTR_DEBUG,
        action='store_true',
        help="[write me]"
    )
    parser.add_argument(
        ARGBRV_DRYRUN, ARGSTR_DRYRUN,
        action='store_true',
        help="Print actions without executing."
    )

    return parser


def main():
    global SYNC_MODE_GLOBAL

    ### Parse script arguments
    pre_argparse()
    arg_parser = argparser_init()
    try:
        args = su.ArgumentPasser(PYTHON_EXE, SCRIPT_FILE, arg_parser, sys.argv)
    except su.ScriptArgumentError as e:
        arg_parser.error(str(e))
        args = None


    ### Further parse/adjust script argument values

    ## Restructure provided source arguments into flat lists
    for src_argstr in ARGGRP_SRC:
        if args.get(src_argstr) is not None:
            src_list_combined = []
            for src_list in args.get(src_argstr):
                src_list_combined.extend(src_list)
            args.set(ARGSTR_SRC, src_list_combined)

    su.set_default_jobscript(args, JOBSCRIPT_DIR)


    ### Validate argument values

    su.check_mut_excl_arggrp(args, ARGCOL_MUT_EXCL)

    arg_dst = args.get(ARGSTR_DST) if args.get(ARGSTR_DST) is not None else args.get(ARGSTR_DSTDIR_GLOBAL)

    if args.get(ARGSTR_SYNC_TREE):
        SYNC_MODE_GLOBAL = ARGMOD_SYNC_MODE_SYNC_TREE
    elif args.get(ARGSTR_TRANSPLANT_TREE):
        SYNC_MODE_GLOBAL = ARGMOD_SYNC_MODE_TRANSPLANT_TREE
    else:
        SYNC_MODE_GLOBAL = ARGMOD_SYNC_MODE_NULL

    src_list = []
    srclist_tasklists = []
    srclist_rooted_tasklists = []

    ## Parse and validate multiple source arguments

    if args.get(ARGSTR_SRC):
        if arg_dst is None:
            arg_parser.error("argument {}/{} is required when {} is provided".format(
                ARGSTR_DST, ARGSTR_DSTDIR_GLOBAL, ARGSTR_SRC))
        src_list.extend(args.get(ARGSTR_SRC))

    if args.get(ARGSTR_SRCLIST):
        for srclist_file in args.get(ARGSTR_SRCLIST):
            try:
                if arg_dst is None:
                    srclist_header = su.read_task_bundle(
                        srclist_file, ncol_min=1, ncol_max=2,
                        header_rows=1, read_header=True,
                        args_delim=args.get(ARGSTR_SRCLIST_DELIM)
                    )
                    if len(srclist_header) == 0:
                        continue
                    elif len(srclist_header) != 2:
                        raise su.DimensionError
                tasklist = su.Tasklist(
                    srclist_file, ncol_min=1, ncol_max=2, ncol_strict=True, ncol_strict_header_separate=True,
                    header_rows=1,
                    args_delim=args.get(ARGSTR_SRCLIST_DELIM)
                )
                if len(tasklist.header) == 0:
                    tasklist.header = None
                elif len(tasklist.tasks) == 0:
                    tasklist.tasks = [tasklist.header]
                    tasklist.header = None
                elif len(tasklist.tasks) > 0:
                    if len(tasklist.header) == 1:
                        if type(tasklist.tasks[0]) is list and len(tasklist.tasks[0]) == 2:
                            raise su.DimensionError
                        tasklist.tasks.insert(0, tasklist.header[0])
                        tasklist.header = None
                    elif len(tasklist.header) == 2:
                        if type(tasklist.tasks[0]) is list and len(tasklist.tasks[0]) == 2:
                            tasklist.tasks.insert(0, tasklist.header)
                            tasklist.header = None
                        elif type(tasklist.tasks[0]) is list and len(tasklist.tasks[0]) == 1:
                            tasklist.tasks.insert(0, [tasklist.header[0]])
                        else:
                            tasklist.tasks.insert(0, tasklist.header[0])
                if type(tasklist.header) is list and len(tasklist.header) == 0:
                    tasklist.header = None
            except su.DimensionError as e:
                traceback.print_exc()
                arg_parser.error("{} textfiles can be structured in one of the following "
                                 "formats:\n".format(ARGSTR_SRCLIST) + ARGHLP_SRCLIST_FORMAT)

            tasklist_src_dne = []
            for task in tasklist.tasks:
                task_src = task if type(task) is not list else task[0]
                if not os.path.exists(task_src):
                    tasklist_src_dne.append(task_src)
            if len(tasklist_src_dne) > 0:
                arg_parser.error("{} {} source paths do not exist:\n{}".format(
                    ARGSTR_SRCLIST, srclist_file, '\n'.join(tasklist_src_dne)
                ))

            if len(tasklist.tasks) > 0:
                srclist_tasklists.append(tasklist)

    if args.get(ARGSTR_SRCLIST_ROOTED):
        for srclist_file in args.get(ARGSTR_SRCLIST_ROOTED):
            try:
                if arg_dst is None:
                    srclist_first_two_lines = su.read_task_bundle(
                        srclist_file, ncol_min=1, ncol_max=2,
                        header_rows=2, read_header=True, allow_1d_task_list=False,
                        args_delim=args.get(ARGSTR_SRCLIST_DELIM)
                    )
                    if len(srclist_first_two_lines) == 0:
                        pass
                    elif 2 not in [len(line_items) for line_items in srclist_first_two_lines]:
                        raise su.DimensionError
                    else:
                        srclist_header = srclist_first_two_lines[0]
                        src_rootdir = srclist_header[0]
                        dst_rootdir = srclist_header[1] if len(srclist_header) == 2 else None
                        if not os.path.isdir(src_rootdir):
                            arg_parser.error(
                                "{} {} source root directory in header must be an existing directory: {}".format(
                                ARGSTR_SRCLIST_ROOTED, srclist_file, src_rootdir
                            ))
                        if dst_rootdir is not None and os.path.isfile(dst_rootdir):
                            arg_parser.error(
                                "{} {} destination root directory in header cannot be an existing file: {}".format(
                                ARGSTR_SRCLIST_ROOTED, srclist_file, dst_rootdir
                            ))
                tasklist = su.Tasklist(
                    srclist_file, ncol_min=1, ncol_max=2, ncol_strict=True, ncol_strict_header_separate=True,
                    header_rows=1,
                    args_delim=args.get(ARGSTR_SRCLIST_DELIM)
                )
            except su.DimensionError as e:
                traceback.print_exc()
                arg_parser.error("{} textfiles must be structured as follows:\n".format(ARGSTR_SRCLIST_ROOTED)
                                 + ARGHLP_SRCLIST_ROOTED_FORMAT)

            tasklist_src_dne = []
            for task in tasklist.tasks:
                task_src = task if type(task) is not list else task[0]
                if not os.path.exists(task_src):
                    tasklist_src_dne.append(task_src)
            if len(tasklist_src_dne) > 0:
                arg_parser.error("{} {} source paths do not exist:\n{}".format(
                    ARGSTR_SRCLIST_ROOTED, srclist_file, '\n'.join(tasklist_src_dne)
                ))

            if len(tasklist.tasks) > 0:
                srclist_rooted_tasklists.append(tasklist)


    arg_dst_can_be_file = False
    if args.get(ARGSTR_SRC) and args.get(ARGSTR_DST) and not os.path.isdir(args.get(ARGSTR_DST)):
        if len(src_list) == 1 and not (args.get(ARGSTR_SRCLIST) or args.get(ARGSTR_SRCLIST_ROOTED)):
            arg_dst_can_be_file = True


    ### Build list of tasks to be performed

    all_task_list = []

    ## Standardize source and destination paths to SYNC-style for file copy tasks

    for src_path in src_list:
        dst_path = arg_dst
        dst_path = adjust_dst_path(src_path, dst_path, arg_dst_can_be_file)
        all_task_list.append((src_path, dst_path))

    for tasklist in srclist_tasklists:

        tasklist_dst_can_be_file = False
        if arg_dst is not None and args.get(ARGSTR_DSTDIR_GLOBAL) is not None:
            tasklist_dst_dir = arg_dst
        elif tasklist.header is not None:
            tasklist_dst_dir = tasklist.header[1]
        else:
            tasklist_dst_dir = None
            tasklist_dst_can_be_file = True

        if tasklist_dst_dir is None:
            dst_path_type = PATH_TYPE_UNKNOWN
        else:
            if not os.path.exists(tasklist_dst_dir):
                dst_path_type = PATH_TYPE_DNE
            elif os.path.isdir(tasklist_dst_dir):
                dst_path_type = PATH_TYPE_DIR
            elif os.path.isfile(tasklist_dst_dir):
                dst_path_type = PATH_TYPE_FILE
            else:
                dst_path_type = PATH_TYPE_UNKNOWN

        for task in tasklist.tasks:
            src_path = task[0] if type(task) is list else task
            dst_path = tasklist_dst_dir if tasklist_dst_dir is not None else task[1]
            if not args.get(ARGSTR_SRCLIST_NOGLOB) and '*' in src_path:
                src_path_glob = glob.glob(src_path)
                for src_path in src_path_glob:
                    dst_path = adjust_dst_path(
                        src_path, dst_path, dst_can_be_file=False, dst_path_type=dst_path_type,
                        sync_mode_default=ARGMOD_SYNC_MODE_TRANSPLANT_TREE
                    )
                    all_task_list.append((src_path, dst_path))
            else:
                dst_path = adjust_dst_path(
                    src_path, dst_path, tasklist_dst_can_be_file, dst_path_type
                )
                all_task_list.append((src_path, dst_path))

    for tasklist in srclist_rooted_tasklists:

        src_rootdir = tasklist.header[0]

        if not os.path.isdir(src_rootdir):
            arg_parser.error(
                "{} {} source root directory in header must be an existing directory: {}".format(
                ARGSTR_SRCLIST_ROOTED, tasklist.tasklist_file, src_rootdir
            ))

        if arg_dst is not None and args.get(ARGSTR_DSTDIR_GLOBAL) is not None:
            tasklist_dst_rootdir = arg_dst
        elif len(tasklist.header) == 2:
            tasklist_dst_rootdir = tasklist.header[1]
        else:
            tasklist_dst_rootdir = None

        sync_mode = SYNC_MODE_GLOBAL
        if sync_mode == ARGMOD_SYNC_MODE_NULL:
            # Assume user expects the new destination directory to mirror the source directory
            sync_mode = ARGMOD_SYNC_MODE_SYNC_TREE

        src_rootdir_dirname = os.path.basename(src_rootdir.rstrip(PATH_SEPARATORS_CAT))
        if tasklist_dst_rootdir is not None and sync_mode == ARGMOD_SYNC_MODE_TRANSPLANT_TREE:
            tasklist_dst_rootdir = os.path.join(tasklist_dst_rootdir, src_rootdir_dirname)

        for task in tasklist.tasks:
            src_path = task[0] if type(task) is list else task
            dst_rootdir = tasklist_dst_rootdir if tasklist_dst_rootdir is not None else task[1]
            if os.path.isfile(dst_rootdir):
                arg_parser.error(
                    "{} {} destination root directory in header cannot be an existing file: {}".format(
                    ARGSTR_SRCLIST_ROOTED, tasklist.tasklist_file, dst_rootdir
                ))
            if not args.get(ARGSTR_SRCLIST_NOGLOB) and '*' in src_path:
                src_path_glob = glob.glob(src_path)
            else:
                src_path_glob = [src_path]
            for src_path in src_path_glob:

                src_path_from_root = src_path.replace(src_rootdir, '') if src_path.startswith(src_rootdir) else src_path
                if tasklist_dst_rootdir is None and sync_mode == ARGMOD_SYNC_MODE_TRANSPLANT_TREE:
                    dst_path = os.path.join(dst_rootdir, src_rootdir_dirname, src_path_from_root)
                else:
                    dst_path = os.path.join(dst_rootdir, src_path_from_root)

                all_task_list.append((src_path, dst_path))


    ### Create output directories if they don't already exist
    if not args.get(ARGSTR_DRYRUN):
        su.create_argument_directories(args, *ARGGRP_OUTDIR)


    ### Perform tasks

    error_trace = None
    try:
        if args.get(su.ARGSTR_SCHEDULER) is not None:
            ## Submit tasks to scheduler
            parent_tasks = all_task_list
            parent_args = args
            child_args = copy.deepcopy(args)
            child_args.unset(su.ARGGRP_SCHEDULER)
            child_args.unset(ARGSTR_TRANSPLANT_TREE)
            child_args.set(ARGSTR_SYNC_TREE)
            su.submit_tasks_to_scheduler(parent_args, parent_tasks,
                                         BUNDLE_TASK_ARGSTRS, BUNDLE_LIST_ARGSTR,
                                         child_args,
                                         task_items_descr=BUNDLE_LIST_DESCR,
                                         task_delim=ARGSTR_SRCLIST_DELIM,
                                         python_version_accepted_min=PYTHON_VERSION_ACCEPTED_MIN,
                                         dryrun=args.get(ARGSTR_DRYRUN))
            sys.exit(0)

        ## Perform tasks in serial
        perform_tasks(args, all_task_list)

    except KeyboardInterrupt:
        raise

    except Exception as e:
        error_trace = su.handle_task_exception(e, args, JOBSCRIPT_INIT)

    if type(args.get(su.ARGSTR_EMAIL)) is str:
        su.send_script_completion_email(args, error_trace)

    sys.exit(1 if error_trace is not None else 0)


def perform_tasks(args, task_list):

    copy_method_obj = copy.copy(COPY_METHOD_FUNCTION_DICT[args.get(ARGSTR_COPY_METHOD)])
    copy_method_obj.set_options(
        copy_overwrite=args.get(ARGSTR_OVERWRITE),
        dryrun=args.get(ARGSTR_DRYRUN),
        verbose=(not args.get(ARGSTR_SILENT)),
        debug=args.get(ARGSTR_DEBUG)
    )

    walk_object = su.WalkObject(
        mindepth=args.get(ARGSTR_MINDEPTH), maxdepth=args.get(ARGSTR_MAXDEPTH),
        copy_method=copy_method_obj, copy_overwrite=args.get(ARGSTR_OVERWRITE),
        transplant_tree=False, collapse_tree=args.get(ARGSTR_COLLAPSE_TREE),
        copy_dryrun=args.get(ARGSTR_DRYRUN), copy_silent=args.get(ARGSTR_SILENT), copy_debug=args.get(ARGSTR_DEBUG)
    )

    for task_srcpath, task_dstpath in task_list:
        if os.path.isfile(task_srcpath):
            task_srcfile = task_srcpath
            task_dstfile = task_dstpath
            copy_method_obj.exec(task_srcfile, task_dstfile)
        else:
            task_srcdir = task_srcpath
            task_dstdir = task_dstpath
            walk_object.walk(task_srcdir, task_dstdir)


def adjust_dst_path(src_path, dst_path, dst_can_be_file=False, dst_path_type=PATH_TYPE_UNKNOWN,
                    sync_mode_default=ARGMOD_SYNC_MODE_NULL):
    global SYNC_MODE_GLOBAL

    if dst_path_type == PATH_TYPE_DIR or (dst_path_type == PATH_TYPE_UNKNOWN and os.path.isdir(dst_path)):
        if os.path.isfile(src_path):
            dst_path = os.path.join(dst_path, os.path.basename(src_path))
        else:
            # src_path is a directory
            sync_mode = SYNC_MODE_GLOBAL
            if sync_mode == ARGMOD_SYNC_MODE_NULL:
                sync_mode = ARGMOD_SYNC_MODE_SYNC_TREE if su.endswith_one_of_coll(src_path, PATH_SEPARATORS_LIST) else ARGMOD_SYNC_MODE_TRANSPLANT_TREE
            if sync_mode == ARGMOD_SYNC_MODE_TRANSPLANT_TREE:
                dst_path = os.path.join(dst_path, os.path.basename(src_path.rstrip(PATH_SEPARATORS_CAT)))
            if not su.endswith_one_of_coll(dst_path, PATH_SEPARATORS_LIST):
                dst_path = dst_path+os.path.sep

    elif dst_path_type == PATH_TYPE_FILE or (dst_path_type == PATH_TYPE_UNKNOWN and os.path.isfile(dst_path)):
        if os.path.isdir(src_path):
            raise su.ScriptArgumentError(
                "source directory ({}) cannot overwrite existing destination file ({})".format(src_path, dst_path)
            )
        else:
            # src_path is a file
            pass

    else:
        # dst_path does not yet exist
        if os.path.isfile(src_path):
            if dst_can_be_file and not su.endswith_one_of_coll(dst_path, PATH_SEPARATORS_LIST):
                # dst_path will be the exact path of the file copy
                pass
            else:
                dst_path = os.path.join(dst_path, os.path.basename(src_path))
        else:
            # src_path is a directory; dst_path will be a new destination directory
            sync_mode = SYNC_MODE_GLOBAL if SYNC_MODE_GLOBAL != ARGMOD_SYNC_MODE_NULL else sync_mode_default
            if sync_mode == ARGMOD_SYNC_MODE_NULL:
                # Assume user expects the new destination directory to mirror the source directory
                sync_mode = ARGMOD_SYNC_MODE_SYNC_TREE
            if sync_mode == ARGMOD_SYNC_MODE_TRANSPLANT_TREE:
                dst_path = os.path.join(dst_path, os.path.basename(src_path.rstrip(PATH_SEPARATORS_CAT)))
            if not su.endswith_one_of_coll(dst_path, PATH_SEPARATORS_LIST):
                dst_path = dst_path+os.path.sep

    return dst_path



if __name__ == '__main__':
    main()
