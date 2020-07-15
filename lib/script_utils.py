
# Version 1.0; Erik Husby; Polar Geospatial Center, University of Minnesota; 2020


from __future__ import print_function
import argparse
import collections
import contextlib
import copy
import filecmp
import fnmatch as fnmatch_module
import functools
import logging
import math
import operator
import os
import platform
import re
import shlex
import shutil
import smtplib
import subprocess
import sys
import traceback
import types
import warnings
from datetime import datetime
from email.mime.text import MIMEText


PYTHON_VERSION_REQUIRED_MIN = "2.7"  # supports multiple dot notation


debug = print_function
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
warning = eprint
critical = eprint


class LoggerDebugFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.DEBUG
class LoggerInfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.INFO
LOGFMT_INFO = logging.Formatter("%(asctime)s (PID %(process)d) :: %(module)s.%(funcName)s :: %(levelname)s -- %(message)s")
LOGFMT_DEBUG = logging.Formatter("%(asctime)s (PID %(process)d) :: %(pathname)s:%(lineno)s:%(funcName)s :: %(levelname)s -- %(message)s")
def setup_logger(logger):
    logger.setLevel(logging.DEBUG)
    h1 = logging.StreamHandler(sys.stdout)
    h1.setLevel(logging.DEBUG)
    h1.setFormatter(LOGFMT_DEBUG)
    h1.addFilter(LoggerDebugFilter())
    h2 = logging.StreamHandler(sys.stdout)
    h2.setLevel(logging.INFO)
    h2.setFormatter(LOGFMT_INFO)
    h2.addFilter(LoggerInfoFilter())
    h3 = logging.StreamHandler(sys.stderr)
    h3.setLevel(logging.WARNING)
    h3.setFormatter(LOGFMT_DEBUG)
    logger.addHandler(h1)
    logger.addHandler(h2)
    logger.addHandler(h3)
def setup_logger_warnings():
    logger = logging.getLogger('py.warnings')
    logger.setLevel(logging.WARNING)
    h1 = logging.StreamHandler(sys.stderr)
    h1.setLevel(logging.WARNING)
    h1.setFormatter(LOGFMT_INFO)
    logger.addHandler(h1)

LOGGER = logging.getLogger(__name__)
if len(LOGGER.handlers) == 0:
    setup_logger(LOGGER)
    setup_logger_warnings()
    logging.captureWarnings(True)

debug = LOGGER.debug
info = LOGGER.info
warning = LOGGER.warning
error = LOGGER.error
critical = LOGGER.critical

print = info
eprint = error


class VersionError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class DeveloperError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class ScriptArgumentError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class InvalidArgumentError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class ExternalError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class DimensionError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)


class VersionString:
    def __init__(self, ver_str_or_num):
        self.ver_str = str(ver_str_or_num)
        self.ver_num_list = [int(n) for n in self.ver_str.split('.')]
    def get_comparable_lists(self, other):
        this_list = list(self.ver_num_list)
        other_list = list(other.ver_num_list)
        if len(this_list) < len(other_list):
            this_list.extend([0]*(len(other_list)-len(this_list)))
        elif len(this_list) > len(other_list):
            other_list.extend([0]*(len(this_list)-len(other_list)))
        return this_list, other_list
    def __str__(self):
        return self.ver_str
    def __repr__(self):
        return self.ver_str
    def __compare_absolute(self, other, inequality=False):
        this_ver_num_list, other_ver_num_list = self.get_comparable_lists(other)
        for i in range(len(this_ver_num_list)):
            if this_ver_num_list[i] != other_ver_num_list[i]:
                return inequality
        return (not inequality)
    def __compare_relative(self, other, op, allow_equal=False):
        this_ver_num_list, other_ver_num_list = self.get_comparable_lists(other)
        for i in range(len(this_ver_num_list)):
            if this_ver_num_list[i] != other_ver_num_list[i]:
                return op(this_ver_num_list[i], other_ver_num_list[i])
        return allow_equal
    def __eq__(self, other):
        return self.__compare_absolute(other, inequality=False)
    def __ne__(self, other):
        return self.__compare_absolute(other, inequality=True)
    def __gt__(self, other):
        return self.__compare_relative(other, operator.gt, allow_equal=False)
    def __ge__(self, other):
        return self.__compare_relative(other, operator.gt, allow_equal=True)
    def __lt__(self, other):
        return self.__compare_relative(other, operator.lt, allow_equal=False)
    def __le__(self, other):
        return self.__compare_relative(other, operator.le, allow_equal=True)

PYTHON_VERSION = VersionString(platform.python_version())
if PYTHON_VERSION < VersionString(PYTHON_VERSION_REQUIRED_MIN):
    raise VersionError("Python version ({}) is below required minimum ({})".format(
        PYTHON_VERSION, PYTHON_VERSION_REQUIRED_MIN))

if PYTHON_VERSION < VersionString(3):
    PYTHON_VERSION_LT_3 = True
    from StringIO import StringIO
else:
    PYTHON_VERSION_LT_3 = False
    from io import StringIO


SYSTYPE = platform.system()
SYSTYPE_WINDOWS = 'Windows'
SYSTYPE_LINUX = 'Linux'
SYSTYPE_DARWIN = 'Darwin'
SYSTYPE_CHOICES = [
    SYSTYPE_WINDOWS,
    SYSTYPE_LINUX,
    SYSTYPE_DARWIN
]
if SYSTYPE not in SYSTYPE_CHOICES:
    raise DeveloperError("platform.system() value '{}' is not supported".format(SYSTYPE))


@contextlib.contextmanager
def capture_stdout_stderr():
    oldout, olderr = sys.stdout, sys.stderr
    out = [StringIO(), StringIO()]
    try:
        sys.stdout, sys.stderr = out
        yield out
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()


showwarning_stderr = warnings.showwarning
def showwarning_stdout(message, category, filename, lineno, file=None, line=None):
    sys.stdout.write(warnings.formatwarning(message, category, filename, lineno))


def startswith_one_of_coll(check_string, string_starting_coll, case_sensitive=True, return_match=False):
    for s_start in string_starting_coll:
        if check_string.startswith(s_start) or (not case_sensitive and check_string.lower().startswith(s_start.lower())):
            return s_start if return_match else True
    return None if return_match else False

def starts_one_of_coll(string_starting, string_coll, case_sensitive=True, return_match=False):
    for s in string_coll:
        if s.startswith(string_starting) or (not case_sensitive and s.lower().startswith(string_starting.lower())):
            return s if return_match else True
    return None if return_match else False

def endswith_one_of_coll(check_string, string_ending_coll, case_sensitive=True, return_match=False):
    for s_end in string_ending_coll:
        if check_string.endswith(s_end) or (not case_sensitive and check_string.lower().endswith(s_end.lower())):
            return s_end if return_match else True
    return None if return_match else False

def ends_one_of_coll(string_ending, string_coll, case_sensitive=True, return_match=False):
    for s in string_coll:
        if s.endswith(string_ending) or (not case_sensitive and s.lower().endswith(string_ending.lower())):
            return s if return_match else True
    return None if return_match else False


def execute_command(cmd_str=None, tokenize_cmd=False,
                    arg_list=[], shell=None,
                    cwd=None, env=None,
                    executable=None, bufsize=-1,
                    stdin=None, stdout=None, stderr=None,
                    send_stderr_to_stdout=False, silent=False,
                    return_streams=False, return_popen=False,
                    print_stderr_in_failure=True,
                    throw_exception_in_failure=True,
                    print_failure_info=False,
                    print_begin_info=False,
                    print_end_info=False):
    if [cmd_str is not None, len(arg_list) > 0].count(True) != 1:
        raise InvalidArgumentError("Only one of (`cmd_str`, `arg_list`) arguments must be provide")
    if return_streams and return_popen:
        raise InvalidArgumentError("Only one of (`return_streams`, `return_popen`) arguments may be provided")
    if cmd_str is not None:
        args = shlex.split(cmd_str) if tokenize_cmd else cmd_str
        if shell is None:
            shell = True
    else:
        args = arg_list
        if shell is None:
            shell = False
        cmd_str = ' '.join(arg_list)
    if silent or return_streams:
        if stdout is None:
            stdout = subprocess.PIPE
        if stderr is None and not send_stderr_to_stdout:
            stderr = subprocess.PIPE
    if send_stderr_to_stdout:
        if stderr is not None:
            raise InvalidArgumentError("`stderr` argument must be None when `send_stderr_to_stdout` argument is True")
        stderr = subprocess.STDOUT
    proc = subprocess.Popen(args, bufsize=bufsize, executable=executable,
                            stdin=stdin, stdout=stdout, stderr=stderr,
                            shell=shell, cwd=cwd, env=env,
                            universal_newlines=True)
    proc_pid = proc.pid
    if print_begin_info:
        print('Beginning external call (PID {}): """ {} """'.format(proc_pid, cmd_str))
    stdout, stderr = proc.communicate()
    return_code = proc.returncode
    if not silent:
        if stdout is not None:
            sys.stdout.write(stdout)
        if stderr is not None:
            sys.stderr.write(stderr)
            print_stderr_in_failure = False
    if return_code != 0:
        if print_stderr_in_failure and stderr is not None:
            sys.stderr.write(stderr)
        errmsg = 'External call (PID {}) failed with non-zero exit status ({}): """ {} """'.format(proc_pid, return_code, cmd_str)
        if print_failure_info:
            eprint(print_failure_info)
        if throw_exception_in_failure:
            raise ExternalError(errmsg)
    if print_end_info:
        print("External call (PID {}) completed successfully".format(proc_pid))
    if return_popen:
        return proc
    else:
        return (return_code, stdout, stderr) if return_streams else return_code


def send_email(to_addr, subject, body, from_addr=None):
    if from_addr is None:
        platform_node = platform.node()
        from_addr = platform_node if platform_node is not None else 'your-computer'
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr
    s = smtplib.SMTP('localhost')
    s.sendmail(to_addr, [to_addr], msg.as_string())
    s.quit()


COPY_METHOD_VERB_TO_GERUND_DICT = {
    'link': 'linking',
    'move': 'moving',
    'copy': 'copying'
}
COPY_METHOD_SHPROGS_LINUX_TO_WINDOWS_DICT = {
    'ln': 'mklink',
    'cp': 'copy',
    'mv': 'move'
}
COPY_METHOD_SHPROGS_ACTION_VERB_DICT = dict()
for prog in ['ln', 'mklink']:
    COPY_METHOD_SHPROGS_ACTION_VERB_DICT[prog] = 'linking'
for prog in ['cp', 'copy']:
    COPY_METHOD_SHPROGS_ACTION_VERB_DICT[prog] = 'copying'
for prog in ['mv', 'move']:
    COPY_METHOD_SHPROGS_ACTION_VERB_DICT[prog] = 'moving'

class CopyMethod:
    def __init__(self,
                 copy_fn, copy_fn_name=None, action_verb=None,
                 reverse_args=None, copy_shcmd_is_fmtstr=False):
        copy_shcmd = None
        copy_shprog = None
        copy_fn_type = type(copy_fn)

        if copy_fn_name is None and copy_fn_type is not str:
            if copy_fn_type in [types.FunctionType, types.BuiltinFunctionType, types.BuiltinMethodType]:
                try:
                    copy_fn_name = copy_fn_type.__name__
                except AttributeError:
                    pass
            if copy_fn_name is None:
                copy_fn_name = str(copy_fn)
        
        if copy_fn_type is str:
            copy_shcmd = copy_fn.strip()
            copy_fn = None
            copy_shcmd_parts = copy_shcmd.split(' ')
            copy_shprog = copy_shcmd_parts[0]
            if SYSTYPE == SYSTYPE_WINDOWS and copy_shprog in COPY_METHOD_SHPROGS_LINUX_TO_WINDOWS_DICT and len(copy_shcmd_parts) == 1:
                copy_shprog = COPY_METHOD_SHPROGS_LINUX_TO_WINDOWS_DICT[copy_shprog]
                copy_shcmd_parts[0] = copy_shprog
                copy_shcmd = ' '.join(copy_shcmd_parts)
            if copy_shprog == 'mklink' and reverse_args is None:
                reverse_args = True
            if action_verb is None and copy_shprog in COPY_METHOD_SHPROGS_ACTION_VERB_DICT:
                action_verb = COPY_METHOD_SHPROGS_ACTION_VERB_DICT[copy_shprog]

        if action_verb is None:
            if copy_shprog is None:
                copy_shprog_verbose = None
            elif copy_shprog in COPY_METHOD_SHPROGS_LINUX_TO_WINDOWS_DICT:
                copy_shprog_verbose = COPY_METHOD_SHPROGS_LINUX_TO_WINDOWS_DICT[copy_shprog]
            else:
                copy_shprog_verbose = copy_shprog
            if copy_shprog_verbose is not None or copy_fn_name is not None:
                for verb in COPY_METHOD_VERB_TO_GERUND_DICT:
                    if (   (copy_shprog_verbose is not None and verb in copy_shprog_verbose)
                        or (copy_fn_name is not None and verb in copy_fn_name)):
                        action_verb = COPY_METHOD_VERB_TO_GERUND_DICT[verb]
                        break
            if action_verb is None:
                action_verb = 'transferring'
        action_verb = action_verb.upper()

        if reverse_args is None:
            reverse_args = False

        self.copy_fn = copy_fn
        self.copy_fn_name = copy_fn_name
        self.action_verb = action_verb
        self.reverse_args = reverse_args
        self.copy_shcmd = copy_shcmd
        self.copy_shprog = copy_shprog
        self.copy_shcmd_is_fmtstr = copy_shcmd_is_fmtstr

        self.copy_overwrite = False
        self.dryrun = False
        self.verbose = True
        self.debug = False

    def __copy__(self):
        copy_method = CopyMethod(self.copy_fn, self.copy_fn_name, self.action_verb,
                                 self.reverse_args, self.copy_shcmd_is_fmtstr)
        copy_method.set_options(self.copy_overwrite, self.dryrun, self.verbose, self.debug)
        return copy_method

    def set_options(self, copy_overwrite=None, dryrun=None, verbose=None, debug=None):
        if copy_overwrite is not None:
            self.copy_overwrite = copy_overwrite
        if dryrun is not None:
            self.dryrun = dryrun
        if verbose is not None:
            self.verbose = verbose
        if debug is not None:
            self.debug = debug

    def exec(self, srcfile, dstfile):
        copy_shcmd_full = None
        if self.copy_shcmd is not None:
            if self.copy_shcmd_is_fmtstr:
                copy_shcmd_full = self.copy_shcmd.format(srcfile, dstfile)
            elif self.reverse_args:
                copy_shcmd_full = "{} {} {}".format(self.copy_shcmd, dstfile, srcfile)
            else:
                copy_shcmd_full = "{} {} {}".format(self.copy_shcmd, srcfile, dstfile)

        dstfile_exists = os.path.isfile(dstfile)
        if dstfile_exists:
            if self.copy_overwrite:
                proceed_with_copy = True
                overwrite_action = "OVERWRITING"
            else:
                proceed_with_copy = False
                dstfile_is_srcfile = filecmp.cmp(srcfile, dstfile) if self.action_verb in ['HARDLINKING', 'LINKING'] else False
                if dstfile_is_srcfile:
                    overwrite_action = "SKIPPING; correct link already exists"
                else:
                    overwrite_action = "SKIPPING; destination file already exists"
        else:
            proceed_with_copy = True
            overwrite_action = ''

        if self.verbose:
            print("{}{}: {} -> {}{}".format(
                "(dryrun) " if self.dryrun else '', self.action_verb,
                srcfile, dstfile,
                " ({})".format(overwrite_action) if dstfile_exists else ''
            ))

        if not proceed_with_copy:
            return

        if self.debug and proceed_with_copy:
            if copy_shcmd_full is not None:
                print(copy_shcmd_full)
            else:
                print("{}({}, {})".format(self.copy_fn_name, srcfile, dstfile))

        if not self.dryrun and proceed_with_copy:
            if dstfile_exists and self.copy_overwrite:
                os.remove(dstfile)
            if copy_shcmd_full is not None:
                execute_command(copy_shcmd_full)
            elif self.reverse_args:
                self.copy_fn(dstfile, srcfile)
            else:
                self.copy_fn(srcfile, dstfile)

if SYSTYPE == SYSTYPE_WINDOWS:
    COPY_METHOD_HARDLINK_SYSTEM = CopyMethod('mklink /H', action_verb='hardlinking', reverse_args=True)
    COPY_METHOD_SYMLINK_SYSTEM = CopyMethod('mklink', action_verb='symlinking', reverse_args=True)
    COPY_METHOD_COPY_SYSTEM = CopyMethod('copy', action_verb='copying')
    COPY_METHOD_MOVE_SYSTEM = CopyMethod('move', action_verb='moving')
else:
    COPY_METHOD_HARDLINK_SYSTEM = CopyMethod('ln', action_verb='hardlinking')
    COPY_METHOD_SYMLINK_SYSTEM = CopyMethod('ln -s', action_verb='symlinking')
    COPY_METHOD_COPY_SYSTEM = CopyMethod('cp', action_verb='copying')
    COPY_METHOD_MOVE_SYSTEM = CopyMethod('mv', action_verb='moving')
COPY_METHOD_SHPROGS = set([cm.copy_shprog for cm in [
        COPY_METHOD_HARDLINK_SYSTEM,
        COPY_METHOD_SYMLINK_SYSTEM,
        COPY_METHOD_COPY_SYSTEM,
        COPY_METHOD_MOVE_SYSTEM
    ]])

try:
    COPY_METHOD_HARDLINK = CopyMethod(os.link, 'os.link', 'hardlinking')
    COPY_METHOD_SYMLINK = CopyMethod(os.symlink, 'os.symlink', 'symlinking')
except AttributeError:
    COPY_METHOD_HARDLINK = COPY_METHOD_HARDLINK_SYSTEM
    COPY_METHOD_SYMLINK = COPY_METHOD_SYMLINK_SYSTEM

COPY_METHOD_COPY_BASIC = CopyMethod(shutil.copyfile, 'shutil.copyfile', 'copying')
COPY_METHOD_COPY_PERMS = CopyMethod(shutil.copy, 'shutil.copy', 'copying')
COPY_METHOD_COPY_META = CopyMethod(shutil.copy2, 'shutil.copy2', 'copying')
COPY_METHOD_COPY_DEFAULT = COPY_METHOD_COPY_META

COPY_METHOD_MOVE = CopyMethod(shutil.move, 'shutil.move', 'moving')

COPY_METHOD_DICT = {
    'link': COPY_METHOD_HARDLINK,
    'hardlink': COPY_METHOD_HARDLINK,
    'symlink': COPY_METHOD_SYMLINK,
    'copy': COPY_METHOD_COPY_DEFAULT,
    'copy-basic': COPY_METHOD_COPY_BASIC,
    'copy-perms': COPY_METHOD_COPY_PERMS,
    'copy-meta': COPY_METHOD_COPY_META,
    'move': COPY_METHOD_MOVE,
}

try:
    WALK_LIST_FUNCTION_DEFAULT = os.scandir
except AttributeError:
    WALK_LIST_FUNCTION_DEFAULT = os.listdir
try:
    WALK_REMATCH_FUNCTION_DEFAULT = re.fullmatch
except AttributeError:
    WALK_REMATCH_FUNCTION_DEFAULT = re.match
WALK_REMATCH_PARTIAL_FUNCTION_DEFAULT = re.search
WALK_RESUB_FUNCTION_DEFAULT = re.sub


def walk_simple(srcdir, mindepth, maxdepth, list_function=WALK_LIST_FUNCTION_DEFAULT):
    if not os.path.isdir(srcdir):
        raise InvalidArgumentError("`srcdir` directory does not exist: {}".format(srcdir))
    if mindepth < 0 or maxdepth < 0:
        raise InvalidArgumentError("depth arguments must be >= 0")
    for x in _walk_simple(srcdir, 1, mindepth, maxdepth, list_function):
        yield x

def _walk_simple(rootdir, depth, mindepth, maxdepth, list_function):
    if depth > maxdepth:
        return
    dnames, fnames = [], []
    for dirent in list_function(rootdir):
        if list_function is os.listdir:
            pname = dirent
            dirent_is_dir = os.path.isdir(os.path.join(rootdir, pname))
        else:
            pname = dirent.name
            dirent_is_dir = dirent.is_dir()
        (dnames if dirent_is_dir else fnames).append(pname)
    if mindepth <= depth <= maxdepth:
        yield rootdir, dnames, fnames
    if depth < maxdepth:
        for dname in dnames:
            for x in _walk_simple(os.path.join(rootdir, dname), depth+1, mindepth, maxdepth, list_function):
                yield x


class WalkObject:
    def __init__(self,
        mindepth=0, maxdepth=float('inf'), dmatch_maxdepth=None,
        fmatch=None, fmatch_re=None, fexcl=None, fexcl_re=None,
        dmatch=None, dmatch_re=None, dexcl=None, dexcl_re=None,
        fsub=None, dsub=None,
        copy_method=None, copy_overwrite=False, transplant_tree=False, collapse_tree=False,
        copy_dryrun=False, copy_silent=False, copy_debug=False,
        mkdir_upon_file_copy=False,
        allow_nonstd_shprogs=False,
        copy_shcmd_fmtstr=None,
        list_function=None,
        rematch_function=None,
        resub_function=None,
        rematch_partial=False):

        if mindepth < 0 or maxdepth < 0 or (dmatch_maxdepth is not None and dmatch_maxdepth < 0):
            raise InvalidArgumentError("depth arguments must be >= 0")
        if copy_method and copy_shcmd_fmtstr:
            raise InvalidArgumentError("`copy_method` and `copy_shcmd_fmtstr` arguments are mutually exclusive")
        if copy_shcmd_fmtstr is not None:
            copy_method = copy_shcmd_fmtstr
            copy_method_is_fmtstr = True
        else:
            copy_method_is_fmtstr = False
        if copy_silent and copy_dryrun:
            raise InvalidArgumentError("`copy_silent` and `copy_dryrun` arguments are mutually exclusive")
        if list_function not in [None, os.listdir, os.scandir]:
            raise InvalidArgumentError("`list_function` must be either os.listdir or os.scandir")

        if dmatch is None and dmatch_re is None:
            dmatch_nomaxdepth = True
        else:
            if copy_method is None:
                dmatch_nomaxdepth = (dmatch_maxdepth is None)
            else:
                dmatch_nomaxdepth = False
        if dmatch_maxdepth is None:
            dmatch_maxdepth = float('inf')

        list_function_given, rematch_function_given, resub_function_given = [
            item is not None for item in [
                list_function, rematch_function, resub_function
            ]
        ]
        if list_function is None:
            list_function = WALK_LIST_FUNCTION_DEFAULT
        if rematch_function is None:
            rematch_function = WALK_REMATCH_PARTIAL_FUNCTION_DEFAULT if rematch_partial else WALK_REMATCH_FUNCTION_DEFAULT
        if resub_function is None:
            resub_function = WALK_RESUB_FUNCTION_DEFAULT

        fmatch, fmatch_re, fexcl, fexcl_re, \
        dmatch, dmatch_re, dexcl, dexcl_re, \
        fsub, dsub = [
            item if (item is None or type(item) is list) else (list(item) if type(item) in tuple else [item]) for item in [
                fmatch, fmatch_re, fexcl, fexcl_re,
                dmatch, dmatch_re, dexcl, dexcl_re,
                fsub, dsub
            ]
        ]

        fsub, dsub = [
            item if (item is None or type(item[0]) in (list, tuple)) else [item] for item in [
                fsub, dsub
            ]
        ]

        fsub_patt = None
        dsub_patt = None
        try:
            if fsub is not None:
                fsub_patt, fsub_repl = list(zip(*fsub))
                fsub_patt = list(fsub_patt)
                if len(fsub_patt) != len(fsub_repl):
                    raise ValueError
            if dsub is not None:
                dsub_patt, dsub_repl = list(zip(*dsub))
                dsub_patt = list(dsub_patt)
                if len(dsub_patt) != len(dsub_repl):
                    raise ValueError
        except ValueError:
            raise InvalidArgumentError("resub arguments must be provided in (pattern, repl_str) groups")

        pattern_coll = [
            patt_list for patt_list in [
                fmatch, fmatch_re, fexcl, fexcl_re,
                dmatch, dmatch_re, dexcl, dexcl_re,
                fsub_patt, dsub_patt
            ] if patt_list is not None
        ]

        for patt_list in pattern_coll:
            for i, pattern in enumerate(patt_list):

                if patt_list in [fmatch, fexcl, dmatch, dexcl]:
                    pattern = fnmatch_module.translate(pattern)

                re_pattern = re.compile(pattern) if type(pattern) is str else pattern
                try:
                    re_pattern_str = re_pattern.pattern
                except AttributeError:
                    traceback.print_exc()
                    raise InvalidArgumentError("regex match/sub argument is invalid")
                if (    not rematch_function_given
                    and rematch_function is re.match and patt_list in [fmatch, dmatch]
                    and not pattern.endswith('$') and not rematch_partial):
                    if type(pattern) is str:
                        re_pattern = re.compile(pattern+'$')
                    else:
                        warning("`re.fullmatch` function is not supported, so `re.match` will be used instead "
                                "and argument regex match pattern '{}' may hit on a partial match")
                patt_list[i] = re_pattern

        fname_rematch = []
        for patt_list in [fmatch, fmatch_re]:
            if patt_list is not None:
                fname_rematch.extend(patt_list)
        fname_reexcl = []
        for patt_list in [fexcl, fexcl_re]:
            if patt_list is not None:
                fname_reexcl.extend(patt_list)
        dname_rematch = []
        for patt_list in [dmatch, dmatch_re]:
            if patt_list is not None:
                dname_rematch.extend(patt_list)
        dname_reexcl = []
        for patt_list in [dexcl, dexcl_re]:
            if patt_list is not None:
                dname_reexcl.extend(patt_list)
        fname_resub = list(zip(fsub_patt, fsub_repl)) if fsub is not None else None
        dname_resub = list(zip(dsub_patt, dsub_repl)) if dsub is not None else None

        if copy_method is not None:
            if type(copy_method) is CopyMethod:
                copy_method = copy.copy(copy_method)
            elif type(copy_method) is str:
                if copy_method in COPY_METHOD_DICT:
                    copy_method = COPY_METHOD_DICT[copy_method]
                else:
                    copy_method = CopyMethod(copy_method, copy_shcmd_is_fmtstr=copy_method_is_fmtstr)
                    if copy_method.copy_shprog not in COPY_METHOD_SHPROGS and not allow_nonstd_shprogs:
                        raise InvalidArgumentError("`copy_method` shell program '{}' is nonstandard and not allowed".format(copy_method.copy_shprog))
            else:
                copy_method = CopyMethod(copy_method)
            copy_method.set_options(copy_overwrite=copy_overwrite, dryrun=copy_dryrun, verbose=(not copy_silent), debug=copy_debug)

        self.srcdir = None
        self.dstdir = None
        self.mindepth = mindepth
        self.maxdepth = maxdepth
        self.dmatch_maxdepth = dmatch_maxdepth
        self.dmatch_nomaxdepth = dmatch_nomaxdepth
        self.fname_rematch = fname_rematch
        self.fname_reexcl = fname_reexcl
        self.dname_rematch = dname_rematch
        self.dname_reexcl = dname_reexcl
        self.fname_resub = fname_resub
        self.dname_resub = dname_resub
        self.copy_method = copy_method
        self.copy_method_inst = copy_method
        self.transplant_tree = transplant_tree
        self.collapse_tree = collapse_tree
        self.collapse_tree_inst = collapse_tree
        self.mkdir_upon_file_copy = mkdir_upon_file_copy
        self.list_function = list_function
        self.rematch_function = rematch_function
        self.resub_function = resub_function

    def walk(self,
             srcdir, dstdir=None,
             copy_overwrite=None, transplant_tree=None, collapse_tree=None):
        if copy_overwrite is None:
            copy_overwrite = self.copy_method.copy_overwrite
        if transplant_tree is None:
            transplant_tree = self.transplant_tree
        if collapse_tree is None:
            collapse_tree = self.collapse_tree

        srcdir = os.path.normpath(os.path.expanduser(srcdir))
        if not os.path.isdir(srcdir):
            raise InvalidArgumentError("`srcdir` directory does not exist: {}".format(srcdir))
        if dstdir is not None:
            dstdir = os.path.normpath(os.path.expanduser(dstdir))
        if transplant_tree:
            dstdir = os.path.join(dstdir, os.path.basename(srcdir))

        self.srcdir = srcdir
        self.dstdir = dstdir
        if copy_overwrite != self.copy_method.copy_overwrite:
            self.copy_method_inst = copy.copy(self.copy_method)
            self.copy_method_inst.set_options(copy_overwrite=copy_overwrite)
        else:
            self.copy_method_inst = self.copy_method
        self.collapse_tree_inst = collapse_tree

        if self.copy_method is not None and self.dstdir is not None and os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

        depth = 1
        dmatch_depth = -1 if self.dmatch_nomaxdepth else 0

        for x in self._walk(self.srcdir, self.dstdir, depth, dmatch_depth):
            yield x

    def _walk(self, srcdir, dstdir, depth, dmatch_depth):
        if depth > self.maxdepth:
            return
        if depth == 1 and dmatch_depth == 0:
            srcdname = os.path.basename(srcdir)
            srcdname_match = True
            if self.dname_rematch:
                srcdname_match = False
                for re_pattern in self.dname_rematch:
                    srcdname_match = self.rematch_function(re_pattern, srcdname)
                    if srcdname_match:
                        break
            if self.dname_reexcl and srcdname_match:
                for re_pattern in self.dname_reexcl:
                    srcdname_match = (not self.rematch_function(re_pattern, srcdname))
                    if not srcdname_match:
                        break
            if srcdname_match:
                dmatch_depth = 1

        srcdname_passes = (dmatch_depth <= self.dmatch_maxdepth and dmatch_depth != 0)

        if dstdir is None or self.copy_method_inst is None:
            dstdir_exists = False
        elif os.path.isdir(dstdir):
            dstdir_exists = True
        elif self.mkdir_upon_file_copy:
            dstdir_exists = False
        elif srcdname_passes:
            os.makedirs(dstdir)
            dstdir_exists = True
        else:
            dstdir_exists = False

        dnames, fnames_filtered = [], []
        dnames_pass = [] if (self.dname_rematch or self.dname_reexcl) else None
        for dirent in self.list_function(srcdir):
            if self.list_function is os.listdir:
                pname = dirent
                dirent_is_dir = os.path.isdir(os.path.join(srcdir, pname))
            else:
                pname = dirent.name
                dirent_is_dir = dirent.is_dir()
            if dirent_is_dir:
                dnames.append(pname)
                if dnames_pass is not None:
                    dname_match = True
                    if self.dname_rematch:
                        dname_match = False
                        for re_pattern in self.dname_rematch:
                            dname_match = self.rematch_function(re_pattern, pname)
                            if dname_match:
                                break
                    if self.dname_reexcl and dname_match:
                        for re_pattern in self.dname_reexcl:
                            dname_match = (not self.rematch_function(re_pattern, pname))
                            if not dname_match:
                                break
                    if dname_match:
                        dnames_pass.append(True)
                    else:
                        dnames_pass.append(False)
            elif srcdname_passes:
                fname_match = True
                if self.fname_rematch:
                    fname_match = False
                    for re_pattern in self.fname_rematch:
                        fname_match = self.rematch_function(re_pattern, pname)
                        if fname_match:
                            break
                if self.fname_reexcl and fname_match:
                    for re_pattern in self.fname_reexcl:
                        fname_match = (not self.rematch_function(re_pattern, pname))
                        if not fname_match:
                            break
                if fname_match:
                    fnames_filtered.append(pname)

        if depth >= self.mindepth:
            if srcdname_passes and self.copy_method_inst is not None and dstdir is not None:
                if not self.copy_method_inst.dryrun and not dstdir_exists and (not self.mkdir_upon_file_copy or fnames_filtered):
                    os.makedirs(dstdir)
                    dstdir_exists = True
                for fname in fnames_filtered:
                    srcfile = os.path.join(srcdir, fname)
                    if self.fname_resub:
                        for re_pattern, repl_str in self.fname_resub:
                            fname = self.resub_function(re_pattern, repl_str, fname)
                    dstfile = os.path.join(dstdir, fname)
                    self.copy_method_inst.exec(srcfile, dstfile)
            if srcdname_passes and not self.dmatch_nomaxdepth:
                dnames_filtered = dnames
            else:
                dnames_filtered = dnames if dnames_pass is None else [dn for i, dn in enumerate(dnames) if dnames_pass[i]]
            yield srcdir, dnames_filtered, fnames_filtered

        if dnames and depth < self.maxdepth:
            depth_next = depth + 1
            dmatch_depth_next_pass = dmatch_depth if dmatch_depth <  0 else 1
            dmatch_depth_next_fail = dmatch_depth if dmatch_depth <= 0 else dmatch_depth + 1
            for i, dn in enumerate(dnames):
                srcdir_next = os.path.join(srcdir, dn)
                dmatch_depth_next = dmatch_depth_next_pass if dnames_pass is None or dnames_pass[i] else dmatch_depth_next_fail
                if dstdir is None:
                    dstdir_next = None
                elif self.collapse_tree_inst:
                    dstdir_next = dstdir
                else:
                    dstdname_next = dn
                    if self.dname_resub:
                        for re_pattern, repl_str in self.dname_resub:
                            dstdname_next = self.resub_function(re_pattern, repl_str, dstdname_next)
                    dstdir_next = os.path.join(dstdir, dstdname_next)
                for x in self._walk(srcdir_next, dstdir_next, depth_next, dmatch_depth_next):
                    yield x


def walk(srcdir, dstdir=None,
        mindepth=0, maxdepth=float('inf'), dmatch_maxdepth=None,
        fmatch=None, fmatch_re=None, fexcl=None, fexcl_re=None,
        dmatch=None, dmatch_re=None, dexcl=None, dexcl_re=None,
        fsub=None, dsub=None,
        copy_method=None, copy_overwrite=False, transplant_tree=False, collapse_tree=False,
        copy_dryrun=False, copy_silent=False, copy_debug=False,
        mkdir_upon_file_copy=False,
        allow_nonstd_shprogs=False,
        copy_shcmd_fmtstr=None,
        list_function=None,
        rematch_function=None,
        resub_function=None,
        rematch_partial=False):
    if not os.path.isdir(srcdir):
        raise InvalidArgumentError("`srcdir` directory does not exist: {}".format(srcdir))
    # if dstdir is not None and copy_method is None:
    #     raise InvalidArgumentError("`copy_method` must be provided to utilize `dstdir` argument")
    if dstdir is None and (copy_method or copy_silent or copy_dryrun or copy_shcmd_fmtstr):
        raise InvalidArgumentError("`dstdir` must be provided to use file copy options")
    walk_object = WalkObject(
        mindepth, maxdepth, dmatch_maxdepth,
        fmatch, fmatch_re, fexcl, fexcl_re,
        dmatch, dmatch_re, dexcl, dexcl_re,
        fsub, dsub,
        copy_method, copy_overwrite, transplant_tree, collapse_tree,
        copy_dryrun, copy_silent, copy_debug,
        mkdir_upon_file_copy,
        allow_nonstd_shprogs,
        copy_shcmd_fmtstr,
        list_function,
        rematch_function,
        resub_function,
        rematch_partial
    )
    for x in walk_object.walk(srcdir, dstdir, copy_overwrite, transplant_tree, collapse_tree):
        yield x


FIND_RETURN_FILES = 1
FIND_RETURN_DIRS = 2
FIND_RETURN_MIX = 3
FIND_RETURN_ITEMS_DICT = {
    'files': FIND_RETURN_FILES,
    'dirs' : FIND_RETURN_DIRS,
    'mix'  : FIND_RETURN_MIX
}

def find(srcdir, dstdir=None,
        vreturn=None, vyield=None, debug=False,
        mindepth=0, maxdepth=float('inf'), dmatch_maxdepth=None,
        fmatch=None, fmatch_re=None, fexcl=None, fexcl_re=None,
        dmatch=None, dmatch_re=None, dexcl=None, dexcl_re=None,
        fsub=None, dsub=None,
        copy_method=None, copy_overwrite=False, transplant_tree=False, collapse_tree=False,
        copy_dryrun=False, copy_silent=False, copy_debug=False,
        mkdir_upon_file_copy=False,
        allow_nonstd_shprogs=False,
        copy_shcmd_fmtstr=None,
        list_function=None,
        rematch_function=None,
        resub_function=None,
        rematch_partial=False):
    if vreturn is None and vyield is None:
        ffilter = ([arg is not None for arg in [fmatch, fmatch_re, fexcl, fexcl_re, fsub]].count(True) > 0)
        dfilter = ([arg is not None for arg in [dmatch, dmatch_re, dexcl, dexcl_re, dsub]].count(True) > 0)
        if ffilter and dfilter:
            vreturn = FIND_RETURN_MIX
        elif ffilter:
            vreturn = FIND_RETURN_FILES
        elif dfilter:
            vreturn = FIND_RETURN_DIRS
        else:
            vreturn = FIND_RETURN_MIX

    return_items = [item_list for item_list in [vreturn, vyield] if item_list is not None]
    if len(return_items) != 1:
        raise InvalidArgumentError("One and only one of (`vreturn`, `vyield`) arguments must be provided")
    if type(return_items[0]) in (tuple, list):
        return_items = list(return_items[0])
    for i, item in enumerate(return_items):
        if type(item) is str:
            if item in FIND_RETURN_ITEMS_DICT:
                item = FIND_RETURN_ITEMS_DICT[item]
            else:
                raise InvalidArgumentError("`vreturn`/`vyield` string arguments must be one of {}, "
                                           "but was {}".format(list(FIND_RETURN_ITEMS_DICT.keys()), item))
        if type(item) is int and item not in list(FIND_RETURN_ITEMS_DICT.values()):
            raise InvalidArgumentError("`vreturn`/`vyield` int arguments must be one of {}, "
                                       "but was {}".format(list(FIND_RETURN_ITEMS_DICT.values()), item))
        return_items[i] = item
    if 1 <= len(set(return_items)) <= 2:
        pass
    else:
        raise InvalidArgumentError("`vreturn`/`vyield` argument contains duplicate items")

    return_mix = (FIND_RETURN_MIX in return_items)
    return_mix_only = (return_items == [FIND_RETURN_MIX])

    dirs_all = []
    files_all = []
    mix_all = []
    def _find_iter():
        for rootdir, dnames, fnames in walk(
            srcdir, dstdir,
            mindepth, maxdepth, dmatch_maxdepth,
            fmatch, fmatch_re, fexcl, fexcl_re,
            dmatch, dmatch_re, dexcl, dexcl_re,
            fsub, dsub,
            copy_method, copy_overwrite, transplant_tree, collapse_tree,
            copy_dryrun, copy_silent, copy_debug,
            mkdir_upon_file_copy,
            allow_nonstd_shprogs,
            copy_shcmd_fmtstr,
            list_function,
            rematch_function,
            resub_function,
            rematch_partial
        ):
            dirs = [os.path.join(rootdir, dn) for dn in dnames] if (FIND_RETURN_DIRS in return_items or return_mix) else None
            files = [os.path.join(rootdir, fn) for fn in fnames] if (FIND_RETURN_FILES in return_items or return_mix) else None
            if return_mix:
                mix = dirs if return_mix_only else list(dirs)
                mix.extend(files)
                if return_mix_only:
                    dirs, files = None, None
            else:
                mix = None

            if debug:
                if mix:
                    for p in mix:
                        sys.stdout.write(p+'\n')
                else:
                    if dirs:
                        for d in dirs:
                            sys.stdout.write(d+'\n')
                    if files:
                        for f in files:
                            sys.stdout.write(f+'\n')

            if vreturn:
                if dirs:
                    dirs_all.extend(dirs)
                if files:
                    files_all.extend(files)
                if mix:
                    mix_all.extend(mix)

            if vyield:
                if len(return_items) == 1:
                    item = return_items[0]
                    yield_results = files if item == FIND_RETURN_FILES else (dirs if item == FIND_RETURN_FILES else mix)
                    for p in yield_results:
                        yield p
                else:
                    yield_results = []
                    for item in return_items:
                        yield_results.append(files if item == FIND_RETURN_FILES else (dirs if item == FIND_RETURN_FILES else mix))
                    yield yield_results

    if vyield:
        return _find_iter()

    if vreturn:
        collections.deque(_find_iter(), maxlen=0)
        if len(return_items) == 1:
            item = return_items[0]
            return_results = files_all if item == FIND_RETURN_FILES else (dirs_all if item == FIND_RETURN_FILES else mix_all)
        else:
            return_results = []
            for item in return_items:
                return_results.append(files_all if item == FIND_RETURN_FILES else (dirs_all if item == FIND_RETURN_FILES else mix_all))
        return return_results


def get_script_arg_values(argstr, nvals=1, dtype=str, list_single_value=False):
    values = []
    for i, arg in sys.argv:
        if arg == argstr:
            argval_i_start = i + 1
            argval_i_end = argval_i_start + nvals
            if argval_i_end <= len(sys.argv):
                values.extend([dtype(val) for val in sys.argv[argval_i_start:argval_i_end]])
    if len(values) == 0:
        values = None
    elif len(values) == 1 and not list_single_value:
        values = values[0]
    return values


def access(path, mode, check_parent_if_dne=False):
    if not os.path.exists(path) and check_parent_if_dne:
        path_check_prev = ''
        path_check = path
        while not os.path.isdir(path_check) and path_check != path_check_prev:
            path_check_prev = path_check
            path_check = os.path.dirname(path_check_prev)
    else:
        path_check = path
    return os.access(path_check, mode)


class RawTextArgumentDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter): pass

def argtype_bool_plus(value, parse_fn=None):
    if parse_fn is not None:
        return parse_fn(value)
    else:
        return value
ARGTYPE_BOOL_PLUS = functools.partial(functools.partial, argtype_bool_plus)

def argtype_path_handler(path, argstr,
                         abspath_fn=os.path.realpath,
                         existcheck_fn=None, existcheck_reqval=None,
                         accesscheck_reqtrue=None, accesscheck_reqfalse=None,
                         accesscheck_parent_if_dne=False):
    path = os.path.expanduser(path)
    existcheck_fn_desc_dict = {
        os.path.isfile: 'file',
        os.path.isdir: 'directory',
        os.path.exists: 'file/directory'
    }
    accesscheck_mode_desc_list = [
        [os.F_OK, 'existing'],
        [os.R_OK, 'readable'],
        [os.W_OK, 'writeable'],
        [os.X_OK, 'executable']
    ]
    pathtype_desc = existcheck_fn_desc_dict[existcheck_fn] if existcheck_fn else 'path'
    if accesscheck_reqtrue is None:
        accesscheck_reqtrue = []
    if accesscheck_reqfalse is None:
        accesscheck_reqfalse = []
    if type(accesscheck_reqtrue) not in (set, tuple, list):
        accesscheck_reqtrue = [accesscheck_reqtrue]
    if type(accesscheck_reqfalse) not in (set, tuple, list):
        accesscheck_reqfalse = [accesscheck_reqfalse]
    accesscheck_reqtrue = set(accesscheck_reqtrue)
    accesscheck_reqfalse = set(accesscheck_reqfalse)
    modes_overlap = set(accesscheck_reqtrue).intersection(accesscheck_reqfalse)
    if len(modes_overlap) > 0:
        raise DeveloperError("The following permission settings (`os.access` modes)"
                             " appear in both required True and False lists: {}".format(modes_overlap))
    if existcheck_fn is not None and existcheck_fn(path) != existcheck_reqval:
        existresult_desc = 'does not exist' if existcheck_reqval is True else 'already exists'
        raise ScriptArgumentError("argument {}: {} {}".format(argstr, pathtype_desc, existresult_desc))
    access_desc_reqtrue_list = [mode_descr for mode, mode_descr in accesscheck_mode_desc_list if mode in accesscheck_reqtrue]
    access_desc_reqfalse_list = [mode_descr for mode, mode_descr in accesscheck_mode_desc_list if mode in accesscheck_reqfalse]
    access_desc_reqtrue_err_list = [mode_descr for mode, mode_descr in accesscheck_mode_desc_list if mode in accesscheck_reqtrue and access(path, mode, accesscheck_parent_if_dne) is not True]
    access_desc_reqfalse_err_list = [mode_descr for mode, mode_descr in accesscheck_mode_desc_list if mode in accesscheck_reqfalse and access(path, mode, accesscheck_parent_if_dne) is not False]
    if len(access_desc_reqtrue_err_list) > 0 or len(access_desc_reqfalse_err_list) > 0:
        errmsg = ' '.join([
            "{} must".format(pathtype_desc),
            (len(access_desc_reqtrue_list) > 0)*"be ({})".format(' & '.join(access_desc_reqtrue_list)),
            "and" if (len(access_desc_reqtrue_list) > 0 and len(access_desc_reqfalse_list) > 0) else '',
            (len(access_desc_reqfalse_list) > 0)*"not be ({})".format(', '.join(access_desc_reqfalse_list)),
            ", but it",
            (len(access_desc_reqtrue_err_list) > 0)*"is not ({})".format(', '.join(access_desc_reqtrue_err_list)),
            "and" if (len(access_desc_reqtrue_err_list) > 0 and len(access_desc_reqfalse_err_list) > 0) else '',
            (len(access_desc_reqfalse_err_list) > 0)*"is ({})".format(', '.join(access_desc_reqfalse_err_list)),
        ])
        errmsg = ' '.join(errmsg.split())
        errmsg = errmsg.replace(' ,', ',')
        raise ScriptArgumentError("argument {}: {}".format(argstr, errmsg))
    return abspath_fn(path) if abspath_fn is not None else path
ARGTYPE_PATH = functools.partial(functools.partial, argtype_path_handler)

def argtype_num_encode(num):
    num_str = str(num)
    if num_str.startswith('-') or num_str.startswith('+'):
        num_str = "'({})' ".format(num_str)
    return num_str
def argtype_num_decode(num_str):
    num_str = ''.join(num_str.split())
    return num_str.strip("'").strip('"').lstrip('(').rstrip(')')
def argtype_num_handler(num_str, argstr,
                        numeric_type=float,
                        allow_pos=True, allow_neg=True, allow_zero=True,
                        allow_inf=False, allow_nan=False,
                        allowed_min=None, allowed_max=None,
                        allowed_min_incl=True, allowed_max_incl=True):
    num_str = argtype_num_decode(num_str)
    if (   (allowed_min is not None and ((allowed_min < 0 and not allow_neg) or (allowed_min == 0 and not allow_zero) or (allowed_min > 0 and not allow_pos)))
        or (allowed_max is not None and ((allowed_max < 0 and not allow_neg) or (allowed_max == 0 and not allow_zero) or (allowed_max > 0 and not allow_pos)))):
        raise DeveloperError("Allowed min/max value does not align with allowed pos/neg/zero settings")
    dtype_name_dict = {
        int: 'integer',
        float: 'decimal'
    }
    lt_min_op = operator.lt if allowed_min_incl else operator.le
    gt_max_op = operator.gt if allowed_max_incl else operator.ge
    errmsg = None
    try:
        number_float = float(num_str)
    except ValueError:
        errmsg = "input could not be parsed as a valid (floating point) number"
    if errmsg is None:
        if number_float != number_float:  # assume number is NaN
            number_true = number_float
            if not allow_nan:
                errmsg = "NaN is not allowed"
        else:
            if number_float in (float('inf'), float('-inf')):
                number_true = number_float
                if not allow_inf:
                    errmsg = "+/-infinity is not allowed"
            else:
                try:
                    number_true = numeric_type(number_float)
                    if number_true != number_float:
                        errmsg = "number must be of {}type {}".format(
                            dtype_name_dict[numeric_type]+' ' if numeric_type in dtype_name_dict else '', numeric_type)
                except ValueError:
                    errmsg = "input could not be parsed as a designated {} number".format(numeric_type)
            if errmsg is None:
                if (   (not allow_pos and number_true > 0) or (not allow_neg and number_true < 0) or (not allow_zero and number_true == 0)
                    or (allowed_min is not None and lt_min_op(number_true, allowed_min)) or (allowed_max is not None and gt_max_op(number_true, allowed_max))):
                    input_cond = ' '.join([
                        "input must be a",
                        'positive'*allow_pos, 'or'*(allow_pos&allow_neg), 'negative'*allow_neg, 'non-zero'*(not allow_zero),
                        '{} number'.format(dtype_name_dict[numeric_type]) if numeric_type in dtype_name_dict else 'number of type {}'.format(numeric_type),
                        (allow_inf|allow_nan)*' ({} allowed)'.format(' '.join([
                            '{}infinity'.format('/'.join(['+'*allow_pos, '-'*allow_neg]).strip('/'))*allow_inf, 'and'*(allow_inf&allow_nan), 'NaN'*allow_nan]))
                    ])
                    if allowed_min is not None or allowed_max is not None:
                        if allowed_min is not None and allowed_max is not None:
                            input_cond_range = "in the range {}{}, {}{}".format(
                                '[' if allowed_min_incl else '(', allowed_min, allowed_max, ']' if allowed_max_incl else ']')
                        else:
                            if allowed_min is not None:
                                cond_comp = 'greater'
                                cond_value = allowed_min
                                cond_bound_incl = allowed_min_incl
                            elif allowed_max is not None:
                                cond_comp = 'less'
                                cond_value = allowed_max
                                cond_bound_incl = allowed_max_incl
                            input_cond_range = '{} than {} ({})'.format(
                                cond_comp, cond_value, 'inclusive' if cond_bound_incl else 'exclusive')
                        input_cond = ' '.join([input_cond, input_cond_range])
                    input_cond = ' '.join(input_cond.split())
                    input_cond = input_cond.replace(' ,', ',')
                    errmsg = input_cond
    if errmsg is not None:
        raise ScriptArgumentError("argument {}: {}".format(argstr, errmsg))
    else:
        return number_true
ARGTYPE_NUM = functools.partial(functools.partial, argtype_num_handler)
ARGNUM_POS_INF = argtype_num_encode(float('inf'))
ARGNUM_NEG_INF = argtype_num_encode(float('-inf'))
ARGNUM_NAN = argtype_num_encode(float('nan'))

def argtype_duration_handler(dur_str, argstr):
    pass
ARGTYPE_DURATION = functools.partial(functools.partial, argtype_duration_handler)


class ArgumentPasser:

    def __init__(self, executable_path, script_file, parser, sys_argv=[''], parse=True):
        self.exe = executable_path
        self.script_file = script_file
        self.script_fname = os.path.basename(script_file)
        self.parser = parser
        self.sys_argv = list(sys_argv)
        self.script_run_cmd = ' '.join(self.sys_argv)
        self.parsed = parse

        self.argstr2varstr = self._make_argstr2varstr_dict()
        self.varstr2argstr = self._make_varstr2argstr_dict()
        self.varstr2action = self._make_varstr2action_dict()
        self.argstr_pos = self._find_pos_args()
        self.provided_opt_args = self._find_provided_opt_args()

        if parse:
            self.vars = self.parser.parse_args()
            self.vars_dict = vars(self.vars)
        else:
            self.vars = None
            self.vars_dict = {varstr: None for varstr in self.varstr2argstr}

        self._fix_bool_plus_args()

        self.cmd_optarg_base = None
        self.cmd = None
        self._update_cmd_base()

    def __deepcopy__(self, memodict):
        args = ArgumentPasser(self.exe, self.script_file, self.parser, self.sys_argv, self.parsed)
        args.vars_dict = copy.deepcopy(self.vars_dict)
        return args

    def get_as_list(self, *argstrs):
        if len(argstrs) < 1:
            raise InvalidArgumentError("One or more argument strings must be provided")
        elif len(argstrs) == 1 and type(argstrs[0]) in (tuple, list):
            argstrs = argstrs[0]
        argstrs_invalid = set(argstrs).difference(set(self.argstr2varstr))
        if argstrs_invalid:
            raise InvalidArgumentError("This {} object does not have the following "
                                       "argument strings: {}".format(type(self).__name__, list(argstrs_invalid)))
        values = [self.vars_dict[self.argstr2varstr[argstr]] for argstr in argstrs]
        return values

    def get(self, *argstrs):
        values = self.get_as_list(*argstrs)
        if len(values) == 1:
            values = values[0]
        return values

    def set(self, argstrs, newval=None):
        if type(argstrs) in (tuple, list) and type(newval) in (tuple, list) and len(argstrs) == len(newval):
            argstr_list = argstrs
            for argstr_i, newval_i in list(zip(argstrs, newval)):
                if argstr_i not in self.argstr2varstr:
                    raise InvalidArgumentError("This {} object has no '{}' argument string".format(type(self).__name__, argstr_i))
                self.vars_dict[self.argstr2varstr[argstr_i]] = newval_i
        else:
            argstr_list = argstrs if type(argstrs) in (tuple, list) else [argstrs]
            for argstr in argstr_list:
                if argstr not in self.argstr2varstr:
                    raise InvalidArgumentError("This {} object has no '{}' argument string".format(type(self).__name__, argstr))
                if newval is None:
                    action = self.varstr2action[self.argstr2varstr[argstr]]
                    acttype = type(action)
                    if acttype is argparse._StoreAction and 'function argtype_bool_plus' in str(action.type):
                        newval = True
                    elif acttype in (argparse._StoreTrueAction, argparse._StoreFalseAction):
                        newval = (acttype is argparse._StoreTrueAction)
                    else:
                        raise InvalidArgumentError("Setting non-boolean argument string '{}' requires "
                                                   "a non-None `newval` value".format(argstr))
                self.vars_dict[self.argstr2varstr[argstr]] = newval
        if set(argstr_list).issubset(set(self.argstr_pos)):
            self._update_cmd()
        else:
            self._update_cmd_base()

    def unset(self, *argstrs):
        if len(argstrs) < 1:
            raise InvalidArgumentError("One or more argument strings must be provided")
        elif len(argstrs) == 1 and type(argstrs[0]) in (tuple, list):
            argstrs = argstrs[0]
        for argstr in argstrs:
            action = self.varstr2action[self.argstr2varstr[argstr]]
            acttype = type(action)
            if acttype is argparse._StoreAction and 'function argtype_bool_plus' in str(action.type):
                newval = False
            elif acttype in (argparse._StoreTrueAction, argparse._StoreFalseAction):
                newval = (acttype is argparse._StoreFalseAction)
            else:
                newval = None
            self.vars_dict[self.argstr2varstr[argstr]] = newval
        if set(argstrs).issubset(set(self.argstr_pos)):
            self._update_cmd()
        else:
            self._update_cmd_base()

    def provided(self, argstr):
        return argstr in self.provided_opt_args

    def _make_argstr2varstr_dict(self):
        argstr2varstr = {}
        for act in self.parser._actions:
            if len(act.option_strings) == 0:
                argstr2varstr[act.dest.replace('_', '-')] = act.dest
            else:
                for os in act.option_strings:
                    argstr2varstr[os] = act.dest
        return argstr2varstr

    def _make_varstr2argstr_dict(self):
        varstr2argstr = {}
        for act in self.parser._actions:
            if len(act.option_strings) == 0:
                varstr2argstr[act.dest] = act.dest.replace('_', '-')
            else:
                varstr2argstr[act.dest] = sorted(act.option_strings)[0]
        return varstr2argstr

    def _make_varstr2action_dict(self):
        return {act.dest: act for act in self.parser._actions}

    def _find_pos_args(self):
        return [act.dest.replace('_', '-') for act in self.parser._actions if len(act.option_strings) == 0]

    def _find_provided_opt_args(self):
        provided_opt_args = []
        for token in self.sys_argv:
            potential_argstr = token.split('=')[0]
            if potential_argstr in self.argstr2varstr:
                provided_opt_args.append(self.varstr2argstr[self.argstr2varstr[potential_argstr]])
        return provided_opt_args

    def _fix_bool_plus_args(self):
        for varstr in self.vars_dict:
            argstr = self.varstr2argstr[varstr]
            action = self.varstr2action[varstr]
            if 'function argtype_bool_plus' in str(action.type) and self.get(argstr) is None:
                self.set(argstr, (argstr in self.provided_opt_args))

    def _argval2str(self, item):
        if type(item) is str:
            if item.startswith('"') and item.endswith('"'):
                item_str = item
            elif item.startswith("'") and item.endswith("'"):
                item_str = item
            else:
                item_str = '"{}"'.format(item)
        else:
            item_str = '{}'.format(item)
        return item_str

    def _update_cmd_base(self):
        arg_list = []
        for varstr, val in self.vars_dict.items():
            argstr = self.varstr2argstr[varstr]
            if argstr not in self.argstr_pos and val is not None:
                if isinstance(val, bool):
                    action = self.varstr2action[varstr]
                    acttype = type(action)
                    if acttype is argparse._StoreAction:
                        if 'function argtype_bool_plus' in str(action.type) and val is True:
                            arg_list.append(argstr)
                    elif (   (acttype is argparse._StoreTrueAction and val is True)
                          or (acttype is argparse._StoreFalseAction and val is False)):
                        arg_list.append(argstr)
                elif isinstance(val, list) or isinstance(val, tuple):
                    arg_list.append('{} {}'.format(argstr, ' '.join([self._argval2str(item) for item in val])))
                else:
                    arg_list.append('{} {}'.format(argstr, self._argval2str(val)))
        self.cmd_optarg_base = ' '.join(arg_list)
        self._update_cmd()

    def _update_cmd(self):
        posarg_list = []
        for argstr in self.argstr_pos:
            varstr = self.argstr2varstr[argstr]
            val = self.vars_dict[varstr]
            if val is not None:
                if isinstance(val, list) or isinstance(val, tuple):
                    posarg_list.append(' '.join([self._argval2str(item) for item in val]))
                else:
                    posarg_list.append(self._argval2str(val))
        self.cmd = '{} {} {} {}'.format(self.exe, self.script_file, " ".join(posarg_list), self.cmd_optarg_base)

    def get_cmd(self):
        return self.cmd

    def get_jobsubmit_cmd(self, scheduler,
                          jobscript=None, jobname=None,
                          time_hr=None, time_min=None, time_sec=None,
                          memory_gb=None, node=None, email=None,
                          envvars=None):
        cmd = None
        cmd_envvars = None
        jobscript_optkey = None

        total_sec = 0
        if time_hr is not None:
            total_sec += time_hr*3600
        if time_min is not None:
            total_sec += time_min*60
        if time_sec is not None:
            total_sec += time_sec

        if total_sec == 0:
            time_hms = None
        else:
            m, s = divmod(total_sec, 60)
            h, m = divmod(m, 60)
            time_hms = '{:d}:{:02d}:{:02d}'.format(h, m, s)

        if envvars is not None:
            if type(envvars) in (tuple, list):
                cmd_envvars = ','.join(['p{}="{}"'.format(i, a) for i, a in enumerate(envvars)])
            elif type(envvars) == dict:
                cmd_envvars = ','.join(['{}="{}"'.format(var_name, var_val) for var_name, var_val in envvars.items()])

        if scheduler == SCHED_PBS:
            cmd = ' '.join([
                'qsub',
                "-N {}".format(jobname) * (jobname is not None),
                "-l {}".format(
                    ','.join([
                        "nodes={}".format(node) if node is not None else '',
                        "walltime={}".format(time_hms) if time_hms is not None else '',
                        "mem={}gb".format(memory_gb) if memory_gb is not None else ''
                    ]).strip(',')
                ) if (time_hms is not None and memory_gb is not None) else '',
                "-v {}".format(cmd_envvars) if cmd_envvars is not None else ''
                "-m ae" if email else ''
            ])
            jobscript_optkey = '#PBS'

        elif scheduler == SCHED_SLURM:
            cmd = ' '.join([
                'sbatch',
                "--job-name {}".format(jobname) if jobname is not None else '',
                "--time {}".format(time_hms) if time_hms is not None else '',
                "--mem {}G".format(memory_gb) if memory_gb is not None else '',
                "-v {}".format(cmd_envvars) if cmd_envvars is not None else ''
                "--mail-type FAIL,END" if email else '',
                "--mail-user {}".format(email) if type(email) is str else None
            ])
            jobscript_optkey = '#SBATCH'

        if jobscript_optkey is not None:
            jobscript_condoptkey = jobscript_optkey.replace('#', '#CONDOPT_')

            jobscript_condopts = []
            with open(jobscript) as job_script_fp:
                for line_num, line in enumerate(job_script_fp.readlines(), 1):
                    if line.lstrip().startswith(jobscript_condoptkey):

                        cond_ifval = None
                        cond_cond = None
                        cond_elseval = None

                        cond_remain = line.replace(jobscript_condoptkey, '').strip()
                        cond_parts = [s.strip() for s in cond_remain.split(' ELSE ')]
                        if len(cond_parts) == 2:
                            cond_remain, cond_elseval = cond_parts
                        cond_parts = [s.strip() for s in cond_remain.split(' IF ')]
                        if len(cond_parts) == 2:
                            cond_ifval, cond_cond = cond_parts

                        try:
                            condopt_add = None

                            if cond_ifval is not None and cond_cond is not None:
                                if self._jobscript_condopt_eval(cond_cond, eval):
                                    condopt_add = self._jobscript_condopt_eval(cond_ifval, str)
                                elif cond_elseval is not None:
                                    condopt_add = self._jobscript_condopt_eval(cond_elseval, str)
                            elif cond_elseval is not None:
                                raise SyntaxError
                            elif cond_remain.startswith('import') or cond_remain.startswith('from'):
                                exec(cond_remain)
                            else:
                                condopt_add = self._jobscript_condopt_eval(cond_remain, str)

                            if condopt_add is not None:
                                jobscript_condopts.append(condopt_add)

                        except SyntaxError:
                            raise InvalidArgumentError(' '.join([
                                "Invalid syntax in jobscript conditional option:",
                                "\n  File '{}', line {}: '{}'".format(jobscript, line_num, line.rstrip()),
                                "\nProper conditional option syntax is as follows:",
                                "'{} <options> [IF <conditional> [ELSE <options>]]'".format(jobscript_condoptkey)
                            ]))

            if jobscript_condopts:
                cmd = r'{} {}'.format(cmd, ' '.join(jobscript_condopts))

        cmd = r'{} "{}"'.format(cmd, jobscript)

        return cmd

    def _jobscript_condopt_eval(self, condopt_expr, out_type):
        if out_type not in (str, eval):
            raise InvalidArgumentError("`out_type` must be either str or eval")
        vars_dict = self.vars_dict
        for varstr in sorted(vars_dict.keys(), key=len, reverse=True):
            possible_substr = {'%'+s for s in [varstr, self.varstr2argstr[varstr], self.varstr2argstr[varstr].lstrip('-')]}
            possible_substr = possible_substr.union({s.lower() for s in possible_substr}, {s.upper() for s in possible_substr})
            for substr in possible_substr:
                if substr in condopt_expr:
                    replstr = str(vars_dict[varstr]) if out_type is str else "vars_dict['{}']".format(varstr)
                    condopt_expr = condopt_expr.replace(substr, replstr)
                    break
        return out_type(condopt_expr)


def get_index_fmtstr(num_items, min_digits=3):
    return '{:0>'+str(max(min_digits, len(str(num_items))))+'}'


def write_task_bundles(task_list, tasks_per_bundle,
                       dstdir, bundle_prefix,
                       task_delim=',', task_fmt='%s'):
    try:
        import numpy as np
        imported_numpy = True
    except ImportError:
        imported_numpy = False
        if task_fmt != '%s':
            raise
    bundle_prefix = os.path.join(dstdir, '{}_{}'.format(bundle_prefix, datetime.now().strftime("%Y%m%d%H%M%S")))
    jobnum_total = int(math.ceil(len(task_list) / float(tasks_per_bundle)))
    jobnum_fmt = get_index_fmtstr(jobnum_total)
    bundle_file_list = []
    print("Writing task bundle text files in directory: {}".format(dstdir))
    for jobnum, tasknum in enumerate(range(0, len(task_list), tasks_per_bundle), 1):
        bundle_file = '{}_{}.txt'.format(bundle_prefix, jobnum_fmt.format(jobnum))
        task_bundle = task_list[tasknum:tasknum+tasks_per_bundle]
        if len(task_bundle) == 0:
            with open(bundle_file, 'w'):
                pass
        elif imported_numpy:
            np.savetxt(bundle_file, task_bundle, fmt=task_fmt, delimiter=task_delim)
        else:
            join_task_items = type(task_bundle[0]) in (tuple, list)
            with open(bundle_file, 'w') as bundle_file_fp:
                for task in task_bundle:
                    task_line = str(task) if not join_task_items else task_delim.join([str(arg) for arg in task])
                    bundle_file_fp.write(task_line+'\n')
        bundle_file_list.append(bundle_file)
    return bundle_file_list


def read_task_bundle(bundle_file, args_dtype=str, args_delim=',', header_rows=0,
                     ncol_strict=True, ncol_min=None, ncol_max=None,
                     allow_1d_task_list=True, read_header=False):
    try:
        import numpy as np
        imported_numpy = True
    except ImportError:
        imported_numpy = False

    task_list_ncols_min = None
    task_list_ncols_max = None

    if ncol_strict and imported_numpy:
        loadtxt_dtype = np.dtype(str) if args_dtype is str else args_dtype
        if read_header:
            loadtxt_skiprows = 0
            loadtxt_maxrows = header_rows
        else:
            loadtxt_skiprows = header_rows
            loadtxt_maxrows = None
        try:
            task_list = np.loadtxt(bundle_file, dtype=loadtxt_dtype, delimiter=args_delim,
                                   skiprows=loadtxt_skiprows, max_rows=loadtxt_maxrows,
                                   ndmin=2)
        except ValueError as e:
            if str(e).startswith("Wrong number of columns"):
                traceback.print_exc()
                raise DimensionError("Inconsistent number of columns in `bundle_file`: {}".format(bundle_file))
            else:
                raise
        task_list_ncols = task_list.shape[1] if task_list.ndim == 2 else task_list.shape[0]
        task_list_ncols_min = task_list_ncols
        task_list_ncols_max = task_list_ncols
        if ncol_min is not None and task_list_ncols < ncol_min:
            raise DimensionError("`bundle_file` line has {} columns, less than required minimum ({}): {}".format(
                                 task_list_ncols, ncol_min, bundle_file))
        if ncol_max is not None and task_list_ncols > ncol_max:
            raise DimensionError("`bundle_file` line has {} columns, more than required maximum ({}): {}".format(
                                 task_list_ncols, ncol_max, bundle_file))
        if allow_1d_task_list and task_list_ncols == 1:
            task_list = task_list[:, 0]

        task_list = task_list.tolist()

    else:
        with open(bundle_file, 'r') as bundle_file_fp:
            if read_header:
                task_list = []
                for i in range(header_rows):
                    header_line = bundle_file_fp.readline().strip()
                    if header_line != '':
                        task_list.append(header_line)
            else:
                task_list = [line for line in bundle_file_fp.read().splitlines() if line.strip() != '']
                if header_rows > 0 and len(task_list) > 0:
                    task_list = task_list[header_rows:]
        if len(task_list) > 0:
            if args_delim is not None:
                if type(args_dtype) in (tuple, list):
                    task_list = [[args_dtype[col_num](arg.strip()) for col_num, arg in enumerate(task.split(args_delim))] for task in task_list]
                else:
                    task_list = [[args_dtype(arg.strip()) for arg in task.split(args_delim)] for task in task_list]

                task_list_ncols = None
                if ncol_min is not None or ncol_max is not None:
                    task_list_ncols = [len(task) for task in task_list]
                    task_list_ncols_min = min(task_list_ncols)
                    task_list_ncols_max = max(task_list_ncols)
                    if task_list_ncols_min == task_list_ncols_max:
                        task_list_ncols = task_list_ncols_min
                elif ncol_strict:
                    first_task_ncols = len(task_list[0])
                    if all(len(task) == first_task_ncols for task in task_list):
                        task_list_ncols = first_task_ncols
                if ncol_strict and task_list_ncols is None:
                    raise DimensionError("Inconsistent number of columns in `bundle_file`: {}".format(bundle_file))

                if allow_1d_task_list and not read_header:
                    task_list = [task[0] if len(task) == 1 else task for task in task_list]
            elif not allow_1d_task_list or read_header:
                task_list = [[args_dtype(arg.strip())] for arg in task_list]
            else:
                task_list = [args_dtype(arg.strip()) for arg in task_list]

    if ncol_min is not None and task_list_ncols_min < ncol_min:
        raise DimensionError("`bundle_file` line has {} columns, less than required minimum ({}): {}".format(
                             task_list_ncols_min, ncol_min, bundle_file))
    if ncol_max is not None and task_list_ncols_max > ncol_max:
        raise DimensionError("`bundle_file` line has {} columns, more than required maximum ({}): {}".format(
                             task_list_ncols_max, ncol_max, bundle_file))

    if read_header and len(task_list) == 1 and type(task_list[0]) is list:
        task_list = task_list[0]

    return task_list


class Tasklist:
    def __init__(self, tasklist_file, args_dtype=str, args_delim=',', header_rows=0,
                 ncol_strict=True, ncol_strict_header_separate=False, ncol_min=None, ncol_max=None,
                 allow_1d_task_list=True, header_dtype=str):
        self.tasklist_file = tasklist_file
        self.header = None
        if header_rows > 0:
            self.header = read_task_bundle(
                tasklist_file, args_dtype=header_dtype, args_delim=args_delim, header_rows=header_rows,
                ncol_strict=ncol_strict, ncol_min=ncol_min, ncol_max=ncol_max,
                allow_1d_task_list=allow_1d_task_list, read_header=True
            )
        self.tasks = read_task_bundle(
            tasklist_file, args_dtype=args_dtype, args_delim=args_delim, header_rows=header_rows,
            ncol_strict=ncol_strict, ncol_min=ncol_min, ncol_max=ncol_max,
            allow_1d_task_list=allow_1d_task_list, read_header=False
        )
        if (    self.header is not None and ncol_strict and not ncol_strict_header_separate
            and len(self.header) > 0 and len(self.tasks) > 0):
                header_ncols = len(self.header[0]) if type(self.header[0]) is list else len(self.header)
                tasks_ncols = len(self.tasks[0]) if type(self.tasks[0]) is list else 1
                if header_ncols != tasks_ncols:
                    raise DimensionError("Inconsistent number of columns in `tasklist_file`: {}".format(tasklist_file))


SCHED_SUPPORTED = []
SCHED_PBS = 'pbs'
SCHED_SLURM = 'slurm'
SCHED_NAME_TESTCMD_DICT = {
    SCHED_PBS: 'pbsnodes',
    SCHED_SLURM: 'sinfo'
}
# if SYSTYPE == SYSTYPE_LINUX:
#     for sched_name in sorted(SCHED_NAME_TESTCMD_DICT.keys()):
#         try:
#             proc = subprocess.Popen(SCHED_NAME_TESTCMD_DICT[sched_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#             if proc.wait() == 0:
#                 SCHED_SUPPORTED.append(sched_name)
#         except OSError:
#             pass
if len(SCHED_SUPPORTED) == 0:
    SCHED_SUPPORTED.append(None)

ARGSTR_SCHEDULER = '--scheduler'
ARGSTR_JOBSCRIPT = '--jobscript'
ARGSTR_JOB_ABBREV = '--job-abbrev'
ARGSTR_JOB_WALLTIME = '--job-walltime'
ARGSTR_JOB_MEMORY = '--job-memory'
ARGSTR_TASKS_PER_JOB = '--tasks-per-job'
ARGSTR_BUNDLEDIR = '--bundledir'
ARGSTR_LOGDIR = '--logdir'
ARGSTR_EMAIL = '--email'
ARGGRP_SCHEDULER = [
    ARGSTR_SCHEDULER,
    ARGSTR_JOBSCRIPT,
    ARGSTR_JOB_ABBREV,
    ARGSTR_JOB_WALLTIME,
    ARGSTR_JOB_MEMORY,
    ARGSTR_TASKS_PER_JOB,
    ARGSTR_BUNDLEDIR,
    ARGSTR_LOGDIR,
    ARGSTR_EMAIL,
]
ARGGRP_OUTDIR = [
    ARGSTR_BUNDLEDIR,
    ARGSTR_LOGDIR
]

def add_scheduler_arguments(parser,
                            job_abbrev,
                            job_walltime,
                            job_memory,
                            jobscript_dir,
                            bundledir):
    parser.add_argument(
        ARGSTR_SCHEDULER,
        type=str,
        choices=SCHED_SUPPORTED,
        default=None,
        help="Name of job scheduler to use for task submission."
    )
    parser.add_argument(
        ARGSTR_JOB_ABBREV,
        type=str,
        default=job_abbrev,
        help="Prefix for the jobnames of jobs submitted to scheduler."
    )
    parser.add_argument(
        ARGSTR_JOB_WALLTIME,
        type=ARGTYPE_NUM(argstr=ARGSTR_JOB_WALLTIME,
            numeric_type=int, allow_neg=False, allow_zero=False, allow_inf=False),
        default=job_walltime,
        help="Wallclock time alloted for each job submitted to scheduler."
    )
    parser.add_argument(
        ARGSTR_JOB_MEMORY,
        type=ARGTYPE_NUM(argstr=ARGSTR_JOB_MEMORY,
            numeric_type=int, allow_neg=False, allow_zero=False, allow_inf=False),
        default=job_memory,
        help="Memory alloted for each job submitted to scheduler."
    )
    parser.add_argument(
        ARGSTR_JOBSCRIPT,
        type=ARGTYPE_PATH(argstr=ARGSTR_JOBSCRIPT,
            existcheck_fn=os.path.isfile,
            existcheck_reqval=True,
            accesscheck_reqtrue=os.W_OK,
            accesscheck_parent_if_dne=True),
        default=None,
        help=' '.join([
            "Script to run in job submission to scheduler.",
            "(default scripts are found in {})".format(jobscript_dir),
        ])
    )
    parser.add_argument(
        ARGSTR_TASKS_PER_JOB,
        type=ARGTYPE_NUM(argstr=ARGSTR_JOB_MEMORY,
            numeric_type=int, allow_neg=False, allow_zero=False, allow_inf=False),
        default=None,
        help=' '.join([
            "Number of tasks to bundle into a single job.",
            "(requires {} option)".format(ARGSTR_SCHEDULER),
        ])
    )
    parser.add_argument(
        ARGSTR_BUNDLEDIR,
        type=ARGTYPE_PATH(argstr=ARGSTR_BUNDLEDIR,
            existcheck_fn=os.path.isfile,
            existcheck_reqval=False,
            accesscheck_reqtrue=os.W_OK,
            accesscheck_parent_if_dne=True),
        default=bundledir,
        help=' '.join([
            "Directory in which task bundle textfiles will be built if {} option is provided".format(ARGSTR_TASKS_PER_JOB),
            "for job submission to scheduler.",
        ])
    )
    parser.add_argument(
        ARGSTR_LOGDIR,
        type=ARGTYPE_PATH(argstr=ARGSTR_LOGDIR,
            existcheck_fn=os.path.isfile,
            existcheck_reqval=False,
            accesscheck_reqtrue=os.W_OK,
            accesscheck_parent_if_dne=True),
        default=None,
        help=' '.join([
            "Directory to which standard output/error log files will be written for batch job runs.",
            "\nIf not provided, default scheduler (or jobscript #CONDOPT_) options will be used.",
            "\n**Note:** Due to implementation difficulties, this directory will also become the",
            "working directory for job processes.",
        ])
    )
    parser.add_argument(
        ARGSTR_EMAIL,
        type=ARGTYPE_BOOL_PLUS(
            parse_fn=str),
        nargs='?',
        help="Send email to user upon end or abort of the LAST SUBMITTED task."
    )


def set_default_jobscript(args, jobscript_dir):
    if args.get(ARGSTR_SCHEDULER) is not None:
        if args.get(ARGSTR_JOBSCRIPT) is None:
            jobscript_default = os.path.join(jobscript_dir, 'head_{}.sh'.format(args.get(ARGSTR_SCHEDULER)))
            if not os.path.isfile(jobscript_default):
                args.parser.error(
                    "Default jobscript ({}) does not exist, ".format(jobscript_default)
                    + "please specify one with {} argument".format(ARGSTR_JOBSCRIPT))
            else:
                args.set(ARGSTR_JOBSCRIPT, jobscript_default)
                print("argument {} set automatically to: {}".format(ARGSTR_JOBSCRIPT, args.get(ARGSTR_JOBSCRIPT)))


def check_mut_excl_arggrp(args, argcol_mut_excl):
    for arggrp in argcol_mut_excl:
        if [args.get(argstr) is True if type(args.get(argstr)) is bool else
            args.get(argstr) is not None for argstr in arggrp].count(True) > 1:
            args.parser.error("{} arguments are mutually exclusive{}".format(
                "{} and {}".format(*arggrp) if len(arggrp) == 2 else "The following",
                '' if len(arggrp) == 2 else ": {}".format(arggrp)
            ))


def create_argument_directories(args, *dir_argstrs):
    dir_argstrs_unpacked = []
    for argstr in dir_argstrs:
        if type(argstr) in (tuple, list):
            dir_argstrs_unpacked.extend(list(argstr))
    for dir_argstr, dir_path in list(zip(dir_argstrs_unpacked, args.get_as_list(dir_argstrs_unpacked))):
        if dir_path is not None and not os.path.isdir(dir_path):
            print("Creating argument {} directory: {}".format(dir_argstr, dir_path))
            os.makedirs(dir_path)


def submit_tasks_to_scheduler(parent_args, parent_tasks,
                              parent_task_argstrs, child_bundle_argstr,
                              child_args=None,
                              task_items_descr=None, task_delim=',',
                              python_version_accepted_min=PYTHON_VERSION_REQUIRED_MIN,
                              dryrun=False):
    if child_args is None:
        child_args = copy.deepcopy(parent_args)
    child_args.unset(ARGGRP_SCHEDULER)

    child_tasks = (parent_tasks if parent_args.get(ARGSTR_TASKS_PER_JOB) is None else
        write_task_bundles(parent_tasks, parent_args.get(ARGSTR_TASKS_PER_JOB), parent_args.get(ARGSTR_BUNDLEDIR),
            '{}_{}'.format(parent_args.get(ARGSTR_JOB_ABBREV),
                           task_items_descr if task_items_descr is not None else child_bundle_argstr.lstrip('-')),
            task_delim=task_delim)
    )
    child_task_argstrs = parent_task_argstrs if child_tasks is parent_tasks else child_bundle_argstr

    job_abbrev, job_walltime_hr, job_memory_gb = parent_args.get(ARGSTR_JOB_ABBREV, ARGSTR_JOB_WALLTIME, ARGSTR_JOB_MEMORY)
    jobname_fmt = job_abbrev+get_index_fmtstr(len(child_tasks))

    last_job_email = parent_args.get(ARGSTR_EMAIL)

    job_num = 0
    num_jobs = len(child_tasks)
    for task_items in child_tasks:
        job_num += 1

        child_args.set(child_task_argstrs, task_items)
        if job_num == num_jobs and last_job_email:
            child_args.set(ARGSTR_EMAIL, last_job_email)
        cmd_single = child_args.get_cmd()

        job_name = jobname_fmt.format(job_num)
        cmd = child_args.get_jobsubmit_cmd(
            parent_args.get(ARGSTR_SCHEDULER),
            jobscript=parent_args.get(ARGSTR_JOBSCRIPT),
            jobname=job_name, time_hr=job_walltime_hr, memory_gb=job_memory_gb, email=parent_args.get(ARGSTR_EMAIL),
            envvars=[parent_args.get(ARGSTR_JOBSCRIPT), job_abbrev, cmd_single, python_version_accepted_min]
        )

        print(cmd)
        if not dryrun:
            subprocess.call(cmd, shell=True, cwd=parent_args.get(ARGSTR_LOGDIR))


def handle_task_exception(e, args, jobscript_init):
    with capture_stdout_stderr() as out:
        traceback.print_exc()
    caught_out, caught_err = out
    error_trace = caught_err
    eprint(error_trace)
    if e.__class__ is ImportError:
        print(' '.join([
            "\nFailed to import necessary module(s)\n"
            "If running on a Linux system where the jobscripts/init.sh file has been properly",
            "set up, try running the following command to activate a working environment",
            "in your current shell session:\n{}\n".format("source {} {}".format(jobscript_init, args.get(ARGSTR_JOB_ABBREV))),
        ]))


def send_script_completion_email(args, error_trace):
    email_body = args.script_run_cmd+'\n'
    if error_trace is not None:
        email_status = "ERROR"
        email_body += "\n{}\n".format(error_trace)
    else:
        email_status = "COMPLETE"
    email_subj = "{} - {}".format(email_status, args.script_fname)
    send_email(args.get(ARGSTR_EMAIL), email_subj, email_body)
