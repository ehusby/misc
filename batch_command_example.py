#!/usr/bin/env python3

# Erik Husby, 2018

# The first line of this file is called a "shebang".
# In a Unix environment, this line tells the shell that
# this script should be run with Python (version 3) if
# the user attempts to execute it directly, for example
# with the command `./batch_command.py`.
# This script may also be run with the command
# `python3 batch_command.py` (or `python batch_command.py`,
# depending on the Python installation and/or contents of
# the shell's PATH environment variable -- Google it).


# Put import statements at the top of the file
# so that their contents will be available when
# referenced at any point in the script.
import argparse
import glob
import os
import subprocess


def main():
    # Add any arguments you need to the bottom of this
    # code block, making sure to parse and validate
    # each argument as needed.
    parser = argparse.ArgumentParser(description=(
        "Perform a command on (a subset of) files in a directory in batch."))
    parser.add_argument('src',
        help=("Path to source directory."))
    parser.add_argument('dst',
        help="Path to destination directory.")
    parser.add_argument('--dryrun', action='store_true', default=False,
        help="Print actions without executing.")

    # Parse arguments.
    args = parser.parse_args()
    srcdir = os.path.abspath(args.src)
    dstdir = os.path.abspath(args.dst)

    # Validate arguments.
    if not os.path.isdir(srcdir):
        parser.error("src must be a directory")
    if not os.path.isdir(dstdir):
        print "Creating destination directory: {}".format(dstdir)
        os.makedirs(dstdir)

    # Filter source files, usually by making use of the wildcard
    # character '*' to make a searchable filename pattern.
    filename_pattern = '*.jpg'

    srcFiles = glob.glob(os.path.join(srcdir, filename_pattern))
    num_jobs = len(srcFiles)
    print("Found {} files to work on".format(num_jobs))

    i = 1
    for srcFile in srcFiles:

        # Set the path of the destination file.
        dstFile = os.path.join(dstdir, os.path.basename(srcFile))
        # A path's "basename" is the filename of the file that
        # you usually see when you browse through files on your PC.
        
        # Build the command to be run.
        cmd = """mklink /h "{}" "{}" """.format(dstFile, srcFile)
        # The command I put here as an example is a Windows
        # command that creates a "hard link" of the source file.
        # Although the hard link appears to be a simple copy of
        # the source file, under the hood the hard link and the
        # source file "point to" the exact same location where
        # the file data is stored on disk. After creating the
        # hard link, that data will not be deleted until both
        # the source file and the hard link file are deleted.
        
        # If this script takes a while to run, it's much better
        # to be able to see what's going on than to be in the dark.
        # Make good use of print statements like these for logging.
        print("({}/{}) {}".format(i, num_jobs, cmd))

        if not args.dryrun:
            # Execute the command that has been built as if it
            # were typed in and run through the command prompt.
            subprocess.call(cmd, shell=True)

        i += 1

    print("Done!")



# The following code block tells Python that when this
# script is executed, enter the function called `main`.
if __name__ == '__main__':
    main()
