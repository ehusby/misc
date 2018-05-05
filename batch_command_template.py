#!/usr/bin/env python3

import argparse
import glob
import os
import subprocess


def main():
    parser = argparse.ArgumentParser(description=(
        "Perform a command on (a subset of) files in a directory in batch."))
    parser.add_argument('src',
        help="Path to source directory.")
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
        print("Creating destination directory: {}".format(dstdir))
        os.makedirs(dstdir)

    # Filter source files.
    filename_pattern = '*.jpg'

    srcFiles = glob.glob(os.path.join(srcdir, filename_pattern))
    num_jobs = len(srcFiles)
    print("Found {} files to work on".format(num_jobs))

    i = 1
    for srcFile in srcFiles:

        # Set the path of the destination file.
        dstFile = os.path.join(dstdir, os.path.basename(srcFile))

        # Build the command to be run.
        cmd = """mklink /h "{}" "{}" """.format(dstFile, srcFile)

        print("({}/{}) {}".format(i, num_jobs, cmd))

        if not args.dryrun:
            subprocess.call(cmd, shell=True)

        i += 1

    print("Done!")



if __name__ == '__main__':
    main()
