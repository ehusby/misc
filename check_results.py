#!/usr/bin/env python2

# Erik Husby, 2017


import argparse
import os


PREFIX_END_INDEX = 92
EXPECTED_NUM_FILES_WITH_PREFIX = 4


def main():
    parser = argparse.ArgumentParser(description=(
        "Scan contents of a text file containing an alphabetized list of filenames"
        " (e.g. flistFile from `ls > [flistFile]`)"
        " to find breaks in filename patterns."))
    parser.add_argument('flistFile',
        help="Path to text file containing alphabetized list of file names, one per line, left-aligned.")
    args = parser.parse_args()
    flistFile = args.flistFile

    if not os.path.isfile(flistFile):
        parser.error("flistFile path is not valid".format(flistFile))

    flistFile_fp = open(flistFile, 'r')
    prefix_num = EXPECTED_NUM_FILES_WITH_PREFIX
    prefix_prev = ''
    prefix = flistFile_fp.readline()[:PREFIX_END_INDEX]
    while prefix != '':
        if prefix == prefix_prev:
            prefix_num += 1
        else:
            if prefix_num != EXPECTED_NUM_FILES_WITH_PREFIX:
                # if prefix_num < EXPECTED_NUM_FILES_WITH_PREFIX:
                #     print "(under) "+prefix_prev
                # else:
                #     print "( over) "+prefix_prev
                print prefix_prev
            prefix_num = 1
        prefix_prev = prefix
        prefix = flistFile_fp.readline()[:PREFIX_END_INDEX]
    flistFile_fp.close()



if __name__ == '__main__':
    main()
