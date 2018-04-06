#!/bin/bash

## This script fixes the permissions of data that has been transferred through Globus
## such that all folders and files within the directories specified as arguments have
## 770 and 660 permission, respectively.

USAGE="Usage: $0 regiondir1 regiondir2 ... regiondirN"

if [ "$#" == "0" ]; then
    echo "$USAGE"
    exit 1
fi

while (( "$#" )); do

    echo "Fixing perms in $1"
    chmod -R u=rwX,g=rwX,o-rwx "$1"
    # find "$1" -type d -exec chmod 770 '{}' \;
    # find "$1" -type d -user husby -print0 | xargs -0 chmod 770

    shift
done

echo "Done!"
