#!/bin/sh
# bot wrapper
this_dir=$(realpath -- "$(dirname "$0")")
cd "$this_dir" || exit 2
this_dirname=$(basename "$this_dir")

if [ -z "$1" ]; then
    if [ -f "${this_dirname}.py" ]; then
        main_script=${this_dirname}.py
    else
        main_script=main.py
    fi
else
    main_script=${1}.py
fi

if ! [ -f "$main_script" ] ; then
	printf >&2 '%s: error: Could not find %s\n' "${0##*/}" "$main_script"
	exit 1
fi

relaunch_counter=0
while [ "$relaunch_counter" -lt 10 ] ; do
    relaunch_counter=$((1 + relaunch_counter))
    /usr/bin/env python3 "$main_script"
    sleep 5
done
exit 2
