#!/bin/sh
# bot wrapper
this_dir="$(dirname "$0")"
cd "$this_dir" || exit 2

relaunch_counter=0
while [ "$relaunch_counter" -lt 10 ] ; do
    relaunch_counter=$((1 + relaunch_counter))
    /usr/bin/env python3 ./tiefblauer-himmel.py
    sleep 5
done
exit 1
