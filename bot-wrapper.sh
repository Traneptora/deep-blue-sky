#!/bin/sh
# bot wrapper
this_dir="$(dirname "$0")"
cd "$this_dir" || exit 2
this_sh="$(basename "$0")"
bot_script="${1}.py"
if ! [ -f "$bot_script" ] ; then
	printf >&2 'Must provide valid bot name\nUsage: %s BOTNAME\n' "$this_sh"
	exit 1
fi

relaunch_counter=0
while [ "$relaunch_counter" -lt 10 ] ; do
    relaunch_counter=$((1 + relaunch_counter))
    /usr/bin/env python3 "$bot_script"
    sleep 5
done
exit 2
