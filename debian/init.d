#!/bin/sh
### BEGIN INIT INFO
# Provides:          pgdb_tools
# Required-Start:    $local_fs $remote_fs
# Required-Stop:     $local_fs $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: The PostgreSQL tools provided are aimed at query result manipulation and abstracting database connection handling.
# Description:       The PostgreSQL tools provided are aimed at query result manipulation and abstracting database connection handling.
### END INIT INFO

set -e


case $1 in
    start|start_boot|console|console_clean|console_boot|foreground)
        mkdir -p -m 0777 /tmp/erl_pipes
        export RELX_REPLACE_OS_VARS=true
        export RELX_OUT_FILE_PATH="/home/pgdbtools/pgdb_tools/releases/default"
        ;;
    *)
        ;;
esac


su pgdbtools -c 'cd /home/pgdbtools/pgdb_tools; . erlang_env.sh; ./bin/pgdb_tools "$@"' -- $0 "$@"
