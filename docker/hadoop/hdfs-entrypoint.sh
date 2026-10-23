#!/usr/bin/env bash
set -euo pipefail

role="${HDFS_ROLE:-${1:-}}"
case "$role" in
  namenode)
    if [[ ! -f /hadoop/dfs/name/current/VERSION ]]; then
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  *)
    echo "Set HDFS_ROLE to namenode or datanode." >&2
    exit 2
    ;;
esac
