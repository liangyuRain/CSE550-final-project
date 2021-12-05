#!/bin/bash

ip_configs="server_ips.config"
server_artifact="out/artifacts/paxos_server_jar/paxos_server.jar"

while IFS= read -r line
do
  scp "${server_artifact}" "user@${line}:~/paxos_server.jar"
  scp "${ip_configs}" "user@${line}:~/${ip_configs}"
done < "${ip_configs}"

TMUX=0
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -t|--tmux)
      TMUX=1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

while IFS= read -r line
do
  if [[ TMUX -eq 1 ]]
  then
    ssh "user@${line}" "tmux new-session -d -s paxos 'java -XX:+HeapDumpOnOutOfMemoryError -jar ~/paxos_server.jar ~/${ip_configs}'" &
  else
    ssh "user@${line}" "java -XX:+HeapDumpOnOutOfMemoryError -jar ~/paxos_server.jar ~/${ip_configs}" &
  fi
done < "${ip_configs}"