#!/bin/bash

ip_configs="server_ips.config"
server_artifact="out/artifacts/paxos_server_jar/paxos_server.jar"

IPs=""
while IFS= read -r line
do
  IPs="${IPs}${line%:*},"
done < "${ip_configs}"
IPs=$(echo "${IPs%,*}" | tr ',' '\n' | sort -u)

while IFS= read -r line
do
  scp "${server_artifact}" "user@${line}:~/paxos_server.jar"
  scp "${ip_configs}" "user@${line}:~/${ip_configs}"
done <<< "${IPs}"

TMUX=0
LOG=" 2> /dev/null"
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -t|--tmux)
      TMUX=1
      shift
      ;;
    -l|--log)
      LOG=""
      shift
      ;;
    *)
      shift
      ;;
  esac
done

while IFS= read -r line
do
  if [[ "${line}" == *":"* ]];
  then
    PORT="${line#*:}"
  else
    PORT="5000"
  fi
  ssh -n "user@${line%:*}" "echo ' ' | sudo -S ufw allow in ${PORT} ; sudo ufw allow out ${PORT}"
done < "${ip_configs}"

while IFS= read -r line
do
  if [[ TMUX -eq 1 ]]
  then
    if [[ "${line}" == *":"* ]];
    then
      PORT="${line#*:}"
    else
      PORT="5000"
    fi
    ssh "user@${line%:*}" "tmux new-session -d -s paxos_${PORT} 'java -XX:+HeapDumpOnOutOfMemoryError -jar ~/paxos_server.jar ${line} ~/${ip_configs} ${LOG}'" &
  else
    ssh "user@${line%:*}" "java -XX:+HeapDumpOnOutOfMemoryError -jar ~/paxos_server.jar ${line} ~/${ip_configs} ${LOG}" &
  fi
done < "${ip_configs}"