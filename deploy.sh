#!/bin/bash

ip_configs="server_ips.config"
server_artifact="out/artifacts/paxos_server_jar/paxos_server.jar"

while IFS= read -r line
do
  scp "${server_artifact}" "user@${line}:~/paxos_server.jar"
  scp "${ip_configs}" "user@${line}:~/${ip_configs}"
done < "${ip_configs}"