#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"
client_artifact="out/artifacts/paxos_client_jar/paxos_client.jar"

while IFS= read -r line
do
  scp "${client_artifact}" "user@${line}:~/paxos_client.jar"
  scp "${server_ip_configs}" "user@${line}:~/${server_ip_configs}"
done < "${client_ip_configs}"


while IFS= read -r line
do
  ssh "user@${line}" "java -jar ~/paxos_client.jar ~/${server_ip_configs}"
done < "${client_ip_configs}"