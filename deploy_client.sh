#!/bin/bash

server_ip_configs="server_ips.config"
client_artifact="out/artifacts/paxos_client_jar/paxos_client.jar"

location=$1

scp "${client_artifact}" "${location}:~/paxos_client.jar"
scp "${server_ip_configs}" "${location}:~/${server_ip_configs}"

ssh "${location}" "java -jar ~/paxos_client.jar ~/${server_ip_configs}"