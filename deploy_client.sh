#!/bin/bash

server_ip_configs="server_ips.config"
client_artifact="out/artifacts/paxos_client_jar/paxos_client.jar"

location=$1

scp "${client_artifact}" "user@${location%:*}:~/paxos_client.jar"
scp "${server_ip_configs}" "user@${location%:*}:~/${server_ip_configs}"

if [[ "${location}" == *":"* ]];
then
  PORT="${location#*:}"
else
  PORT="5000"
fi
ssh "user@${location%:*}" "echo ' ' | sudo -S ufw allow in ${PORT} ; sudo ufw allow out ${PORT}"

ssh "user@${location%:*}" "java -jar ~/paxos_client.jar ${location} ~/${server_ip_configs}"