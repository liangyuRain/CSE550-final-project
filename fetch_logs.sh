#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"

mkdir "log"

while IFS= read -r line
do
  scp "user@${line}:~/PaxosServer.log" "./log/PaxosServer${line//./_}.log"
done < "${server_ip_configs}"

while IFS= read -r line
do
  scp "user@${line}:~/PaxosClient.log" "./log/PaxosClient${line//./_}.log"
done < "${client_ip_configs}"