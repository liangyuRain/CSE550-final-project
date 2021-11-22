#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"
test_client_artifact="out/artifacts/test_client_jar/test_client.jar"

while IFS= read -r line
do
  scp "${test_client_artifact}" "user@${line}:~/test_client.jar"
  scp "${server_ip_configs}" "user@${line}:~/${server_ip_configs}"
done < "${client_ip_configs}"


while IFS= read -r line
do
  ssh "user@${line}" "java -jar ~/test_client.jar ~/${server_ip_configs}"
done < "${client_ip_configs}"