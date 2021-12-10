#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"
test_client_artifact="out/artifacts/test_client_jar/test_client.jar"

IPs=""
while IFS= read -r line
do
  IPs="${IPs}${line%:*},"
done < "${client_ip_configs}"
IPs=$(echo "${IPs%,*}" | tr ',' '\n' | sort -u)

while IFS= read -r line
do
  scp "${test_client_artifact}" "user@${line}:~/test_client.jar"
  scp "${server_ip_configs}" "user@${line}:~/${server_ip_configs}"
done <<< "${IPs}"

while IFS= read -r line
do
  if [[ "${line}" == *":"* ]];
  then
    PORT="${line#*:}"
  else
    PORT="5000"
  fi
  ssh -n "user@${line%:*}" "echo ' ' | sudo -S ufw allow in ${PORT} ; sudo ufw allow out ${PORT}"
done < "${client_ip_configs}"

while IFS= read -r line
do
  ssh "user@${line%:*}" "java -XX:+HeapDumpOnOutOfMemoryError -jar ~/test_client.jar ${line} ~/${server_ip_configs} 2> /dev/null" &
done < "${client_ip_configs}"