#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"

mkdir "log"
rm -f ./log/*.log ./log/*.hprof

IPs=""
while IFS= read -r line
do
  IPs="${IPs}${line%:*},"
done < "${client_ip_configs}"
while IFS= read -r line
do
  IPs="${IPs}${line%:*},"
done < "${server_ip_configs}"
IPs=$(echo "${IPs%,*}" | tr ',' '\n' | sort -u)

while IFS= read -r line
do
  scp "user@${line}:~/*.hprof" "./log/"
done <<< "${IPs}"

while IFS= read -r line
do
  for i in {0..4}
  do
    if [[ "${line}" != *":"* ]]; then
      line="${line}:5000"
    fi
    scp "user@${line%:*}:~/PaxosServer_${line//[.:]/_}.log.$i" "./log/PaxosServer_${line//[.:]/_}_$i.log"
  done
done < "${server_ip_configs}"

while IFS= read -r line
do
  for i in {0..4}
  do
    if [[ "${line}" != *":"* ]]; then
      line="${line}:5000"
    fi
    scp "user@${line%:*}:~/PaxosClient_${line//[.:]/_}.log.$i" "./log/PaxosClient_${line//[.:]/_}_$i.log"
  done
done < "${client_ip_configs}"