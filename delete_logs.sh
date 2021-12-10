#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"

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
  ssh -n "user@${line%:*}" "rm -f *log* *.hprof"
done <<< "${IPs}"
