#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"
vm_name_configs="vm_names.config"

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
#IPs="${IPs}$'\n'"

RESTART=""
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -r|--restart)
      RESTART="-r"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

#while IFS= read -r line
#do
#  prlctl set "${line}" --device-set net0 --connect
#done < "${vm_name_configs}"

while IFS= read -r line
do
  ssh "user@${line%:*}" "echo ' ' | sudo -S shutdown ${RESTART} now" &
done <<< "${IPs}"
