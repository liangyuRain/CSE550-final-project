#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"
vm_name_configs="vm_names.config"

while IFS= read -r line
do
  prlctl set "${line}" --device-set net0 --connect
done < "${vm_name_configs}"

while IFS= read -r line
do
  ssh "user@${line}" "echo ' ' | sudo -S shutdown -r now" &
done < "${server_ip_configs}"

while IFS= read -r line
do
  ssh "user@${line}" "echo ' ' | sudo -S shutdown -r now" &
done < "${client_ip_configs}"