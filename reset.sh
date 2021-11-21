#!/bin/bash

ip_configs="server_ips.config"

while IFS= read -r line
do
  ssh "user@${line}" "echo ' ' | sudo -S shutdown -r now" &
done < "${ip_configs}"