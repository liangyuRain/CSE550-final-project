#!/bin/bash

client_ip_configs="client_ips.config"
server_ip_configs="server_ips.config"

while IFS= read -r line
do
  ssh "user@${line}" "echo ' ' | sudo -S ufw allow in 5000 ; sudo ufw allow out 5000 ; sudo shutdown -r now" &
done < "${server_ip_configs}"

while IFS= read -r line
do
  ssh "user@${line}" "echo ' ' | sudo -S ufw allow in 5000 ; sudo ufw allow out 5000 ; sudo shutdown -r now" &
done < "${client_ip_configs}"