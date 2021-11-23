#!/bin/bash

target_ip=$1

ssh "user@${target_ip}" "echo ' ' | sudo -S ufw allow in 5000 ; sudo ufw allow out 5000"