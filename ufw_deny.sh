#!/bin/bash

target_ip=$1

ssh "user@${target_ip}" "echo ' ' | sudo -S ufw deny in 5000 ; sudo ufw deny out 5000"