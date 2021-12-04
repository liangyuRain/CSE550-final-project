import os
import random
import time
from datetime import datetime

TIME_INTERVAL = 10
TURN_OFF_PROB = 0.25

def turn_on(name):
    os.system(f"prlctl set {name} --device-set net0 --connect")

def turn_off(name):
    os.system(f"prlctl set {name} --device-set net0 --disconnect")

if __name__ ==  '__main__':
    server_status = {}
    with open("vm_names.config") as f:
        server_status = {l[:-1]: True for l in f}
    ips = list(server_status.keys())
    while True:
        server_on = [ip for ip, status in server_status.items() if status]
        server_off = [ip for ip, status in server_status.items() if not status]

        ip = None
        if len(server_on) == 0 or len(server_off) == 0:
            ip = random.choice(ips)
        else:
            if random.random() <= TURN_OFF_PROB: # turn off random server
                ip = random.choice(server_on)
            else: # turn on random server
                ip = random.choice(server_off)
        
        status = server_status[ip]
        if status:
            print(f"Turning off {ip}")
            server_status[ip] = False
            turn_off(ip)
        else:
            print(f"Turning on {ip}")
            server_status[ip] = True
            turn_on(ip)
        print(f"{datetime.now()}: {server_status}")
        time.sleep(TIME_INTERVAL)
