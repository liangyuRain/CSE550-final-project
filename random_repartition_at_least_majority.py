import os
import random
import time
from datetime import datetime

TIME_INTERVAL = 60
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

        majority = random.sample(list(server_status), int(len(server_status) / 2) + 1)

        print(f"{majority}")
        for k, v in server_status.items():
            if v and (k not in majority):
                print(f"Turning off {k}")
                server_status[k] = False
                turn_off(k)
        for k, v in server_status.items():
            if (not v) and (k in majority):
                print(f"Turning on {k}")
                server_status[k] = True
                turn_on(k)
        print(f"{datetime.now()}: {server_status}")
        time.sleep(TIME_INTERVAL)
