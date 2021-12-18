Team Member (only 1): Liangyu Zhao <liangyu@cs.washington.edu>

Writeup is in the `writeup.pdf` file.

To run project, keep the `paxos_client.jar`, `paxos_server.jar`, and `servers.config` in the same folder. Then, run the following commands to deploy Paxos servers:

tmux new-session -d -s paxos_2000 java -jar paxos_server.jar 127.0.0.1:2000 servers.config
tmux new-session -d -s paxos_3000 java -jar paxos_server.jar 127.0.0.1:3000 servers.config
tmux new-session -d -s paxos_4000 java -jar paxos_server.jar 127.0.0.1:4000 servers.config
tmux new-session -d -s paxos_5000 java -jar paxos_server.jar 127.0.0.1:5000 servers.config
tmux new-session -d -s paxos_6000 java -jar paxos_server.jar 127.0.0.1:6000 servers.config

This deploys 5 Paxos servers listening to port 2000, 3000, 4000, 5000, and 6000 respectively. They are run inside tmux session paxos_2000, paxos_3000, paxos_4000, paxos_5000, and paxos_6000 respectively.

The Paxos client can be deployed with following command:

java -jar paxos_client.jar 127.0.0.1:7000 servers.config

Details about the deployment and config file can be found in `writeup.pdf`.