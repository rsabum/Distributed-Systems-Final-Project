from subprocess import Popen, PIPE, signal
from json import dump

import time
import requests
import socket

class Constants:

    # Chose 13 to reduce probability 
    # of ties in the election process
    CLUSTER_SIZES = [13]

    # One second more than the maximum 
    # timeout of any node
    ELECTION_TIMEOUT = 3

    # Endpoints for the client
    MESSAGE = "/message"
    TOPIC = "/topic"
    STATUS = "/status"

    # Roles
    FOLLOWER = "Follower"
    LEADER = "Leader"
    CANDIDATE = "Candidate"

    # Sample topic and message
    TEST_TOPIC = "test_topic"
    TEST_MESSAGE = "test_message"

    # Paths
    PROGRAM_FILE_PATH = "src/node.py"
    CONFIG_PATH = "config.json"
    IP = "127.0.0.1"

    # Originally 1 but had to increase because 
    # requests would often timeout before a node 
    # had a chance to respond
    REQUEST_TIMEOUT = 5

    # Search for leader a few times in case
    # the leader has not been elected yet
    LEADER_LOOPS = 3




class Node:
    def __init__(self,  program_file_path: str, config_path: str, index: int, config: dict, ):
        self.config = config
        self.index = index
        self.address = self.get_address()
        self.program_file_path = program_file_path
        self.config_path = config_path

    def start(self):
        self.startup_sequence = [
            "python3",
            self.program_file_path,
            self.config_path,
            str(self.index)
        ]
        self.process = Popen(self.startup_sequence)
        self.wait_for_flask_startup()
        self.pid = self.process.pid

    def terminate(self):
        self.process.terminate()

    def kill(self):
        self.process.kill()

    def wait(self):
        self.process.wait(5)

    def pause(self):
        self.process.send_signal(signal.SIGSTOP)

    def resume(self):
        self.process.send_signal(signal.SIGCONT)

    def commit_clean(self, sleep=0):
        time.sleep(sleep)
        self.clean(sleep)

    def clean(self, sleep=1e-3):
        self.terminate()
        self.wait()
        self.kill()
        time.sleep(sleep)

    def restart(self):
        self.clean()
        self.start()

    def wait_for_flask_startup(self):
        number_of_tries = 20
        for _ in range(number_of_tries):
            try:
                return requests.get(self.address)
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)
        raise Exception('Cannot connect to server')

    def get_address(self):
        address = self.config["addresses"][self.index]
        return "http://" + address["ip"]+":"+str(address["port"])

    def put_message(self, topic: str, message: str):
        data = {"topic": topic, "message": message}
        return requests.put(self.address + Constants.MESSAGE, json=data,  timeout=Constants.REQUEST_TIMEOUT)

    def get_message(self, topic: str):
        return requests.get(self.address + Constants.MESSAGE + '/' + topic,  timeout=Constants.REQUEST_TIMEOUT)

    def create_topic(self, topic: str):
        data = {"topic": topic}
        return requests.put(self.address + Constants.TOPIC, json=data, timeout=Constants.REQUEST_TIMEOUT)

    def get_topics(self):
        return requests.get(self.address + Constants.TOPIC, timeout=Constants.REQUEST_TIMEOUT)

    def get_status(self):
        return requests.get(self.address + Constants.STATUS, timeout=Constants.REQUEST_TIMEOUT)


class Swarm:
    def __init__(self, program_file_path: str, num_nodes: int):
        self.num_nodes = num_nodes

        # create the config
        config = self.make_config()
        dump(config, open(Constants.CONFIG_PATH, 'w'))

        self.nodes = []
        for index in range(num_nodes):
            self.nodes.append(Node(
                program_file_path, 
                Constants.CONFIG_PATH, 
                index, 
                config
            ))
        

    def start(self, sleep=0):
        for node in self.nodes:
            node.start()
        time.sleep(sleep)

    def terminate(self):
        for node in self.nodes:
            node.terminate()

    def clean(self, sleep=0):
        for node in self.nodes:
            node.clean()
        time.sleep(sleep)

    def restart(self, sleep=0):
        for node in self.nodes:
            node.clean()
            node.start()
        time.sleep(sleep)

    def make_config(self):
        config = {"addresses": []}

        for i in range(self.num_nodes):
            config["addresses"].append({
                "ip": Constants.IP, 
                "port": get_free_port(), 
                "internal_port": get_free_port()
            })

        return config
    
    def get_status(self):
        statuses = {}
        for node in self.nodes:
            try:
                response = node.get_status()
                if (response.ok):
                    statuses[node.index] = response.json()
            
            except requests.exceptions.ConnectionError:
                continue

        return statuses

    def get_leader(self):
        for node in self.nodes:
            try:
                response = node.get_status()
                if (response.ok and response.json()["role"] == Constants.LEADER):
                    return node
                
            except requests.exceptions.ConnectionError:
                continue

        time.sleep(1)
        return None

    def get_leader_loop(self, times: int):
        for _ in range(times):
            leader = self.get_leader()
            if leader:
                return leader
        return None

    def __getitem__(self, key):
        return self.nodes[key]


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    s.close()
    return addr[1]
