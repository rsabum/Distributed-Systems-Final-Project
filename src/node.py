import threading
import time
import random
import requests
from flask import Flask, request, jsonify


class Node:
    def __init__(self, address, nodes):
        self.app = Flask(__name__)

        self.current_term = 0
        self.voted_for = None
        self.log_entries = []
        
        self.last_applied = -1
        
        self.next_index = {}
        self.match_index = {}
        
        self.role = "Follower"
        self.address = address
        self.nodes = nodes
        self.message_queues = {}

        self.timeout = True
        self.election_timeout = random.uniform(1, 2)
        self.heart_rate = 0.5

        self.lock = threading.Lock()

        self.app.add_url_rule("/status", "status", self.status, methods=["GET"])
        self.app.add_url_rule("/topic", "create_topic", self.create_topic, methods=["PUT"])
        self.app.add_url_rule("/topic", "get_topics", self.get_topics, methods=["GET"])
        self.app.add_url_rule("/message", "put_message", self.put_message, methods=["PUT"])
        self.app.add_url_rule("/message/<topic>", "get_message", self.get_message, methods=["GET"])
        self.app.add_url_rule("/request_vote", "request_vote", self.request_vote, methods=["POST"])
        self.app.add_url_rule("/append_entries", "append_entries", self.append_entries, methods=["POST"])
        self.app.add_url_rule("/sync_log", "sync_log", self.sync_log, methods=["POST"])

    @property
    def last_log_index(self):
        return len(self.log_entries) - 1

    @property
    def commit_index(self):
        return len(self.log_entries) - 1

    @property
    def last_log_term(self):
        return self.log_entries[-1]["term"] if self.log_entries else 0

    @property
    def prev_log_index(self):
        return self.last_log_index - 1

    @property
    def prev_log_term(self):
        return self.log_entries[self.prev_log_index]["term"] if self.prev_log_index >= 0 else 0

    def status(self):
        return jsonify({"term": self.current_term, "role": self.role}), 200

    def create_topic(self):
        
        if self.role != "Leader":
            return jsonify({"success": False}), 400

        data = request.get_json()
        topic = data.get("topic")

        if not topic:
            return jsonify({"success": False}), 401

        if topic in self.message_queues:
            return jsonify({"success": False}), 402

        entry = {"command": "create_topic", "term": self.current_term, "topic": topic}

        self.log_entries.append(entry)

        success_count = 1
        for node in self.nodes:
            try:
                response = requests.post(
                    url=f"{node}/append_entries",
                    timeout=1,
                    json={
                        "term": self.current_term,
                        "leader_id": self.address,
                        "prev_log_index": self.prev_log_index,
                        "prev_log_term": self.prev_log_term,
                        "entries": [entry],
                        "leader_commit": self.commit_index,
                    },
                )

                data = response.json()

                term = data.get("term")
                success = data.get("success")

                if term > self.current_term:
                    self.current_term = term
                    self.role = "Follower"
                    return jsonify({"success": False}), 403

                if success:
                    success_count += 1

            except requests.exceptions.RequestException:
                pass

        if success_count > len(self.nodes) // 2:
            self.apply_log_entries()
            return jsonify({"success": True}), 200

        else:
            return jsonify({"success": False}), 500

    def get_topics(self):
        if self.role != "Leader":
            return jsonify({"success": False}), 403

        return jsonify({"success": True, "topics": list(self.message_queues.keys())}), 200
        

    def put_message(self):
        if self.role != "Leader":
            return jsonify({"success": False}), 404

        data = request.get_json()
        topic = data.get("topic")
        message = data.get("message")

        if not topic or not message:
            return jsonify({"success": False}), 405

        if topic not in self.message_queues:
            return jsonify({"success": False}), 406

        entry = {
            "command": "put_message",
            "term": self.current_term,
            "topic": topic,
            "message": message,
        }

        self.log_entries.append(entry)

        success_count = 1
        for node in self.nodes:
            try:
                response = requests.post(
                    url=f"{node}/append_entries",
                    timeout=1,
                    json={
                        "term": self.current_term,
                        "leader_id": self.address,
                        "prev_log_index": self.prev_log_index,
                        "prev_log_term": self.prev_log_term,
                        "entries": [entry],
                        "leader_commit": self.commit_index,
                    },
                )

                data = response.json()

                term = data.get("term")
                success = data.get("success")

                if term > self.current_term:
                    self.current_term = term
                    self.role = "Follower"
                    return jsonify({"success": False}), 407

                if success:
                    success_count += 1

            except requests.exceptions.RequestException:
                pass

        if success_count > len(self.nodes) // 2:
            self.apply_log_entries()
            return jsonify({"success": True}), 200

        else:
            return jsonify({"success": False}), 500

    def get_message(self, topic):
        if self.role != "Leader":
            return jsonify({"success": False}), 408

        if topic not in self.message_queues:
            return jsonify({"success": False}), 409

        if not self.message_queues[topic]:
            return jsonify({"success": False}), 410

        entry = {"command": "get_message", "term": self.current_term, "topic": topic}

        self.log_entries.append(entry)

        success_count = 1
        for node in self.nodes:
            try:
                response = requests.post(
                    url=f"{node}/append_entries",
                    timeout=1,
                    json={
                        "term": self.current_term,
                        "leader_id": self.address,
                        "prev_log_index": self.prev_log_index,
                        "prev_log_term": self.prev_log_term,
                        "entries": [entry],
                        "leader_commit": self.commit_index,
                    },
                )

                data = response.json()

                term = data.get("term")
                success = data.get("success")

                if term > self.current_term:
                    self.current_term = term
                    self.role = "Follower"
                    return jsonify({"success": False}), 411

                if success:
                    success_count += 1

            except requests.exceptions.RequestException:
                pass

        if success_count > len(self.nodes) // 2:
            message = self.apply_log_entries()
            return jsonify({"success": True, "message": message}), 200

        else:
            return jsonify({"success": False}), 500

    def request_vote(self):
        """
        Handles a vote request from a candidate node in the Raft consensus algorithm.
        This method processes a vote request by evaluating the candidate's term, log index, and log term.
        It grants or rejects the vote based on the following conditions:
        - If the candidate's term is greater than the current term, the node updates its term and becomes a follower.
        - If the candidate's term is less than the current term, the vote is rejected.
        - If the candidate's log is less up-to-date than the node's log, the vote is rejected.
        - If the node has already voted for another candidate in the current term, the vote is rejected.
        """

        data = request.get_json()

        term = data.get("term")
        candidate_id = data.get("candidate")
        last_log_index = data.get("last_log_index")
        last_log_term = data.get("last_log_term")

        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.role = "Follower"
                self.timeout = False
                self.voted_for = None

            # Reject vote if candidate's term is behind
            if term < self.current_term:
                return jsonify({"term": self.current_term, "vote_granted": False}), 200

            # Reject vote if candidate's log is behind
            if (last_log_index < self.last_log_index or last_log_term < self.last_log_term):
                return jsonify({"term": self.current_term, "vote_granted": False}), 200

            # Reject vote if already voted for another candidate
            if self.voted_for not in {None, candidate_id}:
                return jsonify({"term": self.current_term, "vote_granted": False}), 200

            self.voted_for = candidate_id
            return jsonify({"term": self.current_term, "vote_granted": True}), 200

    def append_entries(self):
        data = request.get_json()

        term = data.get("term")
        leader_id = data.get("leader_id")
        prev_log_index = data.get("prev_log_index", -1)
        prev_log_term = data.get("prev_log_term", 0)
        entries = data.get("entries", [])
        leader_commit = data.get("leader_commit", -1)

        if term >= self.current_term:
            self.current_term = term
            self.role = "Follower"
            self.timeout = False

        # Reject if term is behind
        if term < self.current_term:
            return jsonify({"term": self.current_term, "success": False}), 410

        # Reject if log is behind
        if prev_log_index >= 0 and (
            len(self.log_entries) <= prev_log_index
            or self.log_entries[prev_log_index]["term"] != prev_log_term
        ):
            return jsonify({"term": self.current_term, "success": False}), 411

        # Truncate log if there is a mismatch
        if prev_log_index >= 0:
            self.log_entries = self.log_entries[:prev_log_index + 1]

        with self.lock:
            self.log_entries.extend(entries)

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.last_log_index)

        return jsonify({"term": self.current_term, "success": True}), 200

    def sync_log(self):

        data = request.get_json()
        last_known_index = data.get("last_index")
        missing_entries = self.log_entries[last_known_index + 1 :]

        return jsonify({"success": True, "entries": missing_entries}), 200

    def apply_log_entries(self):
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log_entries[self.last_applied]

                if entry["command"] == "create_topic":
                    self.message_queues[entry["topic"]] = []

                if entry["command"] == "put_message":
                    self.message_queues[entry["topic"]].append(entry["message"])

                if entry["command"] == "get_message":
                    return self.message_queues[entry["topic"]].pop(0)

    def start_election(self):
        with self.lock:
            self.role = "Candidate"
            self.current_term += 1
            self.voted_for = self.address
            votes_received = 1

        for node in self.nodes:
            try:
                response = requests.post(
                    url=f"{node}/request_vote",
                    json={
                        "term": self.current_term,
                        "candidate": self.address,
                        "last_log_index": self.last_log_index,
                        "last_log_term": self.last_log_term,
                    },
                    timeout=1,
                )

                data = response.json()

                term = data.get("term")
                vote_granted = data.get("vote_granted")

                if term > self.current_term:
                    self.current_term = term
                    self.role = "Follower"
                    return

                if vote_granted:
                    votes_received += 1

            except requests.exceptions.RequestException:
                pass

        if votes_received > len(self.nodes) // 2:
            self.become_leader()

    def become_leader(self):
        self.role = "Leader"

        # for node in self.nodes:
        #     try:
        #         response = requests.post(
        #             url=f"{node}/sync_log",
        #             json={"last_index": self.last_log_index},
        #             timeout=1,
        #         )

        #         data = response.json()

        #         missing_entries = data.get("entries", [])
        #         self.log_entries.extend(missing_entries)

        #     except requests.exceptions.RequestException:
        #         pass

        self.apply_log_entries()
        self.send_heartbeats()

    def send_heartbeats(self):
        while self.role == "Leader":
            for node in self.nodes:
                try:
                    requests.post(
                        url=f"{node}/append_entries",
                        json={"term": self.current_term, "leader_id": self.address},
                        timeout=1,
                    )

                except requests.exceptions.RequestException:
                    pass

            time.sleep(self.heart_rate)

    def start_election_timer(self):
        while True:
            self.timeout = True
            time.sleep(self.election_timeout)
            if self.timeout and self.role != "Leader":
                self.start_election()

    def run(self, host, port):
        threading.Thread(target=self.start_election_timer, daemon=True).start()
        self.app.run(host=host, port=port)


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) != 3:
        sys.exit(1)

    config_file = sys.argv[1]
    idx = int(sys.argv[2])

    with open(config_file, "r") as f:
        config = json.load(f)
        nodes = [f"http://{node['ip']}:{node['port']}" for node in config["addresses"]]
        address = nodes.pop(idx)

    node = Node(address, nodes)
    node.run(host=config["addresses"][idx]["ip"], port=config["addresses"][idx]["port"])
