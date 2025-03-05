import threading
import time
import random
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# In-memory data
message_queues = {}  # Stores committed messages
log_entries = []  # Stores uncommitted log entries
commit_index = -1  # Last committed log entry index
nodes = []  # List of follower nodes
leader = None  # Current leader
current_term = 0  # Current term
voted_for = None  # Node voted for in current term
role = "Follower"  # Current role
lock = threading.Lock()  # Lock for shared resources
election_timeout = random.uniform(1, 10)  # Randomized timeout


@app.route("/topic", methods=["PUT"])
def create_topic():
    """
    Create a new topic in the message queue.

    Body:
        topic (str): The name of the topic to create

    Returns:
        success (bool): success or failure
        error (str): error message if any
    """

    if role != "Leader":
        return jsonify({"success": False, "error": "Not the leader"}), 403

    data = request.get_json()
    topic = data.get("topic")
    if not topic:
        return jsonify({"success": False, "error": "No topic provided"}), 400

    if topic in message_queues:
        return jsonify({"success": False, "error": "Topic already exists"}), 409

    message_queues[topic] = []
    return jsonify({"success": True}), 201


@app.route("/topic", methods=["GET"])
def get_topics():
    """
    Retrieve the list of topics in the message queue.

    Body:
        None

    Returns:
        success (bool): success or failure
        topics (list): list of topics
    """

    return jsonify({"success": True, "topics": list(message_queues.keys())}), 200


@app.route("/message", methods=["PUT"])
def put_message():
    """
    Add a new message to the message queue.

    Body:
        topic (str): The name of the topic to add the message to
        message (str): The message to add to the topic

    Returns:
        success (bool): success or failure
        error (str): error message if any
    """

    if role != "Leader":
        return jsonify({"success": False, "error": "Not the leader"}), 403

    data = request.get_json()
    topic = data.get("topic")
    message = data.get("message")
    if not topic or not message:
        return jsonify({"success": False, "error": "Topic or message missing"}), 400

    if topic not in message_queues:
        return jsonify({"success": False, "error": "Topic does not exist"}), 404

    # Append to leader's log
    entry = {"term": current_term, "topic": topic, "message": message}
    log_entries.append(entry)

    # Replicate log to followers
    success_count = 1  # Leader counts as one
    for node in nodes:
        try:
            response = requests.post(
                f"{node}/append_entries", json={"entries": [entry]}, timeout=1
            )
            if response.json().get("success"):
                success_count += 1

        except requests.exceptions.RequestException:
            pass

    # Commit if majority acknowledges
    if success_count > len(nodes) // 2:
        commit_log_entry(len(log_entries) - 1)
        return jsonify({"success": True}), 200

    else:
        return jsonify({"success": False, "error": "Log replication failed"}), 500


@app.route("/message/<topic>", methods=["GET"])
def get_message(topic):
    """
    Retrieve the next message from the message queue.

    Body:
        topic (str): The name of the topic to retrieve the message from

    Returns:
        success (bool): success or failure
        message (str): the message if retrieved
        error (str): error message if any
    """

    if role != "Leader":
        return jsonify({"success": False, "error": "Not the leader"}), 403

    if topic not in message_queues or not message_queues[topic]:
        return jsonify({"success": False, "error": "No messages available"}), 404

    message = message_queues[topic].pop(0)
    return jsonify({"success": True, "message": message}), 200


@app.route("/append_entries", methods=["POST"])
def append_entries():
    """
    Append log entries to the log per the leader's request.

    Body:
        entries (list): List of log entries to append

    Returns:
        success (bool): success or failure
    """

    global log_entries, commit_index

    data = request.get_json()
    entries = data.get("entries", [])

    with lock:
        log_entries.extend(entries)

    return jsonify({"success": True}), 200


def commit_log_entry(index):
    """
    Commit a log entry and apply it to the state machine.

    Parameters:
        index (int): The index of the log entry to commit

    Returns:
        None
    """

    global commit_index

    with lock:
        if index > commit_index:
            commit_index = index
            entry = log_entries[commit_index]
            message_queues[entry["topic"]].append(entry["message"])


@app.route("/status", methods=["GET"])
def status():
    """
    Retrieve the current status of the node.

    Body:
        None

    Returns:
        role (str): The current role of the node (Follower, Candidate, Leader)
        term (int): The current term of the node
    """

    return jsonify({"role": role, "term": current_term}), 200


@app.route("/request_vote", methods=["POST"])
def request_vote():
    """
    Handle a vote request from a candidate node.

    Body:
        term (int): The term of the candidate
        candidate (str): The ID of the candidate
        last_log_index (int): The index of the candidate's last log entry

    Returns:
        vote_granted (bool): Whether the vote was granted
        term (int): The current term of the node
    """

    global current_term, voted_for, log_entries

    data = request.get_json()
    term = data.get("term")
    candidate_id = data.get("candidate")
    candidate_last_log_index = data.get("last_log_index", -1)

    with lock:
        if term > current_term:
            current_term = term
            voted_for = None  # Reset vote if term is higher

        # Check if candidate’s log is at least as up-to-date
        last_log_index = len(log_entries) - 1
        if (
            voted_for is None or voted_for == candidate_id
        ) and candidate_last_log_index >= last_log_index:
            voted_for = candidate_id
            return jsonify({"vote_granted": True, "term": current_term}), 200

    return jsonify({"vote_granted": False, "term": current_term}), 200


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    """
    Handle a heartbeat request from the leader node.

    Body:
        term (int): The term of the leader
        leader (str): The ID of the leader

    Returns:
        success (bool): success or failure
    """

    global leader
    data = request.get_json()
    leader = data.get("leader")
    return jsonify({"success": True}), 200


@app.route("/sync_log", methods=["POST"])
def sync_log():
    """
    Sync the log entries with a follower node.

    Body:
        last_index (int): The index of the last log entry known by the follower

    Returns:
        success (bool): success or failure
        entries (list): List of missing
    """

    data = request.get_json()
    last_known_index = data.get("last_index")
    missing_entries = log_entries[last_known_index + 1 :]

    return jsonify({"success": True, "entries": missing_entries}), 200


def start_election():
    """
    Initiates the election process for the current node to become the leader.

    Parameters:
        role (str): The current role of the node (Follower, Candidate, Leader)
        current_term (int): The current term of the node
        voted_for (str): The ID of the node voted for in the current term
        nodes (list): List of follower nodes

    Returns:
        None
    """
    global role, current_term, voted_for, leader

    with lock:
        role = "Candidate"
        current_term += 1
        voted_for = "self"
        votes_received = 1

    last_log_index = len(log_entries) - 1

    for node in nodes:
        try:
            response = requests.post(
                f"{node}/request_vote",
                json={
                    "term": current_term,
                    "candidate": "self",
                    "last_log_index": last_log_index,
                },
                timeout=1,
            )

            if response.json().get("vote_granted"):
                votes_received += 1

        except requests.exceptions.RequestException:
            pass

    if votes_received > len(nodes) // 2:
        become_leader()


def become_leader():
    """
    Transition the node to the leader state and start sending heartbeats.

    Parameters:
        role (str): The current role of the node (Follower, Candidate, Leader)
        leader (str): The current leader of the cluster

    Returns:
        None
    """

    global role, leader
    role = "Leader"
    leader = "self"

    # Ask followers for missing logs
    last_log_index = len(log_entries) - 1
    for node in nodes:
        try:
            response = requests.post(
                f"{node}/sync_log", json={"last_index": last_log_index}, timeout=2
            )
            missing_entries = response.json().get("entries", [])
            log_entries.extend(missing_entries)

        except requests.exceptions.RequestException:
            pass

    # Start heartbeats
    start_sending_heartbeats()


def start_sending_heartbeats():
    """
    Start sending periodic heartbeats to followers to maintain leadership.

    Parameters:
        nodes (list): List of follower nodes
        current_term (int): The current term of the node
        leader (str): The current leader of the cluster

    Returns:
        None
    """

    while role == "Leader":
        for node in nodes:
            try:
                requests.post(
                    f"{node}/heartbeat",
                    json={"term": current_term, "leader": "self"},
                    timeout=1,
                )
            except requests.exceptions.RequestException:
                pass
        time.sleep(1)


def start_election_timer():
    """
    Start the election timer to trigger new elections when needed.

    Body:
        leader (str): The current leader of the cluster
        role (str): The current role of the node (Follower, Candidate, Leader)

    Returns:
        None
    """

    while True:
        time.sleep(random.uniform(3, 5))
        if leader is None and role != "Leader":
            start_election()


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) != 3:
        sys.exit(1)

    config_file = sys.argv[1]
    idx = int(sys.argv[2])

    # Load nodes from config file
    with open(config_file, "r") as f:
        config = json.load(f)
        nodes = [f"http://{node['ip']}:{node['port']}" for node in config["addresses"]]
        nodes.pop(idx)  # Remove current node from the list of nodes

    # Start Flask app on specified IP and port
    ip, port = config["addresses"][idx]["ip"], config["addresses"][idx]["port"]

    # Start election timer in a separate thread
    threading.Thread(target=start_election_timer, daemon=True).start()

    # Start Flask app
    app.run(host=ip, port=port)
