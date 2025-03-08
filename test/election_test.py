import pytest
import time
import requests
from test_utils import Swarm, Constants as C


@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(C.PROGRAM_FILE_PATH, num_nodes)
    swarm.start(C.ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()


def collect_leaders_in_buckets(leader_each_terms: dict, new_statuses: list):
    for i, status in new_statuses.items():
        assert "term" in status.keys()
        term = status["term"]

        assert "role" in status.keys()
        role = status["role"]

        if role == C.LEADER:
            leader_each_terms[term] = leader_each_terms.get(term, set())
            leader_each_terms[term].add(i)


def assert_leader_uniqueness_each_term(leader_each_terms):
    for leader_set in leader_each_terms.values():
        assert len(leader_set) <= 1


@pytest.mark.parametrize("num_nodes", [1])
def test_correct_status_message(swarm: Swarm, num_nodes: int):
    status = swarm[0].get_status().json()
    assert "role" in status.keys()
    assert "term" in status.keys()
    assert type(status["role"]) == str
    assert type(status["term"]) == int


@pytest.mark.parametrize("num_nodes", [1])
def test_leader_in_single_node_swarm(swarm: Swarm, num_nodes: int):
    status = swarm[0].get_status().json()
    assert status["role"] == C.LEADER


@pytest.mark.parametrize("num_nodes", [1])
def test_leader_in_single_node_swarm_restart(swarm: Swarm, num_nodes: int):
    status = swarm[0].get_status().json()
    assert status["role"] == C.LEADER
    swarm[0].restart()
    time.sleep(C.ELECTION_TIMEOUT)
    status = swarm[0].get_status().json()
    assert status["role"] == C.LEADER


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_is_leader_elected(swarm: Swarm, num_nodes: int):
    leader = swarm.get_leader_loop(3)
    assert leader != None


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_is_leader_elected_unique(swarm: Swarm, num_nodes: int):
    leader_each_terms = {}
    statuses = swarm.get_status()
    collect_leaders_in_buckets(leader_each_terms, statuses)

    assert_leader_uniqueness_each_term(leader_each_terms)


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_is_newleader_elected(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(C.LEADER_LOOPS)
    assert leader1 != None
    leader1.clean(C.ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(C.LEADER_LOOPS)
    assert leader2 != None
    assert leader2 != leader1


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_follower_rejects_client_requests(swarm: Swarm, num_nodes: int):
    leader = swarm.get_leader_loop(C.LEADER_LOOPS)
    assert leader is not None
    assert leader.create_topic(C.TEST_TOPIC).json() == {"success": True}

    for node in swarm.nodes:
        if node.get_status().json()["role"] == C.LEADER:
            continue

        assert node.create_topic(C.TEST_TOPIC).json() == {"success": False}
        assert node.get_topics().json() == {"success": False}
        assert node.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {
            "success": False
        }
        assert node.get_message(C.TEST_TOPIC).json() == {"success": False}


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_no_leader_if_over_half_nodes_down(swarm: Swarm, num_nodes: int):
    leader = swarm.get_leader_loop(C.LEADER_LOOPS)
    assert leader is not None

    for i in range((num_nodes + 1) // 2):
        swarm.nodes[i].terminate()

    time.sleep(C.ELECTION_TIMEOUT)

    statuses = swarm.get_status()
    for _, status in statuses.items():
        assert status["role"] != C.LEADER
