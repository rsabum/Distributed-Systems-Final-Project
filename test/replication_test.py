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


def wait_for_commit(seconds=1):
    time.sleep(seconds)


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_is_create_topic_shared(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(C.LEADER_LOOPS)

    assert leader1 != None
    assert leader1.create_topic(C.TEST_TOPIC).json() == {"success": True}

    leader1.commit_clean(C.ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(C.LEADER_LOOPS)

    assert leader2 != None
    assert leader2.get_topics().json() == {
        "success": True, 
        "topics": [C.TEST_TOPIC]
    }


@pytest.mark.parametrize("num_nodes", C.CLUSTER_SIZES)
def test_is_get_message_shared(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(C.LEADER_LOOPS)

    assert leader1 != None
    assert leader1.create_topic(C.TEST_TOPIC).json() == {"success": True}
    assert leader1.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {"success": True}

    leader1.commit_clean(C.ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(C.LEADER_LOOPS)

    assert leader2 != None
    assert leader2.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": C.TEST_MESSAGE,
    }

