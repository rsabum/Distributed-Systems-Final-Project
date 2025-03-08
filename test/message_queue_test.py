import pytest
import time
import requests
from test_utils import Swarm, Constants as C


@pytest.fixture
def node_with_test_topic():
    node = Swarm(C.PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(C.ELECTION_TIMEOUT)
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": True}
    yield node
    node.clean()


@pytest.fixture
def node():
    node = Swarm(C.PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(C.ELECTION_TIMEOUT)
    yield node
    node.clean()


def test_create_topic(node):
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": True}


def test_create_topic_different(node):
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": True}
    assert node.create_topic("test_topic_different").json() == {"success": True}


def test_create_topic_duplicate(node):
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": True}
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": False}


def test_create_topic_no_topic(node):
    assert node.create_topic(None).json() == {"success": False}


def test_get_topics_empty(node):
    assert node.get_topics().json() == {"success": True, "topics": []}


def test_put_and_get_topics(node):
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": True}
    assert node.get_topics().json() == {"success": True, "topics": [C.TEST_TOPIC]}


def test_get_topics_duplicate(node):
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": True}
    assert node.create_topic(C.TEST_TOPIC).json() == {"success": False}
    assert node.get_topics().json() == {"success": True, "topics": [C.TEST_TOPIC]}


def test_get_topics_multiple(node):
    topics = []
    for i in range(5):
        topic = C.TEST_TOPIC + str(i)
        assert node.create_topic(topic).json() == {"success": True}
        topics.append(topic)
    assert node.get_topics().json() == {"success": True, "topics": topics}


def test_get_topics_multiple_with_duplicates(node):
    topics = []
    for i in range(5):
        topic = C.TEST_TOPIC + str(i)
        assert node.create_topic(topic).json() == {"success": True}
        assert node.create_topic(topic).json() == {"success": False}
        topics.append(topic)
    assert node.get_topics().json() == {"success": True, "topics": topics}


def test_put_message(node_with_test_topic):
    assert node_with_test_topic.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {
        "success": True
    }


def test_put_message_no_topic(node_with_test_topic):
    assert node_with_test_topic.put_message(None, C.TEST_MESSAGE).json() == {
        "success": False
    }


def test_put_message_no_message(node_with_test_topic):
    assert node_with_test_topic.put_message(C.TEST_TOPIC, None).json() == {
        "success": False
    }


def test_put_message_inexistent_topic(node_with_test_topic):
    assert node_with_test_topic.put_message(
        "inexistent_topic", C.TEST_MESSAGE
    ).json() == {"success": False}


def test_get_message_empty(node_with_test_topic):
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {"success": False}


def test_get_message_from_inexistent_topic(node_with_test_topic):
    assert node_with_test_topic.get_message("inexistent_topic").json() == {
        "success": False
    }


def test_put_and_get_message(node_with_test_topic):
    assert node_with_test_topic.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {
        "success": True
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": C.TEST_MESSAGE,
    }


def test_put2_and_get1_message(node_with_test_topic):
    second_message = C.TEST_MESSAGE + "2"
    assert node_with_test_topic.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {
        "success": True
    }
    assert node_with_test_topic.put_message(C.TEST_TOPIC, second_message).json() == {
        "success": True
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": C.TEST_MESSAGE,
    }


def test_put2_and_get2_message(node_with_test_topic):
    second_message = C.TEST_MESSAGE + "2"
    assert node_with_test_topic.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {
        "success": True
    }
    assert node_with_test_topic.put_message(C.TEST_TOPIC, second_message).json() == {
        "success": True
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": C.TEST_MESSAGE,
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": second_message,
    }


def test_put2_and_get3_message(node_with_test_topic):
    second_message = C.TEST_MESSAGE + "2"
    assert node_with_test_topic.put_message(C.TEST_TOPIC, C.TEST_MESSAGE).json() == {
        "success": True
    }
    assert node_with_test_topic.put_message(C.TEST_TOPIC, second_message).json() == {
        "success": True
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": C.TEST_MESSAGE,
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {
        "success": True,
        "message": second_message,
    }
    assert node_with_test_topic.get_message(C.TEST_TOPIC).json() == {"success": False}
