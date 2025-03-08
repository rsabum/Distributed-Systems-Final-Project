# Testing Report

## 1. Single Node Message Queue  

**Tests** [`message_queue_test.py`](test/message_queue_test.py):  
- **Topic Management**: Creation of new topics, handling of duplicate topics, and edge cases (e.g., `None` or empty topic names).  
- **Message Handling**: Insertion and retrieval of messages, ensuring FIFO order is maintained.  
- **Empty Queue Behavior**: Requests for messages when no messages are present.  

**Limitations**:  
- **Concurrency Testing**: No stress tests to evaluate performance under high concurrency or large payloads.  
- **Persistence Validation**: Not tested, as the system is designed to be in-memory.  

## 2. Leader Election  

**Tests** [`election_test.py`](test/election_test.py):  
- **Election Process**: Ensures a leader is correctly elected, handles term transitions, and verifies role changes.  
- **Leader Uniqueness**: Confirms only one leader per term is elected and followers reject client requests.  
- **Failure Handling**: Tests leader election after node failure and verifies no leader is elected when over half the nodes are down.  

**Limitations**:  
- **Cluster Size Variation**: Only tested with 13-node clusters; smaller clusters (e.g., 3-node) are untested.  
- **Split-Brain Scenarios**: No tests for network partitions or scenarios where multiple nodes incorrectly assume leadership.  
- **Leader Stability**: Long-term leader retention under continuous operation is untested.  

## 3. Log Replication  

**Tests** [`replication_test.py`](test/replication_test.py):  
- **Replication Across Leaders**: Ensures messages and topics persist across leader changes.  
- **Commit Consistency**: Validates that logs remain consistent after a leader restart.  

**Limitations**:  
- **Network Failures**: No tests for partial network failures or their impact on replication.  
- **Log Divergence Recovery**: Unclear how the system handles nodes with conflicting logs.  
- **Performance Under Load**: Replication latency is not measured under high traffic.  

## General Testing Limitations  

- **Determinism**: Tests rely on fixed timeouts, which may lead to failures on slower systems.  
- **Scalability**: Limited to 13-node clusters; smaller configurations remain untested.  
- **Edge Cases**: No explicit tests for simultaneous node failures or Byzantine faults.  
- **Performance Metrics**: Lack of benchmarks for throughput, latency, or resource consumption.
