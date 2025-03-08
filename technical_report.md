# Technical Report

Implementation: [`node.py`](\src\node.py)

## 1. Single Node Message Queue Implementation  

### Design Overview  
- **Data Storage**: Topics and messages are managed in a dictionary (`message_queues`), with topic names as keys and FIFO queues (lists) as values.  
- **API Endpoints**: Flask handles topic creation (`/topic`), message insertion (`/message`), and retrieval (`/message/<topic>`).  
- **Concurrency**: `threading.Lock` ensures atomic operations on shared resources.  
- **Leadership**: Only the leader handles topic creation and message insertion, maintaining consistency.  
- **Logging & Replication**: Operations are logged with term metadata and replicated to followers for fault tolerance.  
- **Leader Election**: Nodes initiate elections using randomized timeouts to prevent collisions.  
- **Heartbeats**: Leaders periodically send heartbeats to prevent unnecessary elections.

### Limitations  
- **Persistence**: Data is stored in memory and lost on restart.  
- **Scalability**: A dictionary-based approach may become inefficient at scale.  
- **Concurrency**: Single-threaded Flask limits throughput.  
- **Network Assumptions**: No retries for failed RPCs.  
- **No Pre-Vote Mechanism**: Candidates may disrupt stable clusters.  
- **Timeouts & Log Growth**: Fixed timeouts limit adaptability; logs grow indefinitely, risking memory exhaustion.  
- **Batching**: Entries are replicated one at a time, reducing efficiency.

## 2. Raft Leader Election  
### Design Overview  
- **State Management**: Nodes transition between `Follower`, `Candidate`, and `Leader` roles based on timeouts and RPCs.  
- **Election Process**: Nodes start elections if they suspect leader failure, requesting votes and winning by majority.  
- **Voting & Term Management**: Candidates follow Raft rules for term updates, log completeness, and vote counting.  
- **Heartbeats**: Leaders suppress elections by sending periodic `/append_entries` messages.  
- **Role Reversion**: Candidates revert to followers if they detect a leader with a higher term.

### Limitations (Shared with Section 1)  
- **Network Reliability**: No retries for failed RPCs.  
- **Persistence & Scalability**: Data loss on restart; dictionary-based storage inefficiencies.  
- **Single-Threaded Flask**: Limited concurrency.  
- **Fixed Timeouts & No Pre-Vote**: Risk of unnecessary elections.  
- **Log Growth & Batching**: No compaction; inefficient replication.

## 3. Log Replication & Fault Tolerance  
### Design Overview  
- **Log Structure**: Operations (topic creation, message PUT/GET) are stored as log entries with term metadata.  
- **Replication Protocol**: Leaders replicate entries via `/append_entries` and commit after quorum acknowledgment.  
- **State Machine Updates**: `apply_log_entries` ensures atomic updates.  
- **Fault Recovery**: Followers sync missing entries and truncate mismatched logs.

### Limitations (Shared with Sections 1 & 2)  
- **Log Growth**: No compaction, risking memory exhaustion.  
- **Read Efficiency**: GET operations are logged, potentially slowing down reads.  
- **Batching**: Replication is one entry at a time, reducing throughput.

## References  
- [Raft Paper](https://raft.github.io/raft.pdf)  
- [Flask Documentation](https://flask.palletsprojects.com/)  
- Python `threading` Module  
