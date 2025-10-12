1. TIMER
2. FIX INITIAL ELECTION
3. NEW VIEW SYNCING





2. [Lean code for further develop]Refactor Server
3. Test 1 transaction per set
4. Timer in Server / Proposer
5. New View 


QUESTIONS:
1. multiple prepare requests at once
-> what is ideal design: mutex, persistent prepare routine + channels?
2. implement timers as context?
-> how to share this timer across?
3. Whats the point of client timer if i emulate network drop by actually responding
4. Accept log a



Future Test
remove interaction loop and blast
stress test



5. Figure out -> Paxos ->


Question:



## Clients
1. The clients have no idea of what nodes are alive and what nodes are faulty.
2. In the beginning, when each client sends its first message, it communicates with node n1 since it does not know who the leader is. This implies that during the system's startup, the first client directs its request to node n1, which then starts the leader election process.


Considerations
1. [DONE] All nodes maintain the last reply message they sent to each client and discard requests whose timestamp is lower than the client timestamp in the last reply they sent to the client to guarantee exactly-once semantics.
2. [DONE] A backup node nb will accept an accept message if the ballot number of the accept message is either equal to or greater than the highest ballot number that the node is aware of.
3. The leader logs all received accepted messages. (paxos server -> f+1 -> [DONE] multicast commit message) 
4. [DONE] Once the request is executed, the leader sends a reply message ⟨REPLY,b,τ,c,r⟩back to client cwhere bis the ballot number (enabling the client to identify the current leader)
5. A timer is initiated when the node receives a request, and the timer is not already running. The node stops the timer when it is no longer waiting to execute the request, but restarts it if, at that point, it is waiting to execute some other request.
6. To avoid multiple nodes initiating the leader election phase simultaneously, a node will only send a prepare message if it has not received any prepare messages in the last tp milliseconds.
7. A node will accept a prepare message if its timer has already expired and the ballot number of the prepare message is greater than any ballot number previously received. If the timer has not yet expired, the node will log the prepare messages it receives and will accept the one with the highest ballot number once its timer does expire.
8. Upon accepting a prepare message, the node responds with a promise message in the form of ⟨ACK,b,AcceptLog⟩to the proposer.
9. The AcceptLog set retains information about all requests accepted by the node since the system’s initiation. We will suggest some enhancements to make this process more efficient.
10. Once the proposer receives a quorum of f + 1 promise messages from different nodes (including itself), it creates a new-view message containing all requests that have been accepted by at least one node within the promise quorum. If a single client transaction has been accepted with multiple ballot numbers, the leader includes the one with the highest ballot number.
11. Subsequently, the leader sends out the new-view message formatted as ⟨NEW-VIEW,b,AcceptLog⟩to all nodes with b indicating the ballot number and AcceptLog being an ordered set of accept messages
12. In cases where, for a given sequence number, the leader has not received any requests (potentially leading to gaps in the sequence), the leader will insert a no-op operation in place of the client request.
13. Backup nodes follow the normal operation of the protocol and send an accepted message to the leader for each accept message in the new-view message (even if they have already sent the accepted message in the previous view).
14. The leader node then waits for accepted messages from a majority of nodes, commits and executes the request, sends a commit message to all backup nodes, and informs the client about the result.