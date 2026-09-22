# Distributed Real-Time Auction System 

A multithreaded client-server and peer-to-peer distributed auction application developed in Java using TCP sockets for control logic and UDP Go-Back-N (GBN) for reliable file transfer.

## Architectural Overview

The application combines a centralized coordinator with decentralized direct node transfers:
- Client-Server Architecture: An AuctionServer coordinates real-time bidding, user authentication, session state management, and reputation calculation via persistent TCP connections.
- Concurrent Sessions: The server supports two independent, parallel auction channels running simultaneously through dedicated manager tasks.
- Peer-to-Peer Transfer: Once a bid concludes, the server leaves the exchange. The winning buyer directly connects to the seller over UDP using the Go-Back-N sliding window protocol to retrieve item metadata files.

## Technical Components and Mechanisms

1. Reputation-Based Queueing:
Items waiting for auction are held in a shared queue. When initiating a session, the server compares the reputation score of the first item's owner against that of the second. If the second seller holds a higher reputation, their item is scheduled first to incentivize reliable participation.

2. Dynamic Reputation Scoring:
Bidders begin with an initial score of 1.0. A successful transaction raises their reputation, whereas reneging on a winning bid decreases it according to a formula with parameter beta = 0.25.

3. Backup Winner Failover:
If the highest bidder cancels a winning claim, the transaction does not crash. The server automatically penalizes the deserter and shifts purchase authorization to the next active bidder with the second highest bid.

4. Auto-Bidding Logic:
Connected peers receive real-time updates and evaluate bidding interest with a 60% probability threshold. Incremental bids increase up to 10% during standard duration and up to 20% in sniper mode (the final 10% of auction time). Bid updates are wrapped inside explicit synchronization blocks to avoid race conditions during concurrent bids.

5. Reliable File Exchange via Go-Back-N:
- Transport: UDP packets segmented into 64-byte chunks with sequence numbers.
- Sliding Window: Window size of 3 packets on sender.
- Fault Simulation: Synthetic 20% packet drop rate and 20% ACK loss to stress test retransmission and timeout handlers.
- Termination Protection: Sender timeout retry caps avoid infinite loops if final cumulative ACKs or EOF markers drop.

## Source Files

- AuctionServer.java: Main server listening on TCP port 8080, handling registrations, user sessions, and worker threads.
- AuctionSession.java: Encapsulates state for individual auction channels, including lock primitives, active item metadata, and bid histories.
- AuctionManagerTask.java: Background thread controlling auction lifecycles, timer countdowns, reputation-based queue ordering, and win events.
- ClientHandler.java: Manages inbound commands per client over TCP, command dispatching, synchronized bid placement, and winner failover.
- User.java: Models account data, trading statistics, seller/buyer counters, and reputation values.
- Peer.java: Client-side driver running TCP control connections alongside an integrated UDP server/client implementing the Go-Back-N protocol.


## Academic Context
Developed as a group project, coursework for the "Computer Networking" course, Academic Year 2025-26.
