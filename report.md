
# Distributed Chat Application Report

## 1. Architecture

The application is a distributed, real-time chat system composed of browser clients, an authentication API, a PostgreSQL database, and two WebSocket chat nodes. The browser pages, `index.html` and `indexB.html`, connect to Node A and Node B respectively. Both nodes serve authenticated clients and exchange messages over a separate server-to-server WebSocket connection. This topology demonstrates communication across multiple chat nodes without requiring each browser to connect to both nodes.

### Authentication and Data Management

`authServer.js` implements the Express authentication API. Its primary endpoints include `POST /api/register`, `POST /api/login`, and `GET /api/health`. Registration validates the username, email, and password, hashes the password with bcrypt, and inserts the account using parameterized PostgreSQL queries. At startup, the service creates the `users` table and a unique index on the lower-cased username. PostgreSQL stores account information, while chat messages are not persisted as historical records.

The authentication API signs a JSON Web Token (JWT) after successful password verification. The token contains the user's ID as its subject and the username as a claim. The browser stores the token in session storage and presents it to the WebSocket server during connection authentication. Each chat node verifies the token using the shared JWT signing secret before accepting the browser client as authenticated.

The username associated with a chat message comes from the verified identity rather than a username supplied in the message text. This prevents clients from freely choosing another user's displayed identity through ordinary message content.

Password hashes are stored instead of plaintext passwords. The API also applies Helmet security headers, CORS origin restrictions, request-body size limits, and separate rate limits for login and registration.

To reduce timing differences that could help an attacker distinguish unknown email addresses from registered accounts, the login route performs a bcrypt comparison against a fixed dummy password hash when the requested email does not exist. Unknown-email and incorrect-password attempts receive the same generic authentication error. This reduces one source of information leakage but does not eliminate every possible timing side channel.

The API issues short-lived JWTs according to the configured token expiry. The chat nodes independently validate these tokens when authenticating browser connections.

### Message Flow and Resilience

When an authenticated client sends non-empty text, its node creates a message packet containing a UUID and the message text associated with the verified username. The node broadcasts the message to its local clients and attempts to deliver the packet to its peer. The receiving node checks the message ID to identify duplicates, broadcasts newly received messages to its local clients, and acknowledges the packet. The sender removes acknowledged packets from its pending queue.

Peer connections use a shared internal authentication secret. Message handling uses bounded in-memory queues of up to 1,000 pending packets per node, an acknowledgement timeout, and up to five automatic retries after the initial attempt. Nodes also maintain a bounded collection of recent message IDs to suppress duplicate processing.

Node A reconnects to Node B using exponential backoff, beginning at two seconds and increasing to a maximum delay of 30 seconds. The browser clients also attempt to reconnect after disconnection.

These mechanisms improve communication during transient connection failures, but they do not provide durable storage. Pending queues, failed-message tracking, acknowledgement timers, and duplicate-detection state are held in process memory and may be lost when a server restarts.

### Deployment Architecture

The deployed application uses Vercel for the browser frontend, Render for the authentication API and WebSocket nodes, and Neon PostgreSQL for account storage.

The principal production endpoints are:

- Frontend: https://distributed-system-chat-app.vercel.app/
- Node B client: https://distributed-system-chat-app.vercel.app/indexB.html
- Authentication API health: https://chat-auth-api.onrender.com/api/health
- Node A WebSocket service: `wss://chat-node-a.onrender.com`
- Node B WebSocket service: `wss://chat-node-b.onrender.com`

The authentication API's health endpoint has been verified to return a healthy status with a connected database. The deployment configuration keeps signing and peer authentication secrets on the server side rather than exposing them in browser code.

## 2. Challenges and Implemented Solutions

### Coordinating Two Delivery Paths

Each node must support local clients while also relaying messages to the other node. A disconnected peer or delayed acknowledgement can otherwise cause messages to be lost or delivered more than once.

The implementation assigns each message a UUID, uses acknowledgements to confirm peer processing, retries timed-out messages, and maintains bounded queues for pending delivery. The receiving node acknowledges duplicate packets as well as newly processed packets. This allows a sender to stop retrying a message that the receiver has already processed.

The design prioritizes controlled retry behavior and bounded resource use. Its main limitation is that queues and duplicate-detection state are process-local rather than durable. A restart can discard pending or failed packets and the duplicate cache. A persistent broker or database-backed outbox would be needed for stronger restart-safe delivery guarantees.

### Establishing Trustworthy Identity

The browser is not a trusted source for a sender's identity, and opening a WebSocket connection does not itself authenticate a user. The authentication API therefore issues signed JWTs after password verification.

Both nodes validate browser authentication tokens and derive the displayed username from verified claims. The servers reject invalid or expired tokens and connections that fail to authenticate within the configured timeout. Node-to-node connections use a separate internal authentication secret, preventing the peer URL alone from serving as proof of identity.

The API uses bcrypt for password hashing and parameterized SQL queries for database operations. Login and registration endpoints have separate rate limits, and unknown-email login attempts use a dummy bcrypt comparison to reduce timing differences.

These protections reduce several common risks but do not replace comprehensive security testing, secure secret management, monitoring, and regular dependency maintenance.

### Configuration Across Local and Hosted Environments

The HTML files contain hosted API and WebSocket URLs, while the servers provide local development defaults. This creates a configuration challenge because browser endpoints, backend ports, CORS origins, database settings, and authentication secrets must match the environment.

The setup documentation explains how to configure the three services and change the browser URLs for local testing. In the current configuration, the default local ports are:

- Authentication API: `3000`
- Node A: `8090`
- Node B: `8081`

The server implementations also support the hosting platform's `PORT` environment variable. The deployed nodes use secure WebSocket URLs and server-side environment variables for authentication secrets.

For continued development, the browser endpoints could be moved into an environment-specific configuration file or build-time configuration mechanism rather than being edited inline.

### Error Handling and Authentication Timing

Authentication errors should reveal as little information as practical about whether an account exists. The login API returns a generic error for unknown email addresses and incorrect passwords. The dummy bcrypt hash ensures that an unknown-email request still performs a password-hash comparison instead of returning immediately after the database lookup.

The API also uses request validation and rate limiting to reduce abuse. These measures improve defensive behavior, although timing differences may still arise from database access, network variation, and other processing paths.

## 3. Performance Evaluation and Potential Improvements

### Evaluation

The repository does not contain a comprehensive benchmark suite, load test, latency report, or measured production traffic analysis. Consequently, this report does not claim a measured maximum throughput, response time, or maximum number of concurrent users.

The current evaluation is based on implementation review, focused manual tests, and observed deployment behavior. The authentication test script, `testNodeAAuth.js`, checks valid-token authentication, invalid-token rejection, and rejection of a connection without a token. Additional manual tests have been used to verify cross-node communication, acknowledgement handling, retry behavior, duplicate detection, queue ordering, and reconnection scenarios.

The production authentication API health endpoint has also been verified to report a connected database. Authentication rate limiting, CORS behavior, HTTP security headers, and invalid-token rejection have been checked during development and deployment verification.

These checks provide useful functional evidence but do not establish performance under load or guarantee the absence of security vulnerabilities. The `npm test` command remains a placeholder rather than a passing automated test suite.

Several implementation controls make resource usage more predictable under ordinary development loads:

- Each peer queue is capped at 1,000 pending packets.
- Each node's duplicate-detection collection retains at most 5,000 recent message IDs.
- The PostgreSQL connection pool allows up to five connections.
- The authentication API applies separate login and registration rate limits.
- Peer delivery is serialized around acknowledgements, allowing the sender to wait for confirmation of the oldest queued message before advancing.

These controls favor bounded memory usage, ordered queue processing, and controlled retry behavior over maximum peer-link throughput.

The application broadcasts messages by iterating over each node's in-memory set of connected clients. Processing work therefore grows with the number of clients connected to that node. A slow or disconnected peer can also accumulate queued work until the queue limit is reached. Neither client membership nor message queues are shared between independent server processes.

The authentication rate limiters use in-memory counters. If the service runs multiple independent instances, those counters are not automatically shared across instances, and they can reset when a process restarts.

### Potential Improvements

1. **Measure before scaling.** Add repeatable tests for registration and login latency, WebSocket round-trip latency, message throughput, concurrent connections, and delivery during peer failure. Record the test environment, message size, client count, throughput, and percentile latency.

2. **Make message delivery durable.** Introduce a persistent broker or transactional outbox so pending messages, retry state, and duplicate-detection records can survive process restarts. Define the required delivery semantics and test them under node failure and recovery.

3. **Improve fan-out and horizontal scaling.** Move client and presence coordination to a shared publish/subscribe layer. Consider batching and backpressure controls for large message fan-outs. The current per-process client set cannot directly reach clients attached to unrelated replicas without additional message propagation.

4. **Expand automated test coverage.** Replace the placeholder `npm test` command with unit and integration tests for validation, login throttling, JWT rejection and expiry, peer authentication, duplicate handling, retry limits, queue recovery, and reconnect behavior. Keep controlled load testing separate from functional correctness tests.

5. **Improve observability and configuration.** Add structured metrics and logs for queue depth, acknowledgement latency, reconnect attempts, failed messages, and connected clients. Provide consistent health checks for both WebSocket nodes and move browser service URLs into environment-specific configuration.

6. **Strengthen production authentication controls.** Continue reviewing rate limits, authentication timing behavior, token expiry, secret rotation, and database access. If the API scales to multiple instances, consider a shared rate-limit store and monitor authentication failures without logging passwords, JWTs, or other sensitive credentials.

7. **Define recovery and retention policies.** Specify how failed messages are inspected, retried, or discarded, and how duplicate-detection state is retained. Document the distinction between peer acknowledgement and successful delivery to every browser client.

## 4. Conclusion

The Distributed Chat Application demonstrates authenticated real-time communication across two WebSocket nodes. It combines PostgreSQL-backed account storage, bcrypt password hashing, JWT-based browser authentication, peer authentication, message acknowledgements, duplicate detection, retry handling, and bounded offline queues.

The application has been deployed using Vercel, Render, and Neon PostgreSQL, and its authentication API health endpoint has been verified. Focused manual testing has also provided evidence of authentication rejection and cross-node message-delivery behavior.

However, the current implementation remains a small-scale demonstration rather than a system with proven production-scale performance or durable message delivery. Its most important next steps are comprehensive automated testing, repeatable performance measurements, durable message coordination, stronger observability, and continued security review before increasing the number of nodes or relying on delivery across server restarts.
