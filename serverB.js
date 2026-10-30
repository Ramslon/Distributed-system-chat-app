

require('dotenv').config();

const WebSocket = require('ws');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const messageStore = require('./messageStore');

if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 32) {
  throw new Error(
    'JWT_SECRET is missing or too short. Check your .env file.'
  );
}

if (
  !process.env.INTERNAL_PEER_SECRET ||
  process.env.INTERNAL_PEER_SECRET.length < 32
) {
  throw new Error(
    'INTERNAL_PEER_SECRET is missing or too short. Check your .env file.'
  );
}

function verifyAuthToken(token) {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    if (
      typeof decoded.sub !== 'string' ||
      typeof decoded.username !== 'string'
    ) {
      return null;
    }

    return {
      id: decoded.sub,
      username: decoded.username
    };
  } catch (error) {
    return null;
  }
}

function isValidPeerSecret(candidate) {
  if (typeof candidate !== 'string') {
    return false;
  }

  const expectedBuffer = Buffer.from(
    process.env.INTERNAL_PEER_SECRET
  );

  const candidateBuffer = Buffer.from(candidate);

  return (
    candidateBuffer.length === expectedBuffer.length &&
    crypto.timingSafeEqual(candidateBuffer, expectedBuffer)
  );
}

// Render supplies PORT in production.
// Locally, Node B continues to use port 8081.
const PORT =
  Number(process.env.PORT || process.env.NODE_B_PORT) || 8081;

const server = new WebSocket.Server({ port: PORT });

const clients = new Set();



// Track message IDs to prevent duplicate processing.
const seenMessageIds = new Set();

// Maximum number of message IDs retained in memory.
const MAX_SEEN_MESSAGE_IDS = 5000;

// Remember a message ID and report whether it is new.
function rememberMessageId(messageId) {
  if (seenMessageIds.has(messageId)) {
    return false;
  }

  seenMessageIds.add(messageId);

  // Remove the oldest ID when the cache exceeds its limit.
  if (seenMessageIds.size > MAX_SEEN_MESSAGE_IDS) {
    const oldestId = seenMessageIds.values().next().value;
    seenMessageIds.delete(oldestId);
  }

  return true;
}

// Store the connection to Server Node A separately.
let peerSocket = null;

// Stores messages waiting to be delivered to Node A.
const pendingMessages = [];

let peerAuthenticated = false;

// Track messages sent to Node A that are awaiting ACKs.
const awaitingAcks = new Set();

// Wait this long for an ACK before retrying a message.
const ACK_TIMEOUT_MS = 5000;

// Maximum number of automatic retries after the initial send.
const MAX_RETRIES = 5;

// Messages that exhausted their retries and need later recovery.
const failedMessages = new Map();

// Track the timeout associated with each in-flight message.
const ackTimers = new Map();

// Count retries for each message ID.
const retryCounts = new Map();

// Limit the queue to prevent unlimited memory growth.
const MAX_PENDING_MESSAGES = 1000;

// Identify the peer connection that owns the active queue flush.
let queueFlushOwner = null;

function releaseQueueFlush(owner) {
  if (queueFlushOwner === owner) {
    queueFlushOwner = null;
  }
}

function invalidateQueueFlush(peer) {
  if (queueFlushOwner && queueFlushOwner.peer === peer) {
    queueFlushOwner = null;
  }
}

// Prevent overlapping PostgreSQL recovery operations.
let recoveringDeliveries = false;

// Remember a refill request made while a database load is already running.
let pendingDeliveryLoadRequested = false;

// Retry database-backed delivery work without creating duplicate timers.
let databaseRetryTimer = null;

// Track messages waiting for PostgreSQL to persist their failed status.
const failedStatusRetryQueue = new Map();

let failedStatusRetryTimer = null;

function scheduleDatabaseRetry() {
  if (databaseRetryTimer) {
    return;
  }

  databaseRetryTimer = setTimeout(() => {
    databaseRetryTimer = null;
    void loadPersistedDeliveries();
  }, ACK_TIMEOUT_MS);
}

function clearDatabaseRetry() {
  if (!databaseRetryTimer) {
    return;
  }

  clearTimeout(databaseRetryTimer);
  databaseRetryTimer = null;
}

function scheduleFailedStatusRetry() {
  if (failedStatusRetryTimer || failedStatusRetryQueue.size === 0) {
    return;
  }

  failedStatusRetryTimer = setTimeout(async () => {
    failedStatusRetryTimer = null;

    for (const [messageId, errorMessage] of failedStatusRetryQueue) {
      // A persisted ACK takes priority over marking the message failed.
      if (ackRetryQueue.has(messageId)) {
        continue;
      }

      try {
        const recorded = await messageStore.markDeliveryFailed(
          messageId,
          'A',
          errorMessage
        );

        const status = recorded
          ? 'failed'
          : await messageStore.getDeliveryStatus(messageId, 'A');

        if (status === 'acknowledged') {
          failedStatusRetryQueue.delete(messageId);

          const index = pendingMessages.findIndex((queuedPacket) => {
            try {
              return JSON.parse(queuedPacket).id === messageId;
            } catch {
              return false;
            }
          });

          if (index !== -1) {
            pendingMessages.splice(index, 1);
          }

          failedMessages.delete(messageId);
          clearAckTimer(messageId);
          awaitingAcks.delete(messageId);
          retryCounts.delete(messageId);

          console.log(`ACK recovered message during failed-status retry: ${messageId}`);

          void loadPersistedDeliveries();
          continue;
        }

        if (status !== 'failed') {
          continue;
        }

        const index = pendingMessages.findIndex((queuedPacket) => {
          try {
            return JSON.parse(queuedPacket).id === messageId;
          } catch {
            return false;
          }
        });

        if (index !== -1) {
          const [packet] = pendingMessages.splice(index, 1);
          failedMessages.set(messageId, packet);
        }

        failedStatusRetryQueue.delete(messageId);
        clearAckTimer(messageId);
        awaitingAcks.delete(messageId);
        retryCounts.delete(messageId);

        console.warn(`Message ${messageId} is now persisted as failed.`);
        console.warn(`Messages awaiting recovery: ${failedMessages.size}`);

        void loadPersistedDeliveries();
      } catch (error) {
        console.error(
          `Failed-status persistence retry failed for ${messageId}:`,
          error.message
        );
      }
    }

    if (failedStatusRetryQueue.size > 0) {
      scheduleFailedStatusRetry();
    }

    void flushPendingMessages();
  }, ACK_TIMEOUT_MS);
}
// Store message IDs whose ACKs still need to be persisted.
// The original message packet is not needed to retry an ACK.
const ackRetryQueue = new Map(); // messageId -> true
let ackRetryTimer = null;

function scheduleAckRetry() {
  if (ackRetryTimer) return;

  ackRetryTimer = setTimeout(async () => {
    ackRetryTimer = null;

    for (const [messageId] of ackRetryQueue) {
      try {
        const recorded = await messageStore.acknowledgeDelivery(messageId, 'A');
        if (recorded) {
          ackRetryQueue.delete(messageId);
          failedStatusRetryQueue.delete(messageId);
          failedMessages.delete(messageId);

          clearAckTimer(messageId);
          awaitingAcks.delete(messageId);
          retryCounts.delete(messageId);

          const index = pendingMessages.findIndex((queuedPacket) => {
            try { return JSON.parse(queuedPacket).id === messageId; } catch { return false; }
          });

          if (index !== -1) {
          pendingMessages.splice(index, 1);
          }

          void loadPersistedDeliveries();

          console.log(`ACK persisted successfully after retry: ${messageId}`);
        }
      } catch (error) {
        console.error(`ACK retry failed for ${messageId}:`, error.message);
      }
    }

    // If queue still has items, reschedule with backoff
    if (ackRetryQueue.size > 0) {
      scheduleAckRetry();
    }
  }, 5000); // retry every 5s (could implement exponential backoff)
}


// Load persisted deliveries for Node A into Node B's FIFO queue.
// This function does not automatically reset failed database records.
async function loadPersistedDeliveries() {
  if (recoveringDeliveries) {
    pendingDeliveryLoadRequested = true;
    return;
  }

  clearDatabaseRetry();
  recoveringDeliveries = true;
  pendingDeliveryLoadRequested = false;

  try {
    const persistedMessages =
      await messageStore.getPendingDeliveries('A', MAX_PENDING_MESSAGES);

    for (const message of persistedMessages) {
      const messageId = message.id;

      // Avoid duplicate entries already in memory.
      const alreadyQueued = pendingMessages.some((queuedPacket) => {
        try {
          return JSON.parse(queuedPacket).id === messageId;
        } catch (error) {
          return false;
        }
      });

      if (alreadyQueued || failedMessages.has(messageId)) {
        continue;
      }

      if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
        console.warn(
          'Node B queue is full; remaining persisted deliveries stay in PostgreSQL.'
        );
        break;
      }

      // Preserve the authenticated sender's identity and original text.
      const packet = JSON.stringify({
        type: 'chat',
        id: message.id,
        senderUserId: String(message.senderUserId),
        username: message.username,
        originNode: message.originNode,
        text: message.text
      });

      pendingMessages.push(packet);
      retryCounts.set(messageId, message.retryCount || 0);
    }

    console.log(
      `Loaded persisted deliveries. Pending count: ${pendingMessages.length}`
    );
  } catch (error) {
    console.error(
      'Could not load persisted deliveries:',
      error.message
    );
    scheduleDatabaseRetry();
  } finally {
    recoveringDeliveries = false;

    const shouldReload =
      pendingDeliveryLoadRequested &&
      pendingMessages.length < MAX_PENDING_MESSAGES;

    if (shouldReload) {
      pendingDeliveryLoadRequested = false;
      void loadPersistedDeliveries();
    }

    void flushPendingMessages();
  }
}

// Print a message when Node B starts.
console.log(`Server Node B running on ws://localhost:${PORT}`);

// Broadcast a JSON packet to Node B's browser clients.
function broadcastToClients(packet) {
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(packet));
    }
  });
}

function sendToPeer(packet) {
  let messageId;

  // Read the message ID from the packet.
  try {
    const parsedPacket = JSON.parse(packet);
    messageId = parsedPacket.id;
  } catch (error) {
    console.error('Cannot queue invalid message packet:', error.message);
    return false;
  }

  if (typeof messageId !== 'string' || messageId.length === 0) {
    console.error('Cannot queue a packet without a valid message ID.');
    return false;
  }

  // Prevent the same message ID from entering the queue twice.
  const alreadyQueued = pendingMessages.some((queuedPacket) => {
    try {
      return JSON.parse(queuedPacket).id === messageId;
    } catch (error) {
      return false;
    }
  });

  if (alreadyQueued) {
    console.log(`Duplicate queue entry prevented: ${messageId}`);
    return true;
  }

  // Do not enqueue a message that is already marked as failed.
  if (failedMessages.has(messageId)) {
    console.log(
      `Message ${messageId} is already failed and awaiting recovery.`
    );
    return true;
  }

  // Reject a new message if the pending queue is full.
  if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
    console.error(
      `Message queue full (${MAX_PENDING_MESSAGES}). ` +
      `Persisted delivery ${messageId} will be loaded when queue space is available.`
    );
    return false;
  }

  // Add the packet to the end of the queue.
  pendingMessages.push(packet);

  console.log(
    `Queued message. Pending count: ${pendingMessages.length}`
  );

  // Attempt delivery.
  flushPendingMessages();

  return true;
}

// Cancel the ACK timeout associated with a message.
function clearAckTimer(messageId) {
  const timer = ackTimers.get(messageId);

  if (timer) {
    clearTimeout(timer);
    ackTimers.delete(messageId);
  }
}


 // Retry a message when its ACK does not arrive in time.

function scheduleAckTimeout(messageId) {
  // Prevent multiple timers for the same message.
  clearAckTimer(messageId);

  const timer = setTimeout(async () => {
    ackTimers.delete(messageId);

    // The message may have been acknowledged while waiting.
    if (!awaitingAcks.has(messageId)) {
      return;
    }

    // Confirm that the message is still queued.
    const stillQueued = pendingMessages.some((queuedPacket) => {
      try {
        return JSON.parse(queuedPacket).id === messageId;
      } catch {
        return false;
      }
    });

    if (!stillQueued) {
      awaitingAcks.delete(messageId);
      retryCounts.delete(messageId);
      void flushPendingMessages();
      return;
    }

    const retries = (retryCounts.get(messageId) || 0) + 1;
    retryCounts.set(messageId, retries);

    // Persist failure after the configured retry limit.
    if (retries > MAX_RETRIES) {
      try {
        const recorded = await messageStore.markDeliveryFailed(
          messageId,
          'A',
          `Maximum delivery retries exceeded (${MAX_RETRIES}).`
        );

        if (!recorded) {
          const status = await messageStore.getDeliveryStatus(
            messageId,
            'A'
          );

          if (status === 'acknowledged') {
            const index = pendingMessages.findIndex((queuedPacket) => {
              try {
                return JSON.parse(queuedPacket).id === messageId;
              } catch {
                return false;
              }
            });

            if (index !== -1) {
              pendingMessages.splice(index, 1);
              void loadPersistedDeliveries();
            }

            failedMessages.delete(messageId);
            awaitingAcks.delete(messageId);
            retryCounts.delete(messageId);

            console.log(
              `Delivery ${messageId} was already acknowledged in PostgreSQL.`
            );

            void flushPendingMessages();
            return;
          }

          if (status === 'failed') {
            const index = pendingMessages.findIndex((queuedPacket) => {
              try {
                return JSON.parse(queuedPacket).id === messageId;
              } catch {
                return false;
              }
            });

            if (index !== -1) {
              const [failedPacket] = pendingMessages.splice(index, 1);
              failedMessages.set(messageId, failedPacket);
              void loadPersistedDeliveries();
            }

            awaitingAcks.delete(messageId);
            retryCounts.delete(messageId);

            console.warn(
              `Delivery ${messageId} is already marked failed in PostgreSQL.`
            );
            console.warn(
              `Messages awaiting recovery: ${failedMessages.size}`
            );

            void flushPendingMessages();
            return;
          }

          console.error(
            `Could not confirm failed delivery status for ${messageId}. ` +
            `Database status: ${status ?? 'missing'}.`
          );

          awaitingAcks.delete(messageId);

failedStatusRetryQueue.set(
  messageId,
  `Maximum delivery retries exceeded (${MAX_RETRIES}).`
);

scheduleFailedStatusRetry();
return;
        }
      } catch (error) {
  console.error(
    `Could not persist failed delivery ${messageId}:`,
    error.message
  );

  // Preserve the packet and prevent further delivery attempts.
  awaitingAcks.delete(messageId);

  failedStatusRetryQueue.set(
    messageId,
    `Maximum delivery retries exceeded (${MAX_RETRIES}).`
  );

  scheduleFailedStatusRetry();
  return;
}

      // An ACK may have been processed while failure was recorded.
      if (!awaitingAcks.has(messageId)) {
        void flushPendingMessages();
        return;
      }

      const index = pendingMessages.findIndex((queuedPacket) => {
        try {
          return JSON.parse(queuedPacket).id === messageId;
        } catch {
          return false;
        }
      });

      if (index !== -1) {
        const [failedPacket] = pendingMessages.splice(index, 1);
        failedMessages.set(messageId, failedPacket);
        void loadPersistedDeliveries();
      }

      awaitingAcks.delete(messageId);
      retryCounts.delete(messageId);

      console.error(
        `Message ${messageId} failed after ${MAX_RETRIES} retries.`
      );
      console.error(
        `Messages awaiting recovery: ${failedMessages.size}`
      );

      void flushPendingMessages();
      return;
    }

    console.log(
      `ACK timeout for ${messageId}. Retry attempt ${retries}.`
    );

    // Resend the same packet with the same message ID.
    awaitingAcks.delete(messageId);
    void flushPendingMessages();
  }, ACK_TIMEOUT_MS);

  ackTimers.set(messageId, timer);
}

// Remove a message only after its ACK is persistently recorded.
async function acknowledgeMessage(messageId) {
  try {
    const recorded = await messageStore.acknowledgeDelivery(messageId, 'A');

    if (!recorded) {
      console.warn(`No matching database delivery found for ${messageId}. Keeping in memory.`);
      return;
    }
  } catch (error) {
  console.error(
    `Could not persist ACK for ${messageId}:`,
    error.message
  );

  // The peer has acknowledged the message.
  // Stop its delivery timeout while persistence is retried.
  clearAckTimer(messageId);
  awaitingAcks.delete(messageId);

  // Preserve the ACK until PostgreSQL confirms it.
  ackRetryQueue.set(messageId, true);

  broadcastToClients({
    type: 'system',
    text: `Delivery for message ${messageId} acknowledged in memory but not yet persisted. Retrying...`
  });

  scheduleAckRetry();
  return;
}

  // Only commit state changes after persistence succeeds
  clearAckTimer(messageId);
  awaitingAcks.delete(messageId);
  retryCounts.delete(messageId);
  failedStatusRetryQueue.delete(messageId);

  const wasPreviouslyFailed = failedMessages.delete(messageId);

  const index = pendingMessages.findIndex((queuedPacket) => {
    try {
      return JSON.parse(queuedPacket).id === messageId;
    } catch {
      return false;
    }
  });

  if (index !== -1) {
    pendingMessages.splice(index, 1);
    void loadPersistedDeliveries();
    console.log(`Message acknowledged: ${messageId}`);
  } else if (wasPreviouslyFailed) {
    console.log(`Late ACK recovered failed message: ${messageId}`);
  } else {
    console.log(`ACK received for message: ${messageId}`);
  }

  flushPendingMessages();
}

function sendAcknowledgement(peer, messageId) {

  if (peer && peer.readyState === WebSocket.OPEN) {
    peer.send(JSON.stringify({
      type: 'ack',
      id: messageId
    }));
  }
}

// Send the oldest queued message and wait for its ACK.

async function flushPendingMessages() {
  if (queueFlushOwner || recoveringDeliveries) return;

  if (
    !peerSocket ||
    peerSocket.readyState !== WebSocket.OPEN ||
    !peerAuthenticated
  ) {
    return;
  }

  if (pendingMessages.length === 0) return;

  const packet = pendingMessages[0];
  let parsedPacket;

  try {
    parsedPacket = JSON.parse(packet);
  } catch (error) {
    console.error('Invalid queued message:', error.message);
    return;
  }

  const messageId = parsedPacket.id;

  // Do not resend a message while its ACK is awaiting persistence.
if (
  awaitingAcks.has(messageId) ||
  ackRetryQueue.has(messageId) ||
  failedStatusRetryQueue.has(messageId)
) {
  return;
}

  const currentPeer = peerSocket;
  const retries = retryCounts.get(messageId) || 0;
  const flushOwner = { peer: currentPeer };

  // Lock the queue before awaiting PostgreSQL.
  // This prevents overlapping flush calls from sending the same message.
  queueFlushOwner = flushOwner;

  // Persist the attempt before sending to the peer.
  try {
    const recorded = await messageStore.markDeliveryAttempt(
      messageId,
      'A',
      retries
    );

    if (queueFlushOwner !== flushOwner) {
      return;
    }

if (!recorded) {
  const status = await messageStore.getDeliveryStatus(
    messageId,
    'A'
  );

  if (queueFlushOwner !== flushOwner) {
    return;
  }

  if (status === 'acknowledged') {
    const index = pendingMessages.indexOf(packet);

    if (index !== -1) {
      pendingMessages.splice(index, 1);
      void loadPersistedDeliveries();
    }

    const timer = ackTimers.get(messageId);

    if (timer) {
      clearTimeout(timer);
      ackTimers.delete(messageId);
    }

    failedMessages.delete(messageId);
    awaitingAcks.delete(messageId);
    retryCounts.delete(messageId);

    console.log(
      `Message ${messageId} was already acknowledged in PostgreSQL.`
    );

    releaseQueueFlush(flushOwner);
    void flushPendingMessages();
    return;
  }

  if (status === 'failed') {
    const index = pendingMessages.indexOf(packet);

    if (index !== -1) {
      const [failedPacket] = pendingMessages.splice(index, 1);
      failedMessages.set(messageId, failedPacket);
      void loadPersistedDeliveries();
    }

    const timer = ackTimers.get(messageId);

    if (timer) {
      clearTimeout(timer);
      ackTimers.delete(messageId);
    }

    awaitingAcks.delete(messageId);

    console.warn(
      `Message ${messageId} is marked failed in PostgreSQL.`
    );

    releaseQueueFlush(flushOwner);
    void flushPendingMessages();
    return;
  }

  console.warn(
    `Could not confirm delivery state for ${messageId}. ` +
    `Database status: ${status ?? 'missing'}.`
  );

  releaseQueueFlush(flushOwner);
  scheduleDatabaseRetry();
  return;
}

  } catch (error) {
    if (queueFlushOwner !== flushOwner) {
      return;
    }

    releaseQueueFlush(flushOwner);

    console.error(
      `Could not record delivery attempt for ${messageId}:`,
      error.message
    );
    scheduleDatabaseRetry();
    return;
  }

  // Recheck the connection and queue after the database operation.
  if (
    queueFlushOwner !== flushOwner ||
    peerSocket !== currentPeer ||
    currentPeer.readyState !== WebSocket.OPEN ||
    pendingMessages[0] !== packet
  ) {
    releaseQueueFlush(flushOwner);
    if (peerSocket && peerAuthenticated) {
      void flushPendingMessages();
    }
    return;
  }

  awaitingAcks.add(messageId);

  console.log(`Sending message ${messageId} to peer`);

  try {
    currentPeer.send(packet, (error) => {
      releaseQueueFlush(flushOwner);

      if (error) {
        console.error(
          'Message send failed; keeping it queued:',
          error.message
        );

        scheduleAckTimeout(messageId);
        return;
      }

      const stillQueued = pendingMessages.some((queuedPacket) => {
        try {
          return JSON.parse(queuedPacket).id === messageId;
        } catch (parseError) {
          return false;
        }
      });

      if (
        stillQueued &&
        awaitingAcks.has(messageId) &&
        peerSocket === currentPeer
      ) {
        console.log(`Waiting for ACK for message ${messageId}`);
        scheduleAckTimeout(messageId);
      }

      if (!stillQueued) {
        flushPendingMessages();
      }
    });
  } catch (error) {
    releaseQueueFlush(flushOwner);
    console.error(
      'Message send failed; keeping it queued:',
      error.message
    );
    scheduleAckTimeout(messageId);
  }
}

server.on('connection', (socket, request) => {
  const requestUrl = new URL(
    request.url,
    `http://localhost:${PORT}`
  );

  const isPeerCandidate =
    requestUrl.searchParams.get('peer') === 'A';

  let authenticated = false;
  let authTimeout = null;

  function clearAuthenticationTimeout() {
    if (authTimeout) {
      clearTimeout(authTimeout);
      authTimeout = null;
    }
  }

  // Handle server-to-server connections.
  // The URL parameter identifies a peer candidate, but does NOT
  // authenticate it. The shared secret must also be verified.
  if (isPeerCandidate) {
    let peerAuthenticatedLocally = false;

    socket.on('message', async(data) => {
      let packet;

      try {
        packet = JSON.parse(data.toString());
      } catch (error) {
        console.error(
          'Invalid JSON received on peer connection:',
          error.message
        );

        socket.close(1008, 'Invalid peer message');
        return;
      }

      // Require the first packet to authenticate Node A.
      if (!peerAuthenticatedLocally) {
        if (
          packet.type !== 'peer_auth' ||
          !isValidPeerSecret(packet.secret)
        ) {
          console.error(
            'Rejected unauthenticated or invalid peer connection.'
          );

          socket.close(1008, 'Peer authentication failed');
          return;
        }

        // Only one active peer connection is needed.
        if (
          peerSocket &&
          peerSocket !== socket &&
          peerSocket.readyState === WebSocket.OPEN
        ) {
          invalidateQueueFlush(peerSocket);
          console.warn(
            'Replacing the existing Node A peer connection.'
          );

          peerSocket.close(1000, 'Peer connection replaced');
        }

        peerAuthenticatedLocally = true;
        peerAuthenticated = true;
        peerSocket = socket;

        socket.send(JSON.stringify({
          type: 'peer_auth_success'
        }));

        console.log(
          'Node A successfully authenticated with Node B.'
        );

        await loadPersistedDeliveries();
        return;
      }

      // Ignore messages from a socket that is no longer the active peer.
      if (
        peerSocket !== socket ||
        !peerAuthenticated
      ) {
        return;
      }


      // Process acknowledgements from Node A.
      if (
        packet.type === 'ack' &&
        typeof packet.id === 'string'
      ) {
        void acknowledgeMessage(packet.id);
        return;
      }

      // Persist incoming messages before acknowledging the sending node.
      if (
        packet.type === 'chat' &&
        typeof packet.id === 'string' &&
        typeof packet.text === 'string' &&
        Number.isInteger(Number(packet.senderUserId)) &&
        typeof packet.username === 'string' &&
        (packet.originNode === 'A' || packet.originNode === 'B')
      ) {
        try {
          const firstReceipt = await messageStore.persistIncomingMessage(
            {
              id: packet.id,
              senderUserId: Number(packet.senderUserId),
              username: packet.username,
              originNode: packet.originNode,
              text: packet.text
            },
            'B'
          );

          // Record the message in the in-memory deduplication cache.
          rememberMessageId(packet.id);

          if (!firstReceipt) {
            console.log(
              'Duplicate message already persisted; acknowledging:',
              packet.id
            );

            sendAcknowledgement(socket, packet.id);
            return;
          }

          // Display only messages that have been persisted successfully.
          broadcastToClients({
  type: 'chat',
  id: packet.id,
  senderUserId: packet.senderUserId,
  username: packet.username,
  originNode: packet.originNode,
  text: packet.text
});

          // ACK only after PostgreSQL confirms persistence.
          sendAcknowledgement(socket, packet.id);
        } catch (error) {
          console.error(
            `Could not persist incoming message ${packet.id}:`,
            error.message
          );

          // Deliberately do not ACK. The sender can retry delivery.
        }
      }
    });

    socket.on('close', () => {
      if (peerSocket === socket) {
        invalidateQueueFlush(socket);
        peerSocket = null;
        peerAuthenticated = false;

        for (const timer of ackTimers.values()) {
          clearTimeout(timer);
        }

        ackTimers.clear();
        awaitingAcks.clear();
        retryCounts.clear();
      }

      console.log('Connection from Server Node A closed.');
    });

    socket.on('error', (error) => {
      console.error(
        'Node A connection error:',
        error.message
      );
    });

    return;
  }

  // All non-peer connections are browser clients.
  // They must authenticate with a valid JWT before joining clients.
  authTimeout = setTimeout(() => {
    if (!authenticated) {
      socket.close(1008, 'Authentication timeout');
    }
  }, 5000);

  socket.on('message', async(data) => {
    const rawMessage = data.toString();

    if (!authenticated) {
      let packet;

      try {
        packet = JSON.parse(rawMessage);
      } catch (error) {
        socket.close(1008, 'Authentication required');
        return;
      }

      if (
        packet.type !== 'auth' ||
        typeof packet.token !== 'string'
      ) {
        socket.close(1008, 'Authentication required');
        return;
      }

      const user = verifyAuthToken(packet.token);

      if (!user) {
        clearAuthenticationTimeout();

        console.warn(
          'Rejected browser client with an invalid JWT.'
        );

        socket.close(1008, 'Invalid authentication token');
        return;
      }

      authenticated = true;
      socket.userId = user.id;
      socket.username = user.username;

      clearAuthenticationTimeout();
      clients.add(socket);

      socket.send(JSON.stringify({
        type: 'auth_success',
        user: {
          id: user.id,
          username: user.username
        }
      }));

      console.log(
        `Authenticated client connected to Node B. Total: ${clients.size}`
      );

      return;
    }



   // Handle explicit recovery of failed messages.
const recoveryCommand = rawMessage.trim().toLowerCase();

if (recoveryCommand === '/recover') {
  if (recoveringDeliveries) {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(
        'System: Message recovery is already in progress. Please try again shortly.'
      );
    }
    return;
  }

  recoveringDeliveries = true;

  let recoveredCount = 0;
  let acknowledgedCount = 0;
  let queueFull = false;

  try {
    for (const [messageId, packet] of failedMessages) {
      try {
        // Check whether this message is already in the pending queue.
        const alreadyQueued = pendingMessages.some((queuedPacket) => {
          try {
            return JSON.parse(queuedPacket).id === messageId;
          } catch {
            return false;
          }
        });

        // Read the current authoritative database status.
        let status = await messageStore.getDeliveryStatus(
          messageId,
          'A'
        );

        // An acknowledged message must not be retried.
        if (status === 'acknowledged') {
          failedMessages.delete(messageId);
          clearAckTimer(messageId);
          awaitingAcks.delete(messageId);
          retryCounts.delete(messageId);

          acknowledgedCount++;

          console.log(
            `Removed already-acknowledged message ${messageId} from failed recovery.`
          );

          continue;
        }

        if (status === null) {
          console.warn(
            `Cannot recover ${messageId}: no delivery record exists.`
          );
          continue;
        }

        // Do not exceed the in-memory queue capacity.
        if (
          !alreadyQueued &&
          pendingMessages.length >= MAX_PENDING_MESSAGES
        ) {
          queueFull = true;
          continue;
        }

        // Change failed database deliveries back to pending.
        if (status === 'failed') {
          const recovered = await messageStore.recoverFailedDelivery(
            messageId,
            'A'
          );

          if (recovered) {
            status = 'pending';
          } else {
            // Recheck in case the database state changed concurrently.
            status = await messageStore.getDeliveryStatus(
              messageId,
              'A'
            );
          }
        }

        // Another process may have acknowledged the message.
        if (status === 'acknowledged') {
          failedMessages.delete(messageId);
          clearAckTimer(messageId);
          awaitingAcks.delete(messageId);
          retryCounts.delete(messageId);

          acknowledgedCount++;
          continue;
        }

        // Only pending messages can return to the delivery queue.
        if (status !== 'pending') {
          console.warn(
            `Message ${messageId} remains awaiting recovery. Database status: ${status ?? 'missing'}.`
          );
          continue;
        }

        // Restore the message to memory if it is not already queued.
        if (!alreadyQueued) {
          // Validate the stored packet before adding it to the queue.
          const parsed = JSON.parse(packet);

          if (parsed.id !== messageId) {
            console.error(
              `Recovery skipped: packet ID does not match ${messageId}.`
            );
            continue;
          }

          pendingMessages.push(packet);

          // Broadcast the recovered message to Node B browser clients.
          broadcastToClients({
            type: 'chat',
            id: parsed.id,
            senderUserId: parsed.senderUserId,
            username: parsed.username,
            originNode: parsed.originNode,
            text: parsed.text
          });
        }

        // Reconcile in-memory delivery state immediately.
        failedMessages.delete(messageId);
        clearAckTimer(messageId);
        awaitingAcks.delete(messageId);
        retryCounts.set(messageId, 0);

        recoveredCount++;

        console.log(
          `Message ${messageId} restored to the pending delivery queue.`
        );
      } catch (messageError) {
        // Keep this message in failedMessages for a later recovery attempt.
        console.error(
          `Could not recover message ${messageId}:`,
          messageError.message
        );
      }
    }

    if (socket.readyState === WebSocket.OPEN) {
      if (recoveredCount > 0) {
        socket.send(
          `System: Recovered ${recoveredCount} failed message(s).`
        );
      } else if (acknowledgedCount > 0) {
        socket.send(
          'System: No failed messages needed recovery. Already-acknowledged messages were cleaned up.'
        );
      } else {
        socket.send(
          'System: No failed messages were available for recovery.'
        );
      }

      if (queueFull) {
        socket.send(
          'System: The pending queue is full. Some failed messages remain awaiting recovery.'
        );
      }
    }

  } catch (error) {
    console.error(
      'Node B failed-message recovery error:',
      error.message
    );

    if (socket.readyState === WebSocket.OPEN) {
      socket.send(
        'System: Message recovery failed. Please try again later.'
      );
    }
  } finally {
  recoveringDeliveries = false;

  // Process a database refill request deferred during recovery.
  if (
    pendingDeliveryLoadRequested &&
    pendingMessages.length < MAX_PENDING_MESSAGES
  ) {
    pendingDeliveryLoadRequested = false;
    void loadPersistedDeliveries();
  }

  // Resume delivery after recovery has released its lock.
  if (recoveredCount > 0) {
    void flushPendingMessages();
  }
}
  return;
}
    // Only authenticated clients may send chat messages.
    const userMessage = rawMessage.trim();

    if (!userMessage) {
      return;
    }

    // Build a message using the identity verified by the JWT.
    const messagePacket = {
      type: 'chat',
      id: crypto.randomUUID(),
      senderUserId: Number(socket.userId),
      username: socket.username,
      originNode: 'B',
      text: userMessage
    };

    // Save the message and delivery record before broadcasting.
    try {
      await messageStore.persistOutgoingMessage(
        messagePacket,
        'A'
      );
    } catch (error) {
      console.error(
        `Could not persist outgoing message ${messagePacket.id}:`,
        error.message
      );

      if (socket.readyState === WebSocket.OPEN) {
        socket.send(
          'System: Message could not be saved. Please try again later.'
        );
      }

      return;
    }

    // Track the successfully persisted message in memory.
    rememberMessageId(messagePacket.id);

    // Broadcast the persisted message as a structured chat packet.
broadcastToClients({
  type: 'chat',
  id: messagePacket.id,
  senderUserId: messagePacket.senderUserId,
  username: messagePacket.username,
  originNode: messagePacket.originNode,
  text: messagePacket.text
});
    // Preserve the existing queue, retry, and ACK mechanisms.
    const queued = sendToPeer(JSON.stringify(messagePacket));

    if (!queued && socket.readyState === WebSocket.OPEN) {
      socket.send(
        'System: Your message was saved and will be delivered when the pending queue has space.'
      );
    }
  });
  socket.on('close', () => {
    clearAuthenticationTimeout();

    if (authenticated) {
      clients.delete(socket);

      console.log(
        `Client disconnected from Node B. Total: ${clients.size}`
      );
    }
  });

  socket.on('error', (error) => {
    console.error(
      'Node B client connection error:',
      error.message
    );
  });
});
