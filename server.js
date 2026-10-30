
require('dotenv').config();

const WebSocket = require('ws');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const messageStore = require('./messageStore');

// The API and Node A must use the same JWT secret.
if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 32) {
  throw new Error(
    'JWT_SECRET is missing or too short. Check your .env file.'
  );
}

/**
 * Verify a login token and return the trusted user identity.
 * Return null if the token is invalid or expired.
 */
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
    // Invalid, expired or incorrectly signed token.
    return null;
  }
}

if (
  !process.env.INTERNAL_PEER_SECRET ||
  process.env.INTERNAL_PEER_SECRET.length < 32
) {
  throw new Error(
    'INTERNAL_PEER_SECRET is missing or too short. Check your .env file.'
  );
}

// Render supplies PORT in production.
// Local development continues to use NODE_A_PORT or 8090.
const PORT =
  Number(process.env.PORT || process.env.NODE_A_PORT) || 8090;

// The peer URL remains configurable.
// Locally, Node A connects to Node B on port 8081.
const NODE_B_URL =
  process.env.NODE_B_URL ||
  'ws://localhost:8081/?peer=A';

// Create the WebSocket server for browser clients.
const server = new WebSocket.Server({ port: PORT });

// Track browser clients connected directly to Node A.
const clients = new Set();

// Track message IDs to prevent duplicate processing.
const seenMessageIds = new Set();

// Keep the deduplication cache within a reasonable size.
const MAX_SEEN_MESSAGE_IDS = 5000;

// Remember a message ID if it has not already been processed.
function rememberMessageId(messageId) {
  if (seenMessageIds.has(messageId)) {
    return false;
  }

  seenMessageIds.add(messageId);

  // Remove the oldest ID when the cache reaches its limit.
  if (seenMessageIds.size > MAX_SEEN_MESSAGE_IDS) {
    const oldestId = seenMessageIds.values().next().value;
    seenMessageIds.delete(oldestId);
  }

  return true;
}

// Store the connection to Node B.
let peerSocket = null;

//Track whether Node B has authenticated
let peerAuthenticated = false;

// Stores the scheduled reconnection timer.
let reconnectTimer = null;

// Tracks how many consecutive reconnection attempts have failed.
let reconnectAttempts = 0;

// Starts with a 2-second delay.
const INITIAL_RECONNECT_DELAY = 2000;

// Never wait longer than 30 seconds between attempts.
const MAX_RECONNECT_DELAY = 30000;

// Tracks whether Node B is currently considered connected.
let peerConnected = false;

// Stores messages waiting to be delivered to Node B.
const pendingMessages = [];

// Store IDs of messages that have been sent but not yet acknowledged.
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

// Prevent multiple queued messages from being flushed simultaneously.
let flushingQueue = false;

// Prevent overlapping PostgreSQL recovery operations.
let recoveringDeliveries = false;

// Retry database-backed delivery work without duplicate timers.
let databaseRetryTimer = null;

/**
 * Explicitly recover failed messages using their persisted delivery state.
 * Original IDs are preserved, and failed entries remain in memory if
 * PostgreSQL cannot confirm their recovery.
 */
async function recoverFailedMessages() {
  const recoveredPackets = [];

  for (const [messageId, packet] of failedMessages) {
    if (pendingMessages.length + recoveredPackets.length >=
        MAX_PENDING_MESSAGES) {
      console.warn(
        'Cannot recover more failed messages: pending queue is full.'
      );
      break;
    }

    // Ensure this packet is not already waiting in the queue.
    const alreadyQueued = pendingMessages.some((queuedPacket) => {
      try {
        return JSON.parse(queuedPacket).id === messageId;
      } catch (error) {
        return false;
      }
    });

    if (alreadyQueued) {
      failedMessages.delete(messageId);
      continue;
    }

    try {
      const recovered = await messageStore.recoverFailedDelivery(
        messageId,
        'B'
      );


           if (!recovered) {
        const status = await messageStore.getDeliveryStatus(
          messageId,
          'B'
        );

        if (status === 'acknowledged') {
          failedMessages.delete(messageId);
          retryCounts.delete(messageId);

          console.log(
            `Message ${messageId} was already acknowledged.`
          );
        } else if (status === 'pending') {
          // The database is already ready for delivery.
          recoveredPackets.push(packet);
          failedMessages.delete(messageId);
          retryCounts.set(messageId, 0);
        } else {
          console.warn(
            `Message ${messageId} was not recovered. ` +
            `Database status: ${status ?? 'missing'}.`
          );
        }

        continue;
      }

      recoveredPackets.push(packet);
      failedMessages.delete(messageId);
      retryCounts.set(messageId, 0);
    } catch (error) {
      console.error(
        `Could not recover message ${messageId}:`,
        error.message
      );
      // Leave the failed packet in memory for another attempt.
    }
  }

    if (recoveredPackets.length > 0) {
    // Preserve failed messages' order ahead of newer queued messages.
    pendingMessages.unshift(...recoveredPackets);

    console.log(
      `Recovered ${recoveredPackets.length} failed message(s).`
    );

    console.log(
      `Messages still awaiting recovery: ${failedMessages.size}`
    );
  }

  return recoveredPackets.length;
}

// Display a startup message.
console.log(`Server Node A running on ws://localhost:${PORT}`);

// Broadcast a message to browser clients connected to Node A.
function broadcastToClients(message) {
  // Convert structured packets into JSON before sending.
  const serializedMessage =
    typeof message === 'string'
      ? message
      : JSON.stringify(message);

  clients.forEach((client) => {
    // Send only if the browser connection is open.
    if (client.readyState === WebSocket.OPEN) {
      client.send(serializedMessage);
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
      `Message queue full (${MAX_PENDING_MESSAGES}). Message was not queued.`
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
  clearAckTimer(messageId);

  const timer = setTimeout(async () => {
    ackTimers.delete(messageId);

    if (!awaitingAcks.has(messageId)) {
      return;
    }

    const findQueuedIndex = () =>
      pendingMessages.findIndex((queuedPacket) => {
        try {
          return JSON.parse(queuedPacket).id === messageId;
        } catch (error) {
          return false;
        }
      });

    if (findQueuedIndex() === -1) {
      awaitingAcks.delete(messageId);
      retryCounts.delete(messageId);
      return;
    }

    const retries = (retryCounts.get(messageId) || 0) + 1;
    retryCounts.set(messageId, retries);

    if (retries > MAX_RETRIES) {
      try {
        const recorded = await messageStore.markDeliveryFailed(
          messageId,
          'B',
          `Maximum delivery retries exceeded (${MAX_RETRIES}).`
        );

        // An ACK may have arrived during the database operation.
        if (!awaitingAcks.has(messageId)) {
          return;
        }

        if (!recorded) {
          const status = await messageStore.getDeliveryStatus(
            messageId,
            'B'
          );

          // Recheck after the second asynchronous database operation.
          if (!awaitingAcks.has(messageId)) {
            return;
          }

          if (status === 'acknowledged') {
            const index = findQueuedIndex();

            if (index !== -1) {
              pendingMessages.splice(index, 1);
            }

            failedMessages.delete(messageId);
            awaitingAcks.delete(messageId);
            retryCounts.delete(messageId);

            console.log(
              `Delivery ${messageId} was already acknowledged in PostgreSQL.`
            );

            flushingQueue = false;
            void flushPendingMessages();
            return;
          }

          if (status === 'failed') {
            const index = findQueuedIndex();

            if (index !== -1) {
              const [failedPacket] = pendingMessages.splice(index, 1);
              failedMessages.set(messageId, failedPacket);
            }

            awaitingAcks.delete(messageId);
            retryCounts.delete(messageId);

            console.warn(
              `Delivery ${messageId} is already marked failed in PostgreSQL.`
            );
            console.warn(
              `Messages awaiting recovery: ${failedMessages.size}`
            );

            flushingQueue = false;
            void flushPendingMessages();
            return;
          }

          console.warn(
            `Could not confirm failed delivery state for ${messageId}. ` +
            `Database status: ${status ?? 'missing'}.`
          );

          // Keep the packet queued and retry later.
          awaitingAcks.delete(messageId);
          flushingQueue = false;
          scheduleDatabaseRetry();
          return;
        }
      } catch (error) {
        console.error(
          `Could not persist failed delivery for ${messageId}:`,
          error.message
        );

        // Preserve the message; release the queue lock before recovery.
        awaitingAcks.delete(messageId);
        flushingQueue = false;
        scheduleDatabaseRetry();
        return;
      }

      // Do not change memory state if an ACK arrived during persistence.
      if (!awaitingAcks.has(messageId)) {
        flushingQueue = false;
        void flushPendingMessages();
        return;
      }

      const index = findQueuedIndex();

      if (index !== -1) {
        const [failedPacket] = pendingMessages.splice(index, 1);
        failedMessages.set(messageId, failedPacket);
      }

      awaitingAcks.delete(messageId);
      retryCounts.delete(messageId);

      console.error(
        `Message ${messageId} failed after ${MAX_RETRIES} retries.`
      );
      console.error(
        `Messages awaiting recovery: ${failedMessages.size}`
      );

      flushingQueue = false;
      void flushPendingMessages();
      return;
    }

    console.log(
      `ACK timeout for ${messageId}. Retry attempt ${retries}.`
    );

    awaitingAcks.delete(messageId);
    flushingQueue = false;
    void flushPendingMessages();
  }, ACK_TIMEOUT_MS);

  ackTimers.set(messageId, timer);
}

// Remove a message only after its ACK arrives.
async function acknowledgeMessage(messageId) {
  try {
    // Persist the acknowledgement before changing in-memory state.
    const recorded = await messageStore.acknowledgeDelivery(
      messageId,
      'B'
    );

            if (!recorded) {
      console.error(
        `Could not confirm persisted ACK for message ${messageId}. ` +
        'The message remains queued.'
      );

      // Preserve the queue and retry tracking.
      return;
    }
  } catch (error) {
    console.error(
      `Could not persist ACK for message ${messageId}:`,
      error.message
    );

    // Preserve the queue and retry tracking if PostgreSQL is unavailable.
    return;
  }

  const index = pendingMessages.findIndex((queuedPacket) => {
    try {
      return JSON.parse(queuedPacket).id === messageId;
    } catch (error) {
      return false;
    }
  });

  clearAckTimer(messageId);
  awaitingAcks.delete(messageId);
  retryCounts.delete(messageId);

  const wasPreviouslyFailed = failedMessages.delete(messageId);

  if (index !== -1) {
    pendingMessages.splice(index, 1);

    console.log(`Message acknowledged: ${messageId}`);
    console.log(
      `Remaining queued messages: ${pendingMessages.length}`
    );
  } else if (wasPreviouslyFailed) {
    console.log(
      `Late ACK received; recovered failed message: ${messageId}`
    );
  } else {
    console.log(`ACK received for message: ${messageId}`);
  }

  // Continue with the next FIFO message.
  flushPendingMessages();
}

// Send an acknowledgement to the peer server.
function sendAcknowledgement(peer, messageId) {
  if (peer && peer.readyState === WebSocket.OPEN) {
    peer.send(JSON.stringify({
      type: 'ack',
      id: messageId
    }));
  }
}


function scheduleDatabaseRetry() {
  if (databaseRetryTimer) {
    return;
  }

  databaseRetryTimer = setTimeout(() => {
    databaseRetryTimer = null;
    void loadPersistedDeliveries();
  }, ACK_TIMEOUT_MS);
}

// Restore pending deliveries destined for Node B.
async function loadPersistedDeliveries() {
  if (recoveringDeliveries) {
    return;
  }

  recoveringDeliveries = true;

  try {
    const persistedMessages =
      await messageStore.getPendingDeliveries(
        'B',
        MAX_PENDING_MESSAGES
      );

    for (const message of persistedMessages) {
      const messageId = message.id;

      // Do not duplicate packets already waiting in memory.
      const alreadyQueued = pendingMessages.some((queuedPacket) => {
        try {
          return JSON.parse(queuedPacket).id === messageId;
        } catch (error) {
          return false;
        }
      });

      // Failed messages require explicit recovery.
      if (alreadyQueued || failedMessages.has(messageId)) {
        continue;
      }

      if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
        console.warn(
          'Node A queue is full; remaining persisted deliveries ' +
          'will stay in PostgreSQL.'
        );
        break;
      }

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
      `Node A restored persisted deliveries. ` +
      `Pending count: ${pendingMessages.length}`
    );
  } catch (error) {
    console.error(
      'Node A could not load persisted deliveries:',
      error.message
    );

    scheduleDatabaseRetry();
  } finally {
    recoveringDeliveries = false;
  }

  void flushPendingMessages();
}


// Send the oldest queued message and wait for its ACK.
async function flushPendingMessages() {
  if (
  flushingQueue ||
  recoveringDeliveries ||
  pendingMessages.length === 0
) {
  return;
}

  if (
    !peerSocket ||
    peerSocket.readyState !== WebSocket.OPEN ||
    !peerAuthenticated
  ) {
    return;
  }

  const packet = pendingMessages[0];
  let messageId;

  try {
    messageId = JSON.parse(packet).id;
  } catch (error) {
    console.error('Invalid queued message:', error.message);
    return;
  }

  if (awaitingAcks.has(messageId)) {
    return;
  }

  const currentPeer = peerSocket;
  const retries = retryCounts.get(messageId) || 0;

  // Lock the queue while the database records this attempt.
  flushingQueue = true;

  try {

const recorded = await messageStore.markDeliveryAttempt(
  messageId,
  'B',
  retries
);

if (!recorded) {
  const status = await messageStore.getDeliveryStatus(
    messageId,
    'B'
  );

  if (status === 'acknowledged') {
    const index = pendingMessages.indexOf(packet);

    if (index !== -1) {
      pendingMessages.splice(index, 1);
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

    flushingQueue = false;
    void flushPendingMessages();
    return;
  }

  if (status === 'failed') {
    const index = pendingMessages.indexOf(packet);

    if (index !== -1) {
      const [failedPacket] = pendingMessages.splice(index, 1);
      failedMessages.set(messageId, failedPacket);
    }

    awaitingAcks.delete(messageId);

    console.warn(
      `Message ${messageId} is marked failed in PostgreSQL.`
    );

    flushingQueue = false;
    void flushPendingMessages();
    return;
  }

  console.warn(
    `Could not confirm delivery state for ${messageId}. ` +
    `Database status: ${status ?? 'missing'}.`
  );

  flushingQueue = false;
  scheduleDatabaseRetry();
  return;
}
} catch (error) {
  console.error(
    `Database error recording delivery attempt for ${messageId}:`,
    error.message
  );

  flushingQueue = false;
  scheduleDatabaseRetry();
  return;
  }

  // Recheck the connection and queue after awaiting PostgreSQL.
  if (
    peerSocket !== currentPeer ||
    currentPeer.readyState !== WebSocket.OPEN ||
    !peerAuthenticated ||
    pendingMessages[0] !== packet
  ) {
    flushingQueue = false;
    return;
  }

  awaitingAcks.add(messageId);

  console.log(`Sending message ${messageId} to peer`);

  currentPeer.send(packet, (error) => {
    flushingQueue = false;

    if (error) {
      console.error(
        'Message send failed; keeping it queued:',
        error.message
      );

      scheduleAckTimeout(messageId);
      return;
    }

    // The ACK could arrive before this callback executes.
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
    } else {
      // Continue only if the previous message is no longer blocking
      // the queue and the current connection remains authenticated.
      void flushPendingMessages();
    }
  });
}

function scheduleReconnect() {
  // Do not create multiple reconnection timers.
  if (reconnectTimer !== null) {
    return;
  }

  // Increase the failure count.
  reconnectAttempts++;

  // Double the delay after each failure, up to 30 seconds.
  const delay = Math.min(
    INITIAL_RECONNECT_DELAY * (2 ** (reconnectAttempts - 1)),
    MAX_RECONNECT_DELAY
  );

  // Only log every few attempts to reduce console noise.
  if (
    reconnectAttempts === 1 ||
    reconnectAttempts % 5 === 0
  ) {
    console.log(
      `Node B unavailable. Retry attempt ${reconnectAttempts} in ${delay / 1000} seconds.`
    );
  }

  // Schedule the next connection attempt.
  reconnectTimer = setTimeout(() => {
    // Clear the timer reference.
    reconnectTimer = null;

    // Attempt to connect again.
    connectToNodeB();
  }, delay);
}

// Establish a WebSocket connection to Node B.
function connectToNodeB() {
  // Avoid creating a second connection while one is connecting or open.
  if (
    peerSocket &&
    (
      peerSocket.readyState === WebSocket.CONNECTING ||
      peerSocket.readyState === WebSocket.OPEN
    )
  ) {
    return;
  }

  console.log('Attempting to connect to Node B...');

  // Create a new outgoing WebSocket connection.
  const peer = new WebSocket(NODE_B_URL);

  // Save this connection.
  peerSocket = peer;


peer.on('open', () => {
  if (peerSocket !== peer) {
    return;
  }

  // Node B must authenticate this server before chat traffic starts.
  peerAuthenticated = false;

  peer.send(JSON.stringify({
    type: 'peer_auth',
    secret: process.env.INTERNAL_PEER_SECRET
  }));

  console.log('Sent peer authentication to Node B.');
});


peer.on('message', async (data) => {
  try {
    const packet = JSON.parse(data.toString());

    // Wait for Node B to confirm the internal authentication handshake.
    if (!peerAuthenticated) {
      if (packet.type !== 'peer_auth_success') {
        console.error('Node B did not confirm peer authentication.');
        peer.close(1008, 'Peer authentication failed');
        return;
      }

      peerAuthenticated = true;
      peerConnected = true;
      reconnectAttempts = 0;

      console.log('Successfully authenticated with Node B.');

      void loadPersistedDeliveries();
      return;
    }

    // Handle an acknowledgement from Node B.
    if (packet.type === 'ack' && typeof packet.id === 'string') {
      void acknowledgeMessage(packet.id);
      return;
    }

    // Persist incoming messages from Node B before acknowledging them.
    if (
      packet.type === 'chat' &&
      typeof packet.id === 'string' &&
      typeof packet.text === 'string' &&
      Number.isInteger(Number(packet.senderUserId)) &&
      Number(packet.senderUserId) > 0 &&
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
          'A'
        );

        // Remember the ID only after persistence succeeds.
        rememberMessageId(packet.id);

        if (!firstReceipt) {
          console.log(
            'Duplicate message already persisted; acknowledging:',
            packet.id
          );

          // ACK retries so Node B does not retry forever.
          sendAcknowledgement(peer, packet.id);
          return;
        }

        // Deliver to browser clients only after successful persistence.
        broadcastToClients(`${packet.username}: ${packet.text}`);

        // ACK only after PostgreSQL confirms the message is saved.
        sendAcknowledgement(peer, packet.id);
      } catch (error) {
        console.error(
          `Could not persist incoming message ${packet.id}:`,
          error.message
        );

        // No ACK: Node B can retry delivery.
      }
    }
  } catch (error) {
    console.error('Error processing message from Node B:', error.message);
  }
});


  peer.on('close', () => {
  // Ignore events from an old connection.
  if (peerSocket !== peer) {
    return;
  }

  // Mark Node B as disconnected.
  peerConnected = false;
  peerAuthenticated = false;
  peerSocket = null;

  // Cancel ACK timers for messages that will be retried.
  for (const timer of ackTimers.values()) {
    clearTimeout(timer);
  }

  ackTimers.clear();
  awaitingAcks.clear();
  retryCounts.clear();
  flushingQueue = false;

  console.log('Connection to Node B closed.');

  // Automatically reconnect to Node B.
  scheduleReconnect();
});

  peer.on('error', () => {

  // Avoid logging a separate error for every failed attempt.
});
}


/**
 * Accept browser connections to Node A.
 * A browser must authenticate before joining the client set.
 */
server.on('connection', (socket) => {
  let authenticated = false;
  let authTimer = null;

  // Reject clients that never provide an authentication token.
  authTimer = setTimeout(() => {
    if (!authenticated) {
      console.log('Unauthenticated connection rejected: timeout.');
      socket.close(1008, 'Authentication required');
    }
  }, 5000);

  socket.on('message', async(data) => {
    const incomingData = data.toString();

    // STEP 1: Require authentication as the first message.
    if (!authenticated) {
      let authPacket;

      try {
        authPacket = JSON.parse(incomingData);
      } catch (error) {
        console.log('Rejected connection: invalid authentication packet.');
        socket.close(1008, 'Authentication required');
        return;
      }

      if (
        !authPacket ||
        authPacket.type !== 'auth' ||
        typeof authPacket.token !== 'string'
      ) {
        console.log('Rejected connection: authentication token missing.');
        socket.close(1008, 'Authentication required');
        return;
      }

      // Verify the JWT created by the login API.
      const user = verifyAuthToken(authPacket.token);

      if (!user) {
        console.log('Rejected connection: invalid or expired token.');
        socket.close(1008, 'Invalid or expired token');
        return;
      }

      // Store the verified identity on this connection.
      socket.username = user.username;
      socket.userId = user.id;
      authenticated = true;

      clearTimeout(authTimer);
      authTimer = null;

      // Only authenticated browsers join the client set.
      clients.add(socket);

      console.log(
        `Authenticated user ${socket.username} connected to Node A. ` +
        `Total: ${clients.size}`
      );

      // Tell the browser authentication succeeded.
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({
          type: 'auth_success',
          user: {
            id: socket.userId,
            username: socket.username
          }
        }));
      }

      return;
    }

    // STEP 2: Ignore empty chat messages.
    const userMessage = incomingData.trim();

    if (!userMessage) {
      return;
    }

        // STEP 3: Handle explicit recovery of failed messages.
    if (userMessage.toLowerCase() === '/recover') {
      try {
        const recovered = await recoverFailedMessages();

        void flushPendingMessages();

        if (socket.readyState === WebSocket.OPEN) {
          if (recovered > 0) {
            socket.send(
              `System: Recovered ${recovered} failed message(s).`
            );
          } else {
            socket.send(
              'System: No failed messages were available for recovery.'
            );
          }
        }
      } catch (error) {
        console.error('Failed to recover messages:', error.message);

        if (socket.readyState === WebSocket.OPEN) {
          socket.send(
            'System: Message recovery failed. Please try again later.'
          );
        }
      }

      return;
    }

    // STEP 4: Build a persistent message packet using the verified identity.
    const messagePacket = {
      type: 'chat',
      id: crypto.randomUUID(),
      senderUserId: Number(socket.userId),
      username: socket.username,
      originNode: 'A',
      text: userMessage
    };

    // STEP 5: Persist the message before broadcasting or queuing it.
    try {
      await messageStore.persistOutgoingMessage(messagePacket, 'B');
    } catch (error) {
      console.error(
        `Could not persist outgoing message ${messagePacket.id}:`,
        error.message
      );

      if (socket.readyState === WebSocket.OPEN) {
        socket.send(
          'System: Your message could not be saved. Please try again later.'
        );
      }

      return;
    }

    // STEP 6: Record the persisted message ID in memory.
    rememberMessageId(messagePacket.id);

    console.log(
  `Node A received: ${messagePacket.username}: ${messagePacket.text}`
);

// Broadcast the persisted message as a structured chat packet.
broadcastToClients({
  type: 'chat',
  id: messagePacket.id,
  senderUserId: messagePacket.senderUserId,
  username: messagePacket.username,
  originNode: messagePacket.originNode,
  text: messagePacket.text
});

    // STEP 8: Queue delivery to Node B using the existing retry mechanism.
    const queued = sendToPeer(JSON.stringify(messagePacket));

    // Notify the sender if the peer queue is full.
    if (!queued && socket.readyState === WebSocket.OPEN) {
      socket.send(
        'System: Message was saved, but the pending delivery queue is full. ' +
        'It may not reach Node B until recovery. Please check before resending.'
      );
    }
  });

  // STEP 8: Clean up when the browser disconnects.
  socket.on('close', () => {
    if (authTimer) {
      clearTimeout(authTimer);
      authTimer = null;
    }

    if (authenticated) {
      clients.delete(socket);

      console.log(
        `Authenticated user ${socket.username} disconnected from Node A. ` +
        `Total: ${clients.size}`
      );
    }
  });

  // STEP 9: Report browser connection errors.
  socket.on('error', (error) => {
    console.error('Browser client error:', error.message);
  });
});


// Start the first connection attempt.
connectToNodeB();
