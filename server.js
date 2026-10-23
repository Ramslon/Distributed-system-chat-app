
require('dotenv').config();

const WebSocket = require('ws');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

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

// Port used by Server Node A.
const PORT = 8090;

// Address of Server Node B.
const NODE_B_URL = 'ws://localhost:8081/?peer=A';

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


/**
 * Recover previously failed messages after Node B reconnects.
 * Original message IDs are preserved to prevent duplicate delivery.
 */
function recoverFailedMessages() {
  if (failedMessages.size === 0) {
    return;
  }

  const availableSlots =
    MAX_PENDING_MESSAGES - pendingMessages.length;

  if (availableSlots <= 0) {
    console.warn(
      'Cannot recover failed messages: pending queue is full.'
    );
    return;
  }

  const recoveredPackets = [];

  for (const [messageId, packet] of failedMessages) {
    // Do not exceed the pending queue limit.
    if (recoveredPackets.length >= availableSlots) {
      break;
    }

    // Check whether this message is already queued.
    const alreadyQueued = pendingMessages.some((queuedPacket) => {
      try {
        return JSON.parse(queuedPacket).id === messageId;
      } catch (error) {
        return false;
      }
    });

    if (alreadyQueued) {
      // Remove the redundant failed entry.
      failedMessages.delete(messageId);
      continue;
    }

    // Recover the original packet without generating a new ID.
    recoveredPackets.push(packet);
    failedMessages.delete(messageId);
  }

  if (recoveredPackets.length > 0) {
    // Place recovered messages before newer pending messages.
    pendingMessages.unshift(...recoveredPackets);

    console.log(
      `Recovered ${recoveredPackets.length} failed message(s).`
    );

    console.log(
      `Messages still awaiting recovery: ${failedMessages.size}`
    );
  }
}


// Display a startup message.
console.log(`Server Node A running on ws://localhost:${PORT}`);

// Broadcast a message to browser clients connected to Node A.
function broadcastToClients(message) {
  clients.forEach((client) => {
    // Send only if the browser connection is open.
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
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
  // Prevent multiple timers for the same message.
  clearAckTimer(messageId);

  const timer = setTimeout(() => {
    ackTimers.delete(messageId);

    // The message may have been acknowledged while the timer
    // was waiting to execute.
    if (!awaitingAcks.has(messageId)) {
      return;
    }

    // If the message was removed from the queue, nothing remains
    // to retry.
    const stillQueued = pendingMessages.some((queuedPacket) => {
      try {
        return JSON.parse(queuedPacket).id === messageId;
      } catch (error) {
        return false;
      }
    });

    if (!stillQueued) {
      awaitingAcks.delete(messageId);
      retryCounts.delete(messageId);
      return;
    }

    const retries = (retryCounts.get(messageId) || 0) + 1;
    retryCounts.set(messageId, retries);

    // Stop automatic retries after the configured limit.
    if (retries > MAX_RETRIES) {
    const index = pendingMessages.findIndex((queuedPacket) => {
    try {
      return JSON.parse(queuedPacket).id === messageId;
    } catch (error) {
      return false;
    }
    });

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

  // Continue processing any remaining queued messages.
  flushingQueue = false;
  flushPendingMessages();
  return;
  }

    console.log(
      `ACK timeout for ${messageId}. Retry attempt ${retries}.`
    );

    // Allow flushPendingMessages() to resend the same packet.
    awaitingAcks.delete(messageId);
    flushingQueue = false;

    flushPendingMessages();
  }, ACK_TIMEOUT_MS);

  ackTimers.set(messageId, timer);
}

// Remove a message only after its ACK arrives.
function acknowledgeMessage(messageId) {
  const index = pendingMessages.findIndex((queuedPacket) => {
    try {
      return JSON.parse(queuedPacket).id === messageId;
    } catch (error) {
      return false;
    }
  });

  // Stop any timeout and clear retry tracking.
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

// Continue processing the next queued message.
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


// Send the oldest queued message and wait for its ACK.
function flushPendingMessages() {
  if (flushingQueue) return;

  if (
  !peerSocket ||
  peerSocket.readyState !== WebSocket.OPEN ||
  !peerAuthenticated
  ) {
  return;
  }

  if (pendingMessages.length === 0) return;

  const packet = pendingMessages[0];
  let messageId;

  try {
    messageId = JSON.parse(packet).id;
  } catch (error) {
    console.error('Invalid queued message:', error.message);
    return;
  }

  // Do not send a message that is already awaiting its ACK.
  if (awaitingAcks.has(messageId)) return;

  flushingQueue = true;
  const currentPeer = peerSocket;

  // Track the message before sending it.
  awaitingAcks.add(messageId);

  console.log(`Sending message ${messageId} to peer`);

  currentPeer.send(packet, (error) => {
    flushingQueue = false;

    if (error) {
  console.error(
    'Message send failed; keeping it queued:',
    error.message
  );

  // Keep tracking the message and retry after the timeout.
  // The peer's close handler will clear this state if it disconnects.
  scheduleAckTimeout(messageId);

  return;
}

    // The ACK might already have arrived before this callback.
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

    // If an ACK already removed this packet, process the next one.
    if (!stillQueued) {
      flushPendingMessages();
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

peer.on('message', (data) => {
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

  recoverFailedMessages();
  flushPendingMessages();
  return;
  }

    // Handle an acknowledgement from Node B.
    if (
      packet.type === 'ack' &&
      typeof packet.id === 'string'
    ) {
      acknowledgeMessage(packet.id);
      return;
    }

    // Handle a chat message from Node B.
    if (
      packet.type === 'chat' &&
      typeof packet.id === 'string' &&
      typeof packet.text === 'string'
    ) {
      // Even duplicates must be acknowledged.
      // Otherwise, Node B may retry the same message indefinitely.
      if (!rememberMessageId(packet.id)) {
        console.log('Duplicate message ignored:', packet.id);
        sendAcknowledgement(peer, packet.id);
        return;
      }

      console.log('Received message from Node B:', packet.text);

      // Deliver the message to Node A's browser clients.
      broadcastToClients(packet.text);

      // Confirm that Node A has processed the message.
      sendAcknowledgement(peer, packet.id);
    }
  } catch (error) {
    console.error(
      'Invalid message received from Node B:',
      error.message
    );
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

  socket.on('message', (data) => {
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

    // STEP 3: Handle the existing recovery command.
    if (userMessage.toLowerCase() === '/recover') {
      const failedBefore = failedMessages.size;

      recoverFailedMessages();

      const recovered = failedBefore - failedMessages.size;

      flushPendingMessages();

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

      return;
    }

    // STEP 4: Build the sender identity from the verified token.
    // Never accept the sender's identity from the browser message.
    const message = `${socket.username}: ${userMessage}`;

    console.log(`Node A received: ${message}`);

    // STEP 5: Create a unique message ID.
    const messageId = crypto.randomUUID();

    rememberMessageId(messageId);

    // STEP 6: Preserve the existing distributed message format.
    const packet = JSON.stringify({
      type: 'chat',
      id: messageId,
      text: message
    });

    // STEP 7: Broadcast locally and queue delivery to Node B.
    broadcastToClients(message);

    const queued = sendToPeer(packet);

    // Notify the sender if the peer queue is full.
    if (!queued && socket.readyState === WebSocket.OPEN) {
      socket.send(
        'System: Message was not queued because the pending queue is full. ' +
        'Please try again later.'
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
