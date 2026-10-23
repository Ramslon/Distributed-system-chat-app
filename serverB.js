

require('dotenv').config();

const WebSocket = require('ws');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

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

const PORT = 8081;
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

// Prevent concurrent queue processing.
let flushingQueue = false;

// Print a message when Node B starts.
console.log(`Server Node B running on ws://localhost:${PORT}`);

// Broadcast a plain-text message to Node B's browser clients.
function broadcastToClients(message) {
  // Visit every browser client connected to Node B.
  clients.forEach((client) => {
    // Send only when the client's connection is open.
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

    socket.on('message', (data) => {
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

        flushPendingMessages();
        return;
      }

      // Ignore messages from a socket that is no longer the active peer.
      if (
        peerSocket !== socket ||
        !peerAuthenticated
      ) {
        return;
      }

      // Preserve the existing acknowledgement system.
      if (
        packet.type === 'ack' &&
        typeof packet.id === 'string'
      ) {
        acknowledgeMessage(packet.id);
        return;
      }

      // Preserve duplicate detection and delivery acknowledgements.
      if (
        packet.type === 'chat' &&
        typeof packet.id === 'string' &&
        typeof packet.text === 'string'
      ) {
        if (!rememberMessageId(packet.id)) {
          console.log(
            'Duplicate message ignored:',
            packet.id
          );

          sendAcknowledgement(socket, packet.id);
          return;
        }

        broadcastToClients(packet.text);
        sendAcknowledgement(socket, packet.id);
      }
    });

    socket.on('close', () => {
      if (peerSocket === socket) {
        peerSocket = null;
        peerAuthenticated = false;

        for (const timer of ackTimers.values()) {
          clearTimeout(timer);
        }

        ackTimers.clear();
        awaitingAcks.clear();
        retryCounts.clear();
        flushingQueue = false;
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

  socket.on('message', (data) => {
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

    // Only authenticated clients may send chat messages.
    const userMessage = rawMessage.trim();

    if (!userMessage) {
      return;
    }

    // Use the authenticated username, not a client-supplied identity.
    const message = `${socket.username}: ${userMessage}`;
    const messageId = crypto.randomUUID();

    rememberMessageId(messageId);

    const packet = JSON.stringify({
      type: 'chat',
      id: messageId,
      text: message
    });

    // Preserve immediate local broadcast and queued peer delivery.
    broadcastToClients(message);

    const queued = sendToPeer(packet);

    if (
      !queued &&
      socket.readyState === WebSocket.OPEN
    ) {
      socket.send(
        'System: Message was not queued because the pending queue is full. Please try again later.'
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

