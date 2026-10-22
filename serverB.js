
const WebSocket = require('ws');
const crypto = require('crypto');

// Port used by Server Node B.
const PORT = 8081;

// Create a WebSocket server for Node B.
const server = new WebSocket.Server({ port: PORT });

// Store browser clients connected directly to Node B.
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

  if (!peerSocket || peerSocket.readyState !== WebSocket.OPEN) {
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

// Listen for new connections to Node B.
server.on('connection', (socket, request) => {
  // Read the URL used to establish this connection.
  const requestUrl = new URL(
    request.url,
    `http://localhost:${PORT}`
  );

  // Check whether this connection comes from Server Node A.
  const isPeer = requestUrl.searchParams.get('peer') === 'A';

  // Handle a server-to-server connection separately.
  if (isPeer) {
    peerSocket = socket;

    console.log('Server Node A connected to Node B');

    // Send messages that were queued while Node A was unavailable.
    flushPendingMessages();

    
socket.on('message', (data) => {
  try {
    const packet = JSON.parse(data.toString());

    // Handle an acknowledgement received from Node A.
    if (
      packet.type === 'ack' &&
      typeof packet.id === 'string'
    ) {
      acknowledgeMessage(packet.id);
      return;
    }

    // Handle a chat message received from Node A.
    if (
      packet.type === 'chat' &&
      typeof packet.id === 'string' &&
      typeof packet.text === 'string'
    ) {
      // Duplicate messages must still receive an ACK.
      if (!rememberMessageId(packet.id)) {
  console.log('Duplicate message ignored:', packet.id);
  sendAcknowledgement(socket, packet.id);
  return;
}

      // Deliver the new message to Node B's browser clients.
      broadcastToClients(packet.text);

      // Normally acknowledge the processed message.
      sendAcknowledgement(socket, packet.id);
    }
  } catch (error) {
    console.error(
      'Invalid message received from Node A:',
      error.message
    );
  }
});

    socket.on('close', () => {
  // Only clean up state if this is still the active peer.
  if (peerSocket === socket) {
    peerSocket = null;

    // Cancel all outstanding ACK timers.
    for (const timer of ackTimers.values()) {
      clearTimeout(timer);
    }

    ackTimers.clear();
    awaitingAcks.clear();
    retryCounts.clear();
    flushingQueue = false;
   }

  console.log('Connection from Server Node A closed');
   });

    // Log errors on the server-to-server connection.
    socket.on('error', (error) => {
      console.error('Node A connection error:', error.message);
    });

    // Do not treat Server Node A as a browser client.
    return;
  }

  // Register an ordinary browser client.
  clients.add(socket);

  console.log(`Client connected to Node B. Total: ${clients.size}`);

  socket.on('message', (data) => {
  const message = data.toString();
  

  console.log(`Node B received: ${message}`);
  
  // Generate a unique ID for this browser message.
  const messageId = crypto.randomUUID();

  // Remember the ID before broadcasting locally.
  rememberMessageId(messageId);

  // Create a structured packet for Node A.
  const packet = JSON.stringify({
    type: 'chat',
    id: messageId,
    text: message
  });

  // Preserve the current local chat behavior.
  broadcastToClients(message);

  // Queue the message for delivery to the peer.
const queued = sendToPeer(packet);

// Inform the sender if the queue is full.
if (!queued && socket.readyState === WebSocket.OPEN) {
  socket.send(
    'System: Message was not queued because the pending queue is full. Please try again later.'
  );
}
   });
  // Remove a browser client when it disconnects.
  socket.on('close', () => {
    clients.delete(socket);

    console.log(`Client disconnected from Node B. Total: ${clients.size}`);
  });

  // Report errors on individual browser connections.
  socket.on('error', (error) => {
    console.error('Browser client error:', error.message);
  });
});
