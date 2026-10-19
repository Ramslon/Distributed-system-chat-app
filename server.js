
const WebSocket = require('ws');
const crypto = require('crypto');

// Port used by Server Node A.
const PORT = 8080;

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

// Limit the queue to prevent unlimited memory growth.
const MAX_PENDING_MESSAGES = 1000;

// Prevent multiple queued messages from being flushed simultaneously.
let flushingQueue = false;

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

// Add a message to the queue and attempt delivery.
function sendToPeer(packet) {
  // Prevent the queue from growing beyond its configured limit.
  if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
    console.error('Message queue full. Message was not queued.');
    return;
  }

  // Add the message to the end of the queue.
  pendingMessages.push(packet);

  console.log('Queued message. Pending count:', pendingMessages.length);

  // Try to deliver queued messages.
  flushPendingMessages();
}

// Remove a message from the queue only after its ACK arrives.
function acknowledgeMessage(messageId) {
  // Find the queued message with the acknowledged ID.
  const index = pendingMessages.findIndex((queuedPacket) => {
    try {
      return JSON.parse(queuedPacket).id === messageId;
    } catch (error) {
      return false;
    }
  });

  // Remove the message if it is still waiting in the queue.
  if (index !== -1) {
    pendingMessages.splice(index, 1);

    console.log(`Message acknowledged: ${messageId}`);
    console.log(
      `Remaining queued messages: ${pendingMessages.length}`
    );
  } else {
    console.log(`ACK received for message: ${messageId}`);
  }

  // The message no longer needs an acknowledgement.
  awaitingAcks.delete(messageId);

  // Continue with the next queued message.
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


// Send one queued message and wait for its ACK before sending the next.
function flushPendingMessages() {
  // Avoid processing the queue concurrently.
  if (flushingQueue) return;

  // A connection to Node B must be open.
  if (!peerSocket || peerSocket.readyState !== WebSocket.OPEN) {
    return;
  }

  // Nothing needs to be delivered.
  if (pendingMessages.length === 0) return;

  // Always process the oldest queued message first.
  const packet = pendingMessages[0];

  let messageId;

  try {
    messageId = JSON.parse(packet).id;
  } catch (error) {
    console.error('Invalid queued message:', error.message);
    return;
  }

  // Do not resend a message while waiting for its ACK.
  if (awaitingAcks.has(messageId)) return;

  flushingQueue = true;

  const currentPeer = peerSocket;

  // Mark the message as awaiting an ACK before sending.
  awaitingAcks.add(messageId);

  console.log(`Sending message ${messageId} to Node B`);

  currentPeer.send(packet, (error) => {
    flushingQueue = false;

    // Keep the queued message if delivery failed.
    if (error) {
      awaitingAcks.delete(messageId);

      console.error(
        'Message send failed; keeping it queued:',
        error.message
      );

      return;
    }

    // Do not remove the message here.
    // We remove it only when acknowledgeMessage() receives its ACK.
    console.log(`Waiting for ACK for message ${messageId}`);
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

  // Runs when Node B accepts the connection.
  peer.on('open', () => {
  // Mark Node B as connected.
  peerConnected = true;

  // Reset the retry count after a successful connection.
  reconnectAttempts = 0;

  console.log('Successfully connected to Node B');

  // Deliver queued messages if you implemented the message queue.
  flushPendingMessages();
});


peer.on('message', (data) => {
  try {
    const packet = JSON.parse(data.toString());

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

  // Handle a closed or failed peer connection.
 peer.on('close', () => {
  // Ignore events from an old connection.
  if (peerSocket !== peer) {
    return;
  }

  // Mark the peer as unavailable.
  peerConnected = false;
  peerSocket = null;

  awaitingAcks.clear();

  // Log the disconnect only once for this connection.
  console.log('Connection to Node B closed.');

  // Schedule the next attempt.
  scheduleReconnect();
});

  peer.on('error', () => {
  
  // Avoid logging a separate error for every failed attempt.
});
}

// Accept browser connections to Node A.
server.on('connection', (socket) => {
  // Add this browser to the connected clients.
  clients.add(socket);

  console.log(`Client connected to Node A. Total: ${clients.size}`);

  socket.on('message', (data) => {
  const message = data.toString();

  console.log(`Node A received: ${message}`);

  // Generate a unique ID for this new browser message.
  const messageId = crypto.randomUUID();

  // Record the ID so a repeated message can be detected.
  rememberMessageId(messageId);

  // Prepare the structured message packet.
  const packet = JSON.stringify({
    type: 'chat',
    id: messageId,
    text: message
  });

  // Preserve the current local chat behavior.
  broadcastToClients(message);

  // Queue the packet for delivery to Node B.
  sendToPeer(packet);
});
  // Remove a browser client when it disconnects.
  socket.on('close', () => {
    clients.delete(socket);

    console.log(`Client disconnected from Node A. Total: ${clients.size}`);
  });

  // Report browser connection errors.
  socket.on('error', (error) => {
    console.error('Browser client error:', error.message);
  });
});

// Start the first connection attempt.
connectToNodeB();
