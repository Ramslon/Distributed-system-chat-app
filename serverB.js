
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

// Add a message to the queue and attempt delivery.
function sendToPeer(packet) {
  // Prevent the queue from growing beyond its configured limit.
  if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
    console.error('Message queue full. Message was not queued.');
    return;
  }

  // Add the message to the end of the queue.
  pendingMessages.push(packet);

  // Try to deliver queued messages.
  flushPendingMessages();
}

// Deliver queued messages to Node A in order.
function flushPendingMessages() {
  // Stop if another flush is in progress.
  if (flushingQueue) {
    return;
  }

  // Stop if Node A is not connected.
  if (!peerSocket || peerSocket.readyState !== WebSocket.OPEN) {
    return;
  }

  // Stop if there are no messages waiting.
  if (pendingMessages.length === 0) {
    return;
  }

  // Mark the queue as being processed.
  flushingQueue = true;

  // Keep the first message in the queue until send completes.
  const packet = pendingMessages[0];

  // Remember which connection is sending this message.
  const currentPeer = peerSocket;

  // Send the first queued message to Node B.
  currentPeer.send(packet, (error) => {
    // Allow the next flush attempt.
    flushingQueue = false;

    // Keep the message queued if sending failed.
    if (error) {
      console.error('Queued message delivery failed:', error.message);
      return;
    }

    // Remove the message only if it is still at the front.
    if (pendingMessages[0] === packet) {
      pendingMessages.shift();
    }

    // Send the next message if this connection remains open.
    if (
      peerSocket === currentPeer &&
      currentPeer.readyState === WebSocket.OPEN
    ) {
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

    // Accept only valid chat packets with a message ID.
    if (
      packet.type === 'chat' &&
      typeof packet.id === 'string' &&
      typeof packet.text === 'string'
    ) {
      // Ignore messages already processed by Node B.
      if (!rememberMessageId(packet.id)) {
        console.log('Duplicate message ignored:', packet.id);
        return;
      }

      // Display the message to Node B's local browser clients.
      broadcastToClients(packet.text);
     }
     } catch (error) {
    console.error('Invalid message received from Node A:', error.message);
     }
    });

    // Clear the peer reference if this connection closes.
    socket.on('close', () => {
      if (peerSocket === socket) {
        peerSocket = null;
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

  // Queue the message for delivery to Node A.
  sendToPeer(packet);
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
