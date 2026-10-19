
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

  console.log(`Sending message ${messageId} to Node A`);

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

    // Handle an acknowledgement from Node A.
    if (
      packet.type === 'ack' &&
      typeof packet.id === 'string'
    ) {
      acknowledgeMessage(packet.id);
      return;
    }

    // Handle a chat message from Node A.
    if (
      packet.type === 'chat' &&
      typeof packet.id === 'string' &&
      typeof packet.text === 'string'
    ) {
      // A duplicate still receives an ACK.
      if (!rememberMessageId(packet.id)) {
        console.log('Duplicate message ignored:', packet.id);
        sendAcknowledgement(socket, packet.id);
        return;
      }

      // Deliver the message to Node B's browser clients.
      broadcastToClients(packet.text);

      // Tell Node A that the message was processed.
      sendAcknowledgement(socket, packet.id);
    }
  } catch (error) {
    console.error(
      'Invalid message received from Node A:',
      error.message
    );
  }
});

    // Clear the peer reference if this connection closes.
    socket.on('close', () => {
      if (peerSocket === socket) {
        peerSocket = null;
        awaitingAcks.clear();
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
