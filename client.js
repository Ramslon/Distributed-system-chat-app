// Step 1: Import WebSocket
const WebSocket = require('ws');

// Step 2: Connect to the server
const socket = new WebSocket('ws://localhost:8080');

// Step 3: When connection opens, send a message
socket.on('open', () => {
  console.log('Connected to server');
  socket.send('Hello from client!');
});

// Step 4: Listen for messages from server
socket.on('message', message => {
  console.log(`Message from server: ${message}`);
});
