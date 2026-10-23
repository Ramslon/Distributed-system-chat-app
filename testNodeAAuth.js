
const WebSocket = require('ws');

const API_URL = 'http://127.0.0.1:3000/api/login';
const WS_URL = 'ws://127.0.0.1:8090';

async function getLoginToken() {
  const email = process.env.CHAT_TEST_EMAIL;
  const password = process.env.CHAT_TEST_PASSWORD;

  if (!email || !password) {
    throw new Error(
      'Set CHAT_TEST_EMAIL and CHAT_TEST_PASSWORD in PowerShell first.'
    );
  }

  const response = await fetch(API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ email, password })
  });

  const result = await response.json();

  if (!response.ok || !result.token) {
    throw new Error('Test login failed: ' + (result.error || 'No token returned'));
  }

  return result.token;
}

function testValidToken(token) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(WS_URL);
    let finished = false;

    const timeout = setTimeout(() => {
      if (!finished) {
        finished = true;
        socket.terminate();
        reject(new Error('Valid-token test timed out.'));
      }
    }, 8000);

    socket.on('open', () => {
      socket.send(JSON.stringify({
        type: 'auth',
        token
      }));
    });

    socket.on('message', (data) => {
      let packet;

      try {
        packet = JSON.parse(data.toString());
      } catch {
        return;
      }

      if (packet.type === 'auth_success') {
        finished = true;
        clearTimeout(timeout);

        console.log(
          'VALID TOKEN: PASS — authenticated as ' +
          packet.user.username
        );

        socket.close();
        resolve();
      }
    });

    socket.on('error', (error) => {
      if (!finished) {
        finished = true;
        clearTimeout(timeout);
        reject(error);
      }
    });

    socket.on('close', () => {
      if (!finished) {
        finished = true;
        clearTimeout(timeout);
        reject(new Error('Node A closed a valid-token connection.'));
      }
    });
  });
}

function testInvalidToken() {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(WS_URL);
    let finished = false;

    const timeout = setTimeout(() => {
      if (!finished) {
        finished = true;
        socket.terminate();
        reject(new Error('Invalid-token test timed out.'));
      }
    }, 8000);

    socket.on('open', () => {
      socket.send(JSON.stringify({
        type: 'auth',
        token: 'this-is-not-a-valid-token'
      }));
    });

    socket.on('close', (code) => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      if (code === 1008) {
        console.log('INVALID TOKEN: PASS — connection rejected.');
        resolve();
      } else {
        reject(new Error('Unexpected close code: ' + code));
      }
    });

    socket.on('error', (error) => {
      if (!finished) {
        finished = true;
        clearTimeout(timeout);
        reject(error);
      }
    });
  });
}

function testMissingToken() {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(WS_URL);
    let finished = false;

    const timeout = setTimeout(() => {
      if (!finished) {
        finished = true;
        socket.terminate();
        reject(new Error('Missing-token test timed out.'));
      }
    }, 8000);

    // Intentionally do not send an authentication message.
    socket.on('close', (code) => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      if (code === 1008) {
        console.log('MISSING TOKEN: PASS — connection rejected.');
        resolve();
      } else {
        reject(new Error('Unexpected close code: ' + code));
      }
    });

    socket.on('error', (error) => {
      if (!finished) {
        finished = true;
        clearTimeout(timeout);
        reject(error);
      }
    });
  });
}

async function main() {
  const token = await getLoginToken();

  await testValidToken(token);
  await testInvalidToken();
  await testMissingToken();

  console.log('\nAll Node A authentication tests passed.');
}

main().catch((error) => {
  console.error('\nAUTHENTICATION TEST FAILED:', error.message);
  process.exitCode = 1;
});
