
# Distributed Chat Application

A browser-based distributed chat application built with Node.js, WebSockets, Express, and PostgreSQL. It consists of an authentication API and two WebSocket chat nodes that exchange messages, allowing authenticated users connected to either node to participate in the same conversation.

## Live Application

- **Chat application:** https://distributed-system-chat-app.vercel.app/
- **Node A client:** https://distributed-system-chat-app.vercel.app/
- **Node B client:** https://distributed-system-chat-app.vercel.app/indexB.html
- **Authentication API health:** https://chat-auth-api.onrender.com/api/health

The frontend is hosted on Vercel, while the authentication API and chat nodes are deployed on Render. The authentication API uses PostgreSQL hosted on Neon.

## Features

- User registration and login using email and password.
- Password hashing with bcrypt.
- JWT-based authentication for WebSocket connections.
- Two WebSocket nodes capable of exchanging chat messages.
- Authenticated usernames attached to messages by the server.
- Peer authentication between chat nodes.
- Message acknowledgements, duplicate detection, retries, and offline message queuing.
- Reconnection handling when a peer node becomes unavailable.
- Rate limiting on registration and login endpoints.
- CORS configuration and HTTP security headers for the authentication API.
- A recovery command for retrying messages that have exhausted automatic delivery retries.

> **Note:** Message recovery and delivery reliability depend on the implemented retry and queue logic. The application does not currently provide a persistent historical chat transcript to users after reconnecting.

## Technology Stack

| Component | Technology |
|---|---|
| Backend | Node.js |
| Real-time communication | WebSockets (`ws`) |
| Authentication API | Express.js |
| Database | PostgreSQL |
| Password security | bcrypt |
| Authentication tokens | JSON Web Tokens (JWT) |
| Frontend | HTML, CSS, JavaScript |
| Frontend hosting | Vercel |
| Backend hosting | Render |
| Database hosting | Neon PostgreSQL |

## Requirements

For local development, install:

- Node.js 20 or later.
- npm.
- Access to a PostgreSQL database.
- A modern web browser.
- A static-file server, such as the VS Code Live Server extension.

## Local Setup and Installation

### 1. Clone the repository

```powershell
git clone https://github.com/Ramslon/Distributed-system-chat-app.git
cd Distributed-system-chat-app
```

### 2. Install dependencies

Run this command from the project directory:

```powershell
npm install
```

### 3. Configure environment variables

Create a `.env` file in the project root. This file is ignored by Git and must not be committed or shared.

Example configuration:

```dotenv
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=require

JWT_SECRET=replace-with-a-random-secret
INTERNAL_PEER_SECRET=replace-with-a-different-random-secret
JWT_EXPIRES_IN=1h

# Optional local port overrides
AUTH_PORT=3000
NODE_A_PORT=8090
NODE_B_PORT=8081

# Node A uses this URL to connect to Node B
NODE_B_URL=ws://localhost:8081/?peer=A
```

Replace the example database credentials and secrets with your own values. `JWT_SECRET` and `INTERNAL_PEER_SECRET` must be different, unpredictable values.

Generate a random secret in PowerShell:

```powershell
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

Run this command separately for each secret.

**Security precautions:**

- Never commit `.env` or share its contents.
- Never put JWT signing secrets or peer authentication secrets in browser-side JavaScript.
- Use HTTPS and secure WebSocket connections (`wss://`) in production.
- Keep dependencies updated and restrict database credentials to the permissions the application needs.

The authentication API initializes the required user table and case-insensitive username index at startup. The database account must have the necessary permissions to create them.

### 4. Start the services

Open three separate PowerShell terminals in the project directory.

**Terminal 1 — Authentication API**

```powershell
node authServer.js
```

**Terminal 2 — WebSocket Node A**

```powershell
node server.js
```

**Terminal 3 — WebSocket Node B**

```powershell
node serverB.js
```

Keep all three processes running during local testing.

The local authentication health endpoint is:

http://localhost:3000/api/health

A healthy response should resemble:

```json
{
  "status": "ok",
  "database": "connected"
}
```

The default local ports are:

- Authentication API: `3000`
- Node A: `8090`
- Node B: `8081`

These defaults can be overridden using the environment variables shown above. On Render, the services may use the platform-provided `PORT` variable.

### 5. Configure the browser clients for local development

The HTML clients currently use the hosted services. For local development, update the `SERVER_URL` and `AUTH_API_URL` constants in each HTML page's inline JavaScript.

| Client | `SERVER_URL` | `AUTH_API_URL` |
|---|---|---|
| `index.html` | `ws://localhost:8090` | `http://localhost:3000/api` |
| `indexB.html` | `ws://localhost:8081` | `http://localhost:3000/api` |

Serve the project directory using VS Code Live Server. The current local CORS configuration allows these origins:

- `http://localhost:5500`
- `http://127.0.0.1:5500`
- `http://localhost:5501`
- `http://127.0.0.1:5501`

Open `index.html` and `indexB.html` through the static-file server rather than relying on the browser's `file://` protocol.

If you use different origins or ports, update the `allowedOrigins` configuration in `authServer.js` accordingly.

The deployed frontend continues to use the hosted API and WebSocket URLs unless you change its configuration.

## Using the Application

### 1. Register an account

Open either browser client, select **Register**, and provide a username, email address, and password that satisfy the application's validation rules.

Usernames must contain 3–24 letters, numbers, or underscores. Passwords must meet the minimum length enforced by the authentication API.

### 2. Log in

If registration does not automatically authenticate you, use **Login** with your registered email address and password.

### 3. Connect to a chat node

Wait for authentication and the WebSocket connection to complete. The client should enable message sending once the connection is ready.

### 4. Send messages

Enter a message and select **Send** or press Enter. The server associates the message with the authenticated username and forwards it to the connected chat clients through the distributed nodes.

### 5. Test distributed communication

Open the Node A and Node B clients in separate browser tabs or windows. Send messages from each client and verify that they are delivered across the two nodes.

### 6. Log out

Use **Logout** to clear the browser session and disconnect. Authentication information is retained in the current tab's session storage according to the client implementation, so a page refresh may restore the connection while the session remains valid.

The application displays live messages; it does not currently load a historical transcript after reconnecting.

## Reliability and Recovery

The chat nodes implement message-delivery reliability mechanisms, including acknowledgements, duplicate detection, retry handling, and temporary queuing when a peer is unavailable.

Node A recognizes the `/recover` command for retrying messages that have exhausted automatic peer-delivery retries.

These mechanisms help handle transient peer failures. They should not be interpreted as a guarantee of permanent message storage or exactly-once delivery to every browser client.

## Project Structure

| File | Description |
|---|---|
| `authServer.js` | Express authentication API, PostgreSQL user setup, registration, login, and token issuance. |
| `server.js` | WebSocket Node A, browser authentication, peer communication, and message delivery. |
| `serverB.js` | WebSocket Node B, browser authentication, peer authentication, and message delivery. |
| `index.html` | Browser chat client connected to Node A. |
| `indexB.html` | Browser chat client connected to Node B. |
| `client.js` | Standalone WebSocket example. |
| `testNodeAAuth.js` | Manual authentication checks for Node A. |
| `async-demo.js` | Demonstration of asynchronous JavaScript execution. |
| `package.json` | Project metadata, dependencies, and npm scripts. |
| `package-lock.json` | Records resolved npm dependency versions. |

## Testing

The authentication check can be run manually while the authentication API and Node A are running locally.

Set credentials for an existing test account in the current PowerShell session:

```powershell
$env:CHAT_TEST_EMAIL = "test-user@example.com"
$env:CHAT_TEST_PASSWORD = "your-test-password"

node testNodeAAuth.js
```

The test checks valid-token authentication, invalid-token rejection, and rejection of a connection without a token.

Replace the example values with test-account credentials. Do not put real credentials in source code or commit them to Git.

### Manual testing checklist

- [ ] Register a new test account.
- [ ] Log in with valid credentials.
- [ ] Confirm unknown emails and incorrect passwords receive generic authentication errors.
- [ ] Confirm an invalid or expired JWT is rejected.
- [ ] Confirm an unauthenticated WebSocket connection is rejected.
- [ ] Send messages between clients connected to Node A and Node B.
- [ ] Stop one node, verify queuing and reconnection behavior, and restart it.
- [ ] Verify that pending messages are retried and duplicate messages are handled correctly.
- [ ] Confirm the authentication API health endpoint reports a connected database.

**Automated testing status:** The `npm test` script is currently a placeholder and exits with an error. The manual checks described above do not replace comprehensive automated, integration, security, or performance testing.

## Deployment

The production application uses the following services:

| Service | Hosting | Purpose |
|---|---|---|
| Frontend | Vercel | Browser chat clients |
| Authentication API | Render | Registration, login, and JWT issuance |
| Node A | Render | WebSocket connections and peer communication |
| Node B | Render | WebSocket connections and peer communication |
| PostgreSQL | Neon | User account storage |

Production configuration is managed through the hosting providers' environment-variable settings.

Ensure the deployed services use consistent JWT signing configuration and matching internal peer authentication secrets. Node A must use Node B's deployed secure WebSocket URL, and the authentication API must use the correct production database connection string.

Never put production secrets in the README, Git repository, or frontend code.

## Contributing

1. Create a branch for your change.
2. Make and test your changes locally.
3. Run the available syntax and whitespace checks.
4. Review the Git diff to ensure no secrets or unrelated files are included.
5. Commit and push your changes.

## License

No license has been specified yet. Add a `LICENSE` file if you intend to grant others permission to use, modify, or distribute this project.
