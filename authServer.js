
require('dotenv').config();

const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

// 1. Validate configuration.
if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 32) {
  throw new Error(
    'JWT_SECRET is missing or too short. Check your .env file.'
  );
}

if (!process.env.DATABASE_URL) {
  throw new Error(
    'DATABASE_URL is missing. Configure your Neon PostgreSQL connection string.'
  );
}

const PORT = Number(process.env.PORT || process.env.AUTH_PORT) || 3000;
const TOKEN_EXPIRY = process.env.JWT_EXPIRES_IN || '1h';

// 2. Create the Express application.
const app = express();

// 3. Configure PostgreSQL.
// Use Neon's connection string and encrypted connection.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  max: 5,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 10000
});

pool.on('error', (error) => {
  console.error(
    'Unexpected idle PostgreSQL connection error:',
    error.message
  );
});

// 4. Configure CORS for local frontend development.
const allowedOrigins = new Set([
  'http://localhost:5500',
  'http://127.0.0.1:5500',
  'http://localhost:5501',
  'http://127.0.0.1:5501'
]);

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (origin && allowedOrigins.has(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }

  res.setHeader(
    'Access-Control-Allow-Methods',
    'GET, POST, OPTIONS'
  );

  res.setHeader(
    'Access-Control-Allow-Headers',
    'Content-Type, Authorization'
  );

  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  next();
});

// Accept JSON request bodies up to 10 KB.
app.use(express.json({ limit: '10kb' }));

// 5. Health endpoint.
app.get('/api/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');

    return res.json({
      status: 'ok',
      database: 'connected'
    });
  } catch (error) {
    console.error('Health check database error:', error.message);

    return res.status(503).json({
      status: 'error',
      database: 'unavailable'
    });
  }
});

// 6. Register a new user.
app.post('/api/register', async (req, res) => {
  try {
    const { username, email, password } = req.body || {};

    if (
      typeof username !== 'string' ||
      typeof email !== 'string' ||
      typeof password !== 'string'
    ) {
      return res.status(400).json({
        error: 'Username, email and password are required.'
      });
    }

    const cleanUsername = username.trim();
    const cleanEmail = email.trim().toLowerCase();

    if (!/^[a-zA-Z0-9_]{3,24}$/.test(cleanUsername)) {
      return res.status(400).json({
        error:
          'Username must be 3–24 characters using letters, numbers or underscores.'
      });
    }

    if (
      cleanEmail.length > 254 ||
      !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)
    ) {
      return res.status(400).json({
        error: 'Enter a valid email address.'
      });
    }

    if (
      password.length < 8 ||
      Buffer.byteLength(password, 'utf8') > 72
    ) {
      return res.status(400).json({
        error:
          'Password must be at least 8 characters and no more than 72 bytes.'
      });
    }

    // Hash the password; never store the original password.
    const passwordHash = await bcrypt.hash(password, 12);

    // PostgreSQL uses $1, $2, $3 placeholders.
    const result = await pool.query(
      `INSERT INTO users (username, email, password_hash)
       VALUES ($1, $2, $3)
       RETURNING id, username, email`,
      [cleanUsername, cleanEmail, passwordHash]
    );

    const user = result.rows[0];

    return res.status(201).json({
      message: 'Account created successfully.',
      user: {
        id: user.id,
        username: user.username,
        email: user.email
      }
    });
  } catch (error) {
    // PostgreSQL unique-constraint violation.
    if (error.code === '23505') {
      return res.status(409).json({
        error: 'That username or email is already registered.'
      });
    }

    console.error('Account creation failed:', error.message);

    return res.status(500).json({
      error: 'Could not create account.'
    });
  }
});

// 7. Log in with an email address and password.
app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body || {};

    if (
      typeof email !== 'string' ||
      typeof password !== 'string' ||
      email.trim().length === 0 ||
      password.length === 0
    ) {
      return res.status(400).json({
        error: 'Email and password are required.'
      });
    }

    const cleanEmail = email.trim().toLowerCase();

    const result = await pool.query(
      `SELECT id, username, email, password_hash
       FROM users
       WHERE email = $1`,
      [cleanEmail]
    );

    const user = result.rows[0];

    // Use the same response for unknown emails and wrong passwords.
    if (!user) {
      return res.status(401).json({
        error: 'Invalid email or password.'
      });
    }

    const matches = await bcrypt.compare(
      password,
      user.password_hash
    );

    if (!matches) {
      return res.status(401).json({
        error: 'Invalid email or password.'
      });
    }

    // Preserve the existing JWT payload and subject.
    const token = jwt.sign(
      {
        username: user.username
      },
      process.env.JWT_SECRET,
      {
        subject: String(user.id),
        expiresIn: TOKEN_EXPIRY
      }
    );

    return res.json({
      message: 'Login successful.',
      token,
      expiresIn: TOKEN_EXPIRY,
      user: {
        id: user.id,
        username: user.username,
        email: user.email
      }
    });
  } catch (error) {
    console.error('Login database error:', error.message);

    return res.status(500).json({
      error: 'Login is temporarily unavailable.'
    });
  }
});

// 8. Handle malformed JSON and unexpected errors.
app.use((error, req, res, next) => {
  if (
    error instanceof SyntaxError &&
    error.status === 400 &&
    'body' in error
  ) {
    return res.status(400).json({
      error: 'Request body contains invalid JSON.'
    });
  }

  console.error('Unexpected API error:', error.message);

  return res.status(500).json({
    error: 'An unexpected server error occurred.'
  });
});

// 9. Create the table and start only after PostgreSQL is ready.
async function startServer() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Preserve case-insensitive username uniqueness from SQLite.
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_unique
      ON users (LOWER(username))
    `);

    await pool.query('SELECT 1');

    const server = app.listen(PORT, '0.0.0.0', () => {
      console.log(`Authentication API listening on port ${PORT}`);
      console.log('PostgreSQL connection established.');
    });

    const shutdown = async () => {
      console.log('Shutting down authentication API...');

      server.close(async () => {
        await pool.end();
        process.exit(0);
      });
    };

    process.once('SIGTERM', shutdown);
    process.once('SIGINT', shutdown);
  } catch (error) {
    console.error(
      'Failed to initialize PostgreSQL or start the API:',
      error.message
    );

    await pool.end().catch(() => {});
    process.exitCode = 1;
  }
}

startServer();
