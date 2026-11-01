
require('dotenv').config();

const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const helmet = require('helmet');
const { rateLimit } = require('express-rate-limit');
const messageStore = require('./messageStore');


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

const DUMMY_PASSWORD_HASH =
  '$2b$12$kG//rw1DHCoNIR3X8DdnjOqFmGZb5UYSlKRH3daQqXAkEvZ/jVeMG';

// 2. Create the Express application.
const app = express();

// Rate-limit repeated authentication requests.
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  limit: 10,                 // Maximum 10 requests per window
  standardHeaders: 'draft-8',
  legacyHeaders: false,
  message: {
    error: 'Too many login attempts. Please try again in 15 minutes.',
  },
});

const registerLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  limit: 5,                 // Maximum 5 registration requests per window
  standardHeaders: 'draft-8',
  legacyHeaders: false,
  message: {
    error: 'Too many registration attempts. Please try again later.',
  },
});

// Apply HTTP security headers.
app.use(helmet());

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

const cors = require('cors');

const allowedOrigins = [
  'http://localhost:5500',
  'http://127.0.0.1:5500',
  'http://localhost:5501',
  'http://127.0.0.1:5501',
  'https://distributed-system-chat-app.vercel.app',
];

app.use(cors({
  origin: function (origin, callback) {
    // Permit requests without an Origin header, such as
    // trusted server-to-server or command-line requests.
    if (!origin || allowedOrigins.includes(origin)) {
      return callback(null, true);
    }

    return callback(new Error('Origin not allowed by CORS'));
  },
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// Accept JSON request bodies up to 10 KB.
app.use(express.json({ limit: '10kb' }));

// 5. Authenticate requests using JWT.
async function authenticateToken(req, res, next) {
  // Read the Authorization header.
  const authHeader = req.headers.authorization;

  // Require the Bearer authentication scheme.
  const tokenMatch =
    typeof authHeader === 'string'
      ? authHeader.match(/^Bearer\s+(\S+)$/i)
      : null;

  if (!tokenMatch) {
    return res.status(401).json({
      error: 'Authentication token is required.'
    });
  }

  const token = tokenMatch[1];

  let decoded;

  try {
    // Verify the token signature and expiration.
    decoded = jwt.verify(
      token,
      process.env.JWT_SECRET,
      { algorithms: ['HS256'] }
    );
  } catch (error) {
    return res.status(401).json({
      error: 'Invalid or expired authentication token.'
    });
  }

  // Extract the database user ID from the JWT subject.
  const userId = Number(decoded.sub);

  if (
    !Number.isSafeInteger(userId) ||
    userId <= 0 ||
    userId > 2147483647
  ) {
    return res.status(401).json({
      error: 'Invalid authentication token.'
    });
  }

  try {
    // Confirm that the authenticated account still exists.
    const result = await pool.query(
      `SELECT id, username
       FROM users
       WHERE id = $1`,
      [userId]
    );

    if (result.rowCount !== 1) {
      return res.status(401).json({
        error: 'The authenticated account no longer exists.'
      });
    }

    // Make the verified user available to the route.
    req.authenticatedUser = {
      id: Number(result.rows[0].id),
      username: result.rows[0].username
    };

    // Continue to the requested endpoint.
    return next();
  } catch (error) {
    // Pass database errors to the central error handler.
    return next(error);
  }
}

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

// Retrieve paginated chat history for authenticated users.
app.get('/api/messages/history', authenticateToken, async (req, res, next) => {
  // Use 50 messages per page when no limit is provided.
  const rawLimit = req.query.limit === undefined
    ? '50'
    : req.query.limit;

  // Ensure the limit is a positive whole-number string.
  if (
    typeof rawLimit !== 'string' ||
    !/^\d+$/.test(rawLimit)
  ) {
    return res.status(400).json({
      error: 'History limit must be a valid whole number.'
    });
  }

  // Convert the requested limit from a string to a number.
  const limit = Number(rawLimit);

  // Enforce the permitted page size.
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
    return res.status(400).json({
      error: 'History limit must be between 1 and 100.'
    });
  }

  // Start without a cursor; this retrieves the latest messages.
  let cursor = null;

  // Read and validate the cursor when the client supplies one.
  if (req.query.cursor !== undefined) {
    const rawCursor = req.query.cursor;

    // Reject unexpected cursor types and excessively long values.
    if (
      typeof rawCursor !== 'string' ||
      rawCursor.length > 512
    ) {
      return res.status(400).json({
        error: 'Invalid message history cursor.'
      });
    }

    // Convert the cursor JSON string into a JavaScript object.
    try {
      cursor = JSON.parse(rawCursor);
    } catch (error) {
      return res.status(400).json({
        error: 'Invalid message history cursor.'
      });
    }
  }

  // Retrieve the requested page from PostgreSQL.
  try {
    const history = await messageStore.getMessageHistory(
      limit,
      cursor
    );

    // Return the messages and pagination information.
    return res.json(history);
  } catch (error) {
    // Invalid cursors are client errors, not server errors.
    if (error.message === 'Invalid message history cursor.') {
      return res.status(400).json({
        error: error.message
      });
    }

    // Record unexpected errors on the server.
    console.error('Message history retrieval failed:', error.message);

    // Return a generic error without exposing database details.
    return res.status(500).json({
      error: 'Could not retrieve message history.'
    });
  }
});

// 6. Register a new user.
app.post('/api/register', registerLimiter, async (req, res) => {
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
app.post('/api/login', loginLimiter, async (req, res) => {
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

    // Always perform a bcrypt comparison, even when the email is unknown.
    const passwordHash = user
      ? user.password_hash
      : DUMMY_PASSWORD_HASH;

    const matches = await bcrypt.compare(
      password,
      passwordHash
    );

    // Return the same response for unknown emails and wrong passwords.
    if (!user || !matches) {
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


/* 8. Handle malformed JSON, CORS rejections and unexpected errors. */
app.use((error, req, res, next) => {
  // Reject requests from origins not on the allowlist.
  if (error.message === 'Origin not allowed by CORS') {
    return res.status(403).json({
      error: 'Origin not allowed.'
    });
  }

  // Handle malformed JSON request bodies.
  if (
    error instanceof SyntaxError &&
    error.status === 400 &&
    'body' in error
  ) {
    return res.status(400).json({
      error: 'Request body contains invalid JSON.'
    });
  }

  // Log unexpected errors without exposing internal details.
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
