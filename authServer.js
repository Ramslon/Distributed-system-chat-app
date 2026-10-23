
require('dotenv').config();

const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// 1. Validate the required configuration before starting.
if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 32) {
  throw new Error(
    'JWT_SECRET is missing or too short. Check your .env file.'
  );
}

const PORT = Number(process.env.PORT) || 3000;
const TOKEN_EXPIRY = process.env.JWT_EXPIRES_IN || '1h';

// 2. Create the Express application.
const app = express();


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

  // Respond to the browser's CORS preflight request.
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  next();
});


// Accept JSON request bodies up to 10 KB.
app.use(express.json({ limit: '10kb' }));

// 3. Open the SQLite database file in the project directory.
const dbPath = path.join(__dirname, 'chat.db');
const db = new sqlite3.Database(dbPath);

// 4. Create the users table if it doesn't already exist.
db.run(
  `CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL COLLATE NOCASE UNIQUE,
    email TEXT NOT NULL COLLATE NOCASE UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`,
  (error) => {
    if (error) {
      console.error('Could not create users table:', error.message);
      process.exitCode = 1;
      db.close();
      return;
    }

    console.log('Users table is ready.');

    // Start the API only after the database is ready.
    app.listen(PORT, '127.0.0.1', () => {
      console.log(`Authentication API running on http://127.0.0.1:${PORT}`);
    });
  }
);

// 5. A simple endpoint to check whether the API is running.
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// 6. Register a new user.
app.post('/api/register', (req, res) => {
  const { username, email, password } = req.body || {};

  // Validate that all required values are strings.
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

  // Keep usernames predictable and safe to display.
  if (!/^[a-zA-Z0-9_]{3,24}$/.test(cleanUsername)) {
    return res.status(400).json({
      error: 'Username must be 3–24 characters using letters, numbers or underscores.'
    });
  }

  // Basic email-format validation.
  if (
    cleanEmail.length > 254 ||
    !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)
  ) {
    return res.status(400).json({
      error: 'Enter a valid email address.'
    });
  }

  // Reject short or excessively long passwords.
  if (password.length < 8 || Buffer.byteLength(password, 'utf8') > 72) {
    return res.status(400).json({
      error: 'Password must be at least 8 characters and no more than 72 bytes.'
    });
  }

  // Hash the password; never store the original password.
  bcrypt.hash(password, 12, (hashError, passwordHash) => {
    if (hashError) {
      console.error('Password hashing failed:', hashError.message);

      return res.status(500).json({
        error: 'Could not create account.'
      });
    }

    // Use placeholders to safely insert user-provided values.
    db.run(
      `INSERT INTO users (username, email, password_hash)
       VALUES (?, ?, ?)`,
      [cleanUsername, cleanEmail, passwordHash],
      function (dbError) {
        if (dbError) {
          if (dbError.code === 'SQLITE_CONSTRAINT') {
            return res.status(409).json({
              error: 'That username or email is already registered.'
            });
          }

          console.error('Account creation failed:', dbError.message);

          return res.status(500).json({
            error: 'Could not create account.'
          });
        }

        return res.status(201).json({
          message: 'Account created successfully.',
          user: {
            id: this.lastID,
            username: cleanUsername,
            email: cleanEmail
          }
        });
      }
    );
  });
});

// 7. Log in with an email address and password.
app.post('/api/login', (req, res) => {
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

  // Find the account using its email address.
  db.get(
    `SELECT id, username, email, password_hash
     FROM users
     WHERE email = ?`,
    [cleanEmail],
    (dbError, user) => {
      if (dbError) {
        console.error('Login database error:', dbError.message);

        return res.status(500).json({
          error: 'Login is temporarily unavailable.'
        });
      }

      // Use the same response for unknown emails and wrong passwords.
      if (!user) {
        return res.status(401).json({
          error: 'Invalid email or password.'
        });
      }

      // Compare the supplied password against its stored hash.
      bcrypt.compare(password, user.password_hash, (compareError, matches) => {
        if (compareError) {
          console.error('Password comparison failed:', compareError.message);

          return res.status(500).json({
            error: 'Login is temporarily unavailable.'
          });
        }

        if (!matches) {
          return res.status(401).json({
            error: 'Invalid email or password.'
          });
        }

        // Issue a signed token for the authenticated user.
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
      });
    }
  );
});

// 8. Handle malformed JSON requests cleanly.
app.use((error, req, res, next) => {
  if (error instanceof SyntaxError && error.status === 400 && 'body' in error) {
    return res.status(400).json({
      error: 'Request body contains invalid JSON.'
    });
  }

  console.error('Unexpected API error:', error.message);

  return res.status(500).json({
    error: 'An unexpected server error occurred.'
  });
});
