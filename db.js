
require('dotenv').config();

const { Pool } = require('pg');

// Both WebSocket nodes must use the same PostgreSQL database
// as the authentication API.
if (!process.env.DATABASE_URL) {
  throw new Error(
    'DATABASE_URL is missing. Check your environment configuration.'
  );
}

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

module.exports = pool;
