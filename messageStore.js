
const pool = require('./db');

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const MAX_MESSAGE_LENGTH = 4000;
const MAX_USER_ID = 2147483647;
const VALID_NODES = ['A', 'B'];

function validateMessage(packet) {
  if (
    !packet ||
    typeof packet !== 'object' ||
    Array.isArray(packet)
  ) {
    throw new Error('A valid message packet is required.');
  }

  if (
    typeof packet.id !== 'string' ||
    !UUID_PATTERN.test(packet.id)
  ) {
    throw new Error('The message ID must be a valid UUID.');
  }

  const userId =
    typeof packet.senderUserId === 'number'
      ? packet.senderUserId
      : typeof packet.senderUserId === 'string' &&
          /^\d+$/.test(packet.senderUserId)
        ? Number(packet.senderUserId)
        : NaN;

  if (
    !Number.isSafeInteger(userId) ||
    userId <= 0 ||
    userId > MAX_USER_ID
  ) {
    throw new Error(
      'The sender user ID must be a valid positive integer.'
    );
  }

  if (
    typeof packet.username !== 'string' ||
    packet.username.trim().length === 0
  ) {
    throw new Error('The sender username is required.');
  }

  if (
    typeof packet.text !== 'string' ||
    packet.text.trim().length === 0
  ) {
    throw new Error('The message text cannot be empty.');
  }

  if (packet.text.length > MAX_MESSAGE_LENGTH) {
    throw new Error(
      `The message cannot exceed ${MAX_MESSAGE_LENGTH} characters.`
    );
  }

  if (!VALID_NODES.includes(packet.originNode)) {
    throw new Error('The message origin node must be A or B.');
  }

  return {
    ...packet,
    senderUserId: userId
  };
}

function validateDestination(originNode, destinationNode) {
  if (
    !VALID_NODES.includes(destinationNode) ||
    destinationNode === originNode
  ) {
    throw new Error('Invalid destination node.');
  }
}

function validateNode(nodeId) {
  if (!VALID_NODES.includes(nodeId)) {
    throw new Error('Node ID must be A or B.');
  }
}

function validateRetryCount(retryCount) {
  if (
    !Number.isSafeInteger(retryCount) ||
    retryCount < 0
  ) {
    throw new Error('Retry count must be a non-negative integer.');
  }
}

/**
 * Verify the registered sender and insert the message if new.
 * Reusing a message UUID with different content is rejected.
 */
async function insertOrVerifyMessage(client, packet) {
  const senderResult = await client.query(
    `SELECT username
     FROM users
     WHERE id = $1
     FOR SHARE`,
    [packet.senderUserId]
  );

  const sender = senderResult.rows[0];

  if (!sender || sender.username !== packet.username) {
    throw new Error(
      'The message sender does not match a registered user.'
    );
  }

  await client.query(
    `INSERT INTO chat_messages
       (id, sender_user_id, origin_node, message_text)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (id) DO NOTHING`,
    [
      packet.id,
      packet.senderUserId,
      packet.originNode,
      packet.text
    ]
  );

  const result = await client.query(
    `SELECT sender_user_id, origin_node, message_text
     FROM chat_messages
     WHERE id = $1
     FOR SHARE`,
    [packet.id]
  );

  const stored = result.rows[0];

  if (
    !stored ||
    Number(stored.sender_user_id) !== packet.senderUserId ||
    stored.origin_node !== packet.originNode ||
    stored.message_text !== packet.text
  ) {
    throw new Error(
      'Message ID conflicts with existing persisted message data.'
    );
  }
}

/**
 * Run a database transaction and safely roll it back on failure.
 */
async function withTransaction(operation) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const result = await operation(client);

    await client.query('COMMIT');

    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      console.error(
        'Failed to roll back database transaction:',
        rollbackError.message
      );
    }

    throw error;
  } finally {
    client.release();
  }
}

/**
 * Persist an outgoing message and its initial delivery state.
 * Existing delivery state is not reset on duplicate calls.
 */
async function persistOutgoingMessage(packet, destinationNode) {
  packet = validateMessage(packet);
  validateDestination(packet.originNode, destinationNode);

  return withTransaction(async (client) => {
    await insertOrVerifyMessage(client, packet);

    await client.query(
      `INSERT INTO message_node_receipts (message_id, node_id)
       VALUES ($1, $2)
       ON CONFLICT (message_id, node_id) DO NOTHING`,
      [packet.id, packet.originNode]
    );

    await client.query(
      `INSERT INTO message_delivery_state
         (message_id, destination_node, status)
       VALUES ($1, $2, 'pending')
       ON CONFLICT (message_id, destination_node) DO NOTHING`,
      [packet.id, destinationNode]
    );
  });
}

/**
 * Persist an incoming message before it is broadcast or acknowledged.
 * Returns true if this node records its receipt for the first time.
 */
async function persistIncomingMessage(packet, receivingNode) {
  packet = validateMessage(packet);
  validateDestination(packet.originNode, receivingNode);

  return withTransaction(async (client) => {
    await insertOrVerifyMessage(client, packet);

    const receipt = await client.query(
      `INSERT INTO message_node_receipts (message_id, node_id)
       VALUES ($1, $2)
       ON CONFLICT (message_id, node_id) DO NOTHING
       RETURNING message_id`,
      [packet.id, receivingNode]
    );

    return receipt.rowCount === 1;
  });
}

/**
 * Record an outgoing delivery attempt.
 */
async function markDeliveryAttempt(
  messageId,
  destinationNode,
  retryCount
) {
  if (
    typeof messageId !== 'string' ||
    !UUID_PATTERN.test(messageId)
  ) {
    throw new Error('Invalid message ID.');
  }

  validateNode(destinationNode);
  validateRetryCount(retryCount);

  const result = await pool.query(
    `UPDATE message_delivery_state
     SET retry_count = $3,
         last_attempt_at = CURRENT_TIMESTAMP,
         updated_at = CURRENT_TIMESTAMP,
         last_error = NULL
     WHERE message_id = $1
       AND destination_node = $2
       AND status = 'pending'
     RETURNING message_id`,
    [messageId, destinationNode, retryCount]
  );

  return result.rowCount === 1;
}

/**
 * Persist an ACK. A late ACK can recover a previously failed message.
 */

async function acknowledgeDelivery(messageId, destinationNode) {
  if (
    typeof messageId !== 'string' ||
    !UUID_PATTERN.test(messageId)
  ) {
    throw new Error('Invalid message ID.');
  }

  validateNode(destinationNode);

  const result = await pool.query(
    `UPDATE message_delivery_state
     SET status = 'acknowledged',
         acknowledged_at = CURRENT_TIMESTAMP,
         updated_at = CURRENT_TIMESTAMP,
         last_error = NULL
     WHERE message_id = $1
       AND destination_node = $2
       AND status <> 'acknowledged'
     RETURNING message_id`,
    [messageId, destinationNode]
  );

  // A new acknowledgement was successfully recorded.
  if (result.rowCount === 1) {
    return true;
  }

  // No update occurred. Check whether this is a duplicate ACK
  // for a message that was already acknowledged.
  const existing = await pool.query(
    `SELECT status
     FROM message_delivery_state
     WHERE message_id = $1
       AND destination_node = $2`,
    [messageId, destinationNode]
  );

  return (
    existing.rowCount === 1 &&
    existing.rows[0].status === 'acknowledged'
  );
}

/**
 * Mark an outgoing message as failed after its retry limit is reached.
 */
async function markDeliveryFailed(
  messageId,
  destinationNode,
  errorMessage = 'Maximum delivery retries exceeded.'
) {
  if (
    typeof messageId !== 'string' ||
    !UUID_PATTERN.test(messageId)
  ) {
    throw new Error('Invalid message ID.');
  }

  validateNode(destinationNode);

  if (typeof errorMessage !== 'string') {
    throw new Error('Delivery error must be a string.');
  }

  const result = await pool.query(
    `UPDATE message_delivery_state
     SET status = 'failed',
         updated_at = CURRENT_TIMESTAMP,
         last_error = $3
     WHERE message_id = $1
       AND destination_node = $2
       AND status = 'pending'
     RETURNING message_id`,
    [messageId, destinationNode, errorMessage.slice(0, 1000)]
  );

  return result.rowCount === 1;
}

/**
 * Retrieve pending outgoing deliveries in FIFO order.
 * Sender usernames come from the registered users table.
 */
async function getPendingDeliveries(destinationNode, limit = 1000) {
  validateNode(destinationNode);

  if (
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > 10000
  ) {
    throw new Error('Recovery limit must be between 1 and 10000.');
  }

  const result = await pool.query(
    `SELECT
       m.id,
       m.sender_user_id,
       u.username,
       m.origin_node,
       m.message_text,
       d.retry_count,
       d.status,
       d.created_at
     FROM message_delivery_state AS d
     JOIN chat_messages AS m
       ON m.id = d.message_id
     JOIN users AS u
       ON u.id = m.sender_user_id
     WHERE d.destination_node = $1
       AND d.status = 'pending'
     ORDER BY d.created_at ASC, m.id ASC
     LIMIT $2`,
    [destinationNode, limit]
  );

  return result.rows.map((row) => ({
    id: row.id,
    senderUserId: Number(row.sender_user_id),
    username: row.username,
    originNode: row.origin_node,
    text: row.message_text,
    retryCount: row.retry_count,
    status: row.status
  }));
}

/**
 * Requeue failed database deliveries for explicit recovery.
 * Acknowledged messages are never reset to pending.
 */
async function recoverFailedDeliveries(destinationNode) {
  validateNode(destinationNode);

  const result = await pool.query(
    `UPDATE message_delivery_state
     SET status = 'pending',
         retry_count = 0,
         updated_at = CURRENT_TIMESTAMP,
         last_error = NULL
     WHERE destination_node = $1
       AND status = 'failed'
     RETURNING message_id`,
    [destinationNode]
  );

  return result.rowCount;
}

/**
 * Recover one failed delivery without resetting other messages.
 * Acknowledged deliveries are never changed.
 */
async function recoverFailedDelivery(messageId, destinationNode) {
  if (
    typeof messageId !== 'string' ||
    !UUID_PATTERN.test(messageId)
  ) {
    throw new Error('Invalid message ID.');
  }

  validateNode(destinationNode);

  const result = await pool.query(
    `UPDATE message_delivery_state
     SET status = 'pending',
         retry_count = 0,
         updated_at = CURRENT_TIMESTAMP,
         last_error = NULL
     WHERE message_id = $1
       AND destination_node = $2
       AND status = 'failed'
     RETURNING message_id`,
    [messageId, destinationNode]
  );

  return result.rowCount === 1;
}

/**
 * Read the current status of an outgoing delivery.
 */
async function getDeliveryStatus(messageId, destinationNode) {
  if (
    typeof messageId !== 'string' ||
    !UUID_PATTERN.test(messageId)
  ) {
    throw new Error('Invalid message ID.');
  }

  validateNode(destinationNode);

  const result = await pool.query(
    `SELECT status
     FROM message_delivery_state
     WHERE message_id = $1
       AND destination_node = $2`,
    [messageId, destinationNode]
  );

  return result.rowCount === 1
    ? result.rows[0].status
    : null;
}

module.exports = {
  validateMessage,
  persistOutgoingMessage,
  persistIncomingMessage,
  markDeliveryAttempt,
  acknowledgeDelivery,
  markDeliveryFailed,
  getDeliveryStatus,
  getPendingDeliveries,
  recoverFailedDeliveries,
  recoverFailedDelivery
};
