
BEGIN;

-- Track applied database migrations.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Store each chat message exactly once.
CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY,

    sender_user_id INTEGER NOT NULL
        REFERENCES users(id) ON DELETE RESTRICT,

    origin_node TEXT NOT NULL
        CHECK (origin_node IN ('A', 'B')),

    message_text TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chat_messages_nonempty_text
        CHECK (length(btrim(message_text)) > 0)
);

-- Record which node has processed each message.
-- Prevents duplicate broadcasts after retries or server restarts.
CREATE TABLE IF NOT EXISTS message_node_receipts (
    message_id UUID NOT NULL
        REFERENCES chat_messages(id) ON DELETE CASCADE,

    node_id TEXT NOT NULL
        CHECK (node_id IN ('A', 'B')),

    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (message_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_message_node_receipts_node
    ON message_node_receipts (node_id, received_at DESC);

-- Track delivery to the other node.
CREATE TABLE IF NOT EXISTS message_delivery_state (
    message_id UUID NOT NULL
        REFERENCES chat_messages(id) ON DELETE CASCADE,

    destination_node TEXT NOT NULL
        CHECK (destination_node IN ('A', 'B')),

    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'acknowledged', 'failed')),

    retry_count INTEGER NOT NULL DEFAULT 0
        CHECK (retry_count >= 0),

    last_attempt_at TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (message_id, destination_node)
);

-- Efficient chronological shared-history queries.
CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at
    ON chat_messages (created_at DESC, id DESC);

-- Efficient queries for a user's own messages.
CREATE INDEX IF NOT EXISTS idx_chat_messages_sender_created
    ON chat_messages (sender_user_id, created_at DESC);

-- Efficient recovery of pending deliveries after a restart.
CREATE INDEX IF NOT EXISTS idx_delivery_pending
    ON message_delivery_state (destination_node, created_at)
    WHERE status = 'pending';

-- Efficient inspection and recovery of failed deliveries.
CREATE INDEX IF NOT EXISTS idx_delivery_failed
    ON message_delivery_state (destination_node, created_at)
    WHERE status = 'failed';

INSERT INTO schema_migrations (version)
VALUES ('001_persistent_messaging')
ON CONFLICT (version) DO NOTHING;

COMMIT;
