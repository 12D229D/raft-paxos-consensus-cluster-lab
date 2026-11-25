-- schema.sql - Raft, Multi-Paxos & Byzantine Consensus Cluster Lab
-- SQLite Declarative Schema for WAL, Node Consensus State, Snapshots & Event Traces

PRAGMA foreign_keys = ON;

-- 1. Tabla de Nodos del Clúster
CREATE TABLE IF NOT EXISTS cluster_nodes (
    node_id TEXT PRIMARY KEY,
    node_name TEXT NOT NULL,
    protocol TEXT NOT NULL DEFAULT 'raft' CHECK(protocol IN ('raft', 'paxos', 'pbft')),
    role TEXT NOT NULL DEFAULT 'FOLLOWER',
    status TEXT NOT NULL DEFAULT 'ALIVE' CHECK(status IN ('ALIVE', 'CRASHED', 'BYZANTINE', 'ISOLATED')),
    partition_id INTEGER DEFAULT 0,
    current_term INTEGER DEFAULT 0,
    voted_for TEXT,
    commit_index INTEGER DEFAULT 0,
    last_applied INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Write-Ahead Log (WAL) Replicado por Nodo
CREATE TABLE IF NOT EXISTS raft_wal_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    term INTEGER NOT NULL,
    command TEXT NOT NULL,
    command_key TEXT,
    command_value TEXT,
    is_committed INTEGER DEFAULT 0 CHECK(is_committed IN (0, 1)),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(node_id) REFERENCES cluster_nodes(node_id) ON DELETE CASCADE,
    UNIQUE(node_id, log_index)
);

-- 3. Estado de Consenso de Multi-Paxos
CREATE TABLE IF NOT EXISTS paxos_consensus_state (
    node_id TEXT NOT NULL,
    slot_index INTEGER NOT NULL,
    min_proposal INTEGER DEFAULT 0,
    accepted_proposal INTEGER DEFAULT 0,
    accepted_value TEXT,
    is_decided INTEGER DEFAULT 0 CHECK(is_decided IN (0, 1)),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(node_id, slot_index),
    FOREIGN KEY(node_id) REFERENCES cluster_nodes(node_id) ON DELETE CASCADE
);

-- 4. Estado de Consenso Bizantino (PBFT)
CREATE TABLE IF NOT EXISTS pbft_consensus_state (
    node_id TEXT NOT NULL,
    view_number INTEGER NOT NULL,
    sequence_number INTEGER NOT NULL,
    digest TEXT NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('PRE_PREPARE', 'PREPARE', 'COMMIT', 'COMMITTED')),
    prepared_votes_count INTEGER DEFAULT 0,
    commit_votes_count INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(node_id, view_number, sequence_number),
    FOREIGN KEY(node_id) REFERENCES cluster_nodes(node_id) ON DELETE CASCADE
);

-- 5. Snapshots de la Máquina de Estados (State Machine Store)
CREATE TABLE IF NOT EXISTS state_machine_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    last_included_index INTEGER NOT NULL,
    last_included_term INTEGER NOT NULL,
    state_json TEXT NOT NULL, -- Key-Value store serializado JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(node_id) REFERENCES cluster_nodes(node_id) ON DELETE CASCADE
);

-- 6. Trazas de Mensajes de Red y Relojes Vectoriales
CREATE TABLE IF NOT EXISTS message_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    sender_id TEXT NOT NULL,
    receiver_id TEXT NOT NULL,
    protocol TEXT NOT NULL,
    msg_type TEXT NOT NULL,
    term_or_view INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    lamport_ts INTEGER NOT NULL,
    vector_clock_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'DELIVERED' CHECK(status IN ('DELIVERED', 'DROPPED', 'DELAYED', 'CORRUPTED')),
    latency_ms REAL NOT NULL,
    sent_at REAL NOT NULL,
    delivered_at REAL
);

-- Índices para consultas de alta velocidad en simulación
CREATE INDEX IF NOT EXISTS idx_wal_node_index ON raft_wal_entries(node_id, log_index);
CREATE INDEX IF NOT EXISTS idx_wal_committed ON raft_wal_entries(is_committed);
CREATE INDEX IF NOT EXISTS idx_traces_sender ON message_traces(sender_id);
CREATE INDEX IF NOT EXISTS idx_traces_receiver ON message_traces(receiver_id);
