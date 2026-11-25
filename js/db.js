// js/db.js - Persistencia Local SQLite / IndexedDB / Memory
// Almacena formalmente el Write-Ahead Log (WAL), el estado de consenso y snapshots
// Implementa la interfaz declarativa correspondiente a schema.sql

export class ClusterDatabase {
    constructor() {
        this.nodes = new Map();
        this.wal = []; // [{ id, nodeId, logIndex, term, command, commandKey, commandValue, isCommitted, createdAt }]
        this.paxosState = new Map(); // `${nodeId}:${slot}` -> state
        this.pbftState = new Map();  // `${nodeId}:${view}:${seq}` -> state
        this.snapshots = []; // [{ id, nodeId, lastIncludedIndex, lastIncludedTerm, stateJson, createdAt }]
        this.traces = [];    // [{ id, senderId, receiverId, protocol, msgType, lamportTs, vectorClock, status, ... }]
        this.walAutoIncrement = 1;
        this.storageKey = 'codex_consensus_cluster_db_v1';
        
        this.loadFromLocalStorage();
    }

    loadFromLocalStorage() {
        try {
            const raw = localStorage.getItem(this.storageKey);
            if (raw) {
                const data = JSON.parse(raw);
                if (Array.isArray(data.wal)) {
                    this.wal = data.wal;
                    this.walAutoIncrement = data.wal.length ? Math.max(...data.wal.map(e => e.id || 0)) + 1 : 1;
                }
                if (Array.isArray(data.snapshots)) {
                    this.snapshots = data.snapshots;
                }
            }
        } catch (e) {
            console.warn('No se pudo recuperar DB de LocalStorage:', e);
        }
    }

    persist() {
        try {
            const dump = {
                wal: this.wal.slice(-500), // Mantener últimas 500 entradas
                snapshots: this.snapshots.slice(-50)
            };
            localStorage.setItem(this.storageKey, JSON.stringify(dump));
        } catch (e) {
            // Ignorar cuota excedida
        }
    }

    /**
     * Registra o actualiza un nodo en la base de datos
     */
    upsertNode(nodeData) {
        this.nodes.set(nodeData.nodeId, {
            ...nodeData,
            updatedAt: new Date().toISOString()
        });
    }

    /**
     * Inserta una entrada en el Write-Ahead Log de un nodo
     */
    appendWalEntry(nodeId, logIndex, term, command, commandKey = null, commandValue = null, isCommitted = 0) {
        // Remover si ya existía entrada en ese índice por sobreescritura Raft (conflict resolution)
        this.wal = this.wal.filter(e => !(e.nodeId === nodeId && e.logIndex === logIndex));

        const entry = {
            id: this.walAutoIncrement++,
            nodeId,
            logIndex,
            term,
            command,
            commandKey,
            commandValue,
            isCommitted: isCommitted ? 1 : 0,
            createdAt: new Date().toISOString()
        };

        this.wal.push(entry);
        this.persist();
        return entry;
    }

    /**
     * Marca como comprometida (COMMITTED) una entrada en la bitácora
     */
    commitWalEntry(nodeId, logIndex) {
        const entry = this.wal.find(e => e.nodeId === nodeId && e.logIndex === logIndex);
        if (entry) {
            entry.isCommitted = 1;
            this.persist();
        }
    }

    /**
     * Compromete todas las entradas hasta cierto commitIndex
     */
    commitUpToIndex(nodeId, commitIndex) {
        let changed = false;
        this.wal.forEach(e => {
            if (e.nodeId === nodeId && e.logIndex <= commitIndex && !e.isCommitted) {
                e.isCommitted = 1;
                changed = true;
            }
        });
        if (changed) this.persist();
    }

    /**
     * Trunca entradas conflictivas a partir de determinado índice
     */
    truncateWalFrom(nodeId, fromIndex) {
        this.wal = this.wal.filter(e => !(e.nodeId === nodeId && e.logIndex >= fromIndex));
        this.persist();
    }

    /**
     * Obtiene el WAL completo de un nodo
     */
    getNodeWal(nodeId) {
        return this.wal
            .filter(e => e.nodeId === nodeId)
            .sort((a, b) => a.logIndex - b.logIndex);
    }

    /**
     * Guarda un Snapshot de la máquina de estados replicada
     */
    saveSnapshot(nodeId, lastIncludedIndex, lastIncludedTerm, stateObj) {
        const snap = {
            id: this.snapshots.length + 1,
            nodeId,
            lastIncludedIndex,
            lastIncludedTerm,
            stateJson: JSON.stringify(stateObj),
            createdAt: new Date().toISOString()
        };
        this.snapshots.push(snap);
        this.persist();
        return snap;
    }

    /**
     * Guarda traza de mensaje de red para trazabilidad de Lamport
     */
    recordMessageTrace(trace) {
        this.traces.push({
            id: this.traces.length + 1,
            ...trace,
            timestamp: Date.now()
        });
        // Mantener tope para memoria
        if (this.traces.length > 200) {
            this.traces.shift();
        }
    }

    /**
     * Reinicia toda la persistencia
     */
    clearDatabase() {
        this.nodes.clear();
        this.wal = [];
        this.paxosState.clear();
        this.pbftState.clear();
        this.snapshots = [];
        this.traces = [];
        this.walAutoIncrement = 1;
        localStorage.removeItem(this.storageKey);
    }

    /**
     * Exporta el estado a un archivo descargable SQL o JSON
     */
    exportSqlDump() {
        let sql = `-- Codex Raft & Paxos Consensus Lab SQL Dump\n-- Generado: ${new Date().toISOString()}\n\n`;
        this.wal.forEach(w => {
            sql += `INSERT INTO raft_wal_entries (node_id, log_index, term, command, command_key, command_value, is_committed) VALUES ('${w.nodeId}', ${w.logIndex}, ${w.term}, '${w.command}', ${w.commandKey ? `'${w.commandKey}'` : 'NULL'}, ${w.commandValue ? `'${w.commandValue}'` : 'NULL'}, ${w.isCommitted});\n`;
        });
        return sql;
    }
}
