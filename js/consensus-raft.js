// js/consensus-raft.js - Máquina de Estados Finitos de Raft (FSM)
// Implementa formalmente el algoritmo de consenso de Diego Ongaro & John Ousterhout (Stanford, 2014)
// Roles: FOLLOWER, CANDIDATE, LEADER
// Invariantes: Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety

import { VectorClock } from './vector-clocks.js';

export const RaftRole = {
    FOLLOWER: 'FOLLOWER',
    CANDIDATE: 'CANDIDATE',
    LEADER: 'LEADER'
};

export class RaftNode {
    /**
     * @param {string} id - e.g., 'N1'
     * @param {string[]} clusterNodeIds - Lista completa de IDs del clúster
     * @param {import('./network-transport.js').NetworkTransport} transport 
     * @param {import('./db.js').ClusterDatabase} db 
     */
    constructor(id, clusterNodeIds, transport, db) {
        this.id = id;
        this.clusterNodeIds = [...clusterNodeIds];
        this.transport = transport;
        this.db = db;

        // Estado persistente en todos los servidores
        this.currentTerm = 0;
        this.votedFor = null;
        // log entries: [{ index, term, command, key, val, committed }] (1-based index)
        this.log = [];

        // Estado volátil en todos los servidores
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.role = RaftRole.FOLLOWER;

        // Estado volátil en Líderes (se reinicializa tras cada elección)
        this.nextIndex = new Map();  // nodeId -> index of next log entry to send
        this.matchIndex = new Map(); // nodeId -> highest log entry known to be replicated

        // Máquina de estados replicada (Key-Value Store)
        this.stateMachine = new Map();

        // Control de fallos / Caos
        this.isCrashed = false;
        this.isByzantine = false; // Modo Bizantino para pruebas comparativas

        // Reloj vectorial causal
        this.vectorClock = new VectorClock(this.id, this.clusterNodeIds);

        // Temporizadores (Election timeout & Heartbeat timer)
        this.electionTimeoutMs = this.getRandomElectionTimeout();
        this.heartbeatIntervalMs = 80; // Heartbeats cada 80ms
        this.electionTimer = null;
        this.heartbeatTimer = null;
        this.lastHeartbeatTime = Date.now();

        // Quórum de votos durante candidatura
        this.votesReceived = new Set();

        // Iniciar temporizador de elección
        this.resetElectionTimer();
    }

    /**
     * Retorna timeout aleatorio entre 150ms y 300ms (Regla Raft)
     */
    getRandomElectionTimeout() {
        return Math.floor(Math.random() * 150) + 150;
    }

    /**
     * Resetea el reloj de elección de líder
     */
    resetElectionTimer() {
        if (this.electionTimer) clearTimeout(this.electionTimer);
        this.electionTimeoutMs = this.getRandomElectionTimeout();
        this.lastHeartbeatTime = Date.now();

        if (this.isCrashed) return;

        this.electionTimer = setTimeout(() => {
            this.handleElectionTimeout();
        }, this.electionTimeoutMs);
    }

    /**
     * Detiene los temporizadores activos
     */
    stopTimers() {
        if (this.electionTimer) clearTimeout(this.electionTimer);
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    }

    /**
     * Manejador de timeout: Conviértete en Candidato e inicia elección
     */
    handleElectionTimeout() {
        if (this.isCrashed || this.role === RaftRole.LEADER) return;

        this.becomeCandidate();
    }

    /**
     * Transición a Candidato:
     * - Incrementar currentTerm
     * - Votar por sí mismo
     * - Resetear election timer
     * - Enviar RequestVote RPC a todos los pares
     */
    becomeCandidate() {
        this.role = RaftRole.CANDIDATE;
        this.currentTerm += 1;
        this.votedFor = this.id;
        this.votesReceived.clear();
        this.votesReceived.add(this.id); // Voto propio

        this.vectorClock.tickLocalEvent();
        this.resetElectionTimer();

        // Si es un clúster de 1 solo nodo, se proclama líder de inmediato
        if (this.votesReceived.size >= this.getQuorumSize()) {
            this.becomeLeader();
            return;
        }

        const lastLogIndex = this.getLastLogIndex();
        const lastLogTerm = this.getLastLogTerm();

        // Enviar RequestVote RPC a todos los nodos
        for (const peerId of this.clusterNodeIds) {
            if (peerId === this.id) continue;

            const voteRequest = {
                type: 'REQUEST_VOTE',
                term: this.currentTerm,
                candidateId: this.id,
                lastLogIndex,
                lastLogTerm,
                lamportTs: this.vectorClock.lamportTime,
                vectorClock: this.vectorClock.clock
            };

            this.transport.send(this.id, peerId, voteRequest);
        }
    }

    /**
     * Transición a Líder:
     * - Inicializar nextIndex y matchIndex
     * - Detener election timer
     * - Iniciar latidos periódicos (AppendEntries vacíos)
     */
    becomeLeader() {
        this.role = RaftRole.LEADER;
        if (this.electionTimer) clearTimeout(this.electionTimer);

        const lastLogIndex = this.getLastLogIndex();
        for (const peerId of this.clusterNodeIds) {
            if (peerId === this.id) continue;
            this.nextIndex.set(peerId, lastLogIndex + 1);
            this.matchIndex.set(peerId, 0);
        }

        // Heartbeats inmediatos
        this.sendHeartbeats();

        // Programar heartbeats periódicos
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = setInterval(() => {
            if (this.role === RaftRole.LEADER && !this.isCrashed) {
                this.sendHeartbeats();
            }
        }, this.heartbeatIntervalMs);
    }

    /**
     * Transición a Seguidor (Follower)
     */
    becomeFollower(term) {
        this.role = RaftRole.FOLLOWER;
        if (term > this.currentTerm) {
            this.currentTerm = term;
            this.votedFor = null;
        }
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
        this.resetElectionTimer();
    }

    /**
     * Tamaño mínimo para alcanzar Quórum de mayoría: floor(N/2) + 1
     */
    getQuorumSize() {
        return Math.floor(this.clusterNodeIds.length / 2) + 1;
    }

    getLastLogIndex() {
        return this.log.length > 0 ? this.log[this.log.length - 1].index : 0;
    }

    getLastLogTerm() {
        return this.log.length > 0 ? this.log[this.log.length - 1].term : 0;
    }

    /**
     * Cliente emite comando al Líder (ej: SET x=10)
     * @param {string} command 
     * @param {string} key 
     * @param {string} val 
     */
    executeClientCommand(command, key, val) {
        if (this.role !== RaftRole.LEADER) {
            return {
                success: false,
                reason: 'NOT_LEADER',
                leaderHint: this.votedFor
            };
        }

        this.vectorClock.tickLocalEvent();

        const newIndex = this.getLastLogIndex() + 1;
        const entry = {
            index: newIndex,
            term: this.currentTerm,
            command,
            key,
            val,
            committed: false
        };

        this.log.push(entry);
        this.db.appendWalEntry(this.id, newIndex, this.currentTerm, command, key, val, 0);

        // Replicar de inmediato a los seguidores
        this.sendAppendEntriesToAll();

        return {
            success: true,
            index: newIndex,
            term: this.currentTerm
        };
    }

    /**
     * Envía latidos (heartbeats) sin entradas de log a todos los seguidores
     */
    sendHeartbeats() {
        this.sendAppendEntriesToAll(true);
    }

    /**
     * Envía AppendEntries RPC a todos los pares
     * @param {boolean} isHeartbeatOnly 
     */
    sendAppendEntriesToAll(isHeartbeatOnly = false) {
        if (this.role !== RaftRole.LEADER || this.isCrashed) return;

        for (const peerId of this.clusterNodeIds) {
            if (peerId === this.id) continue;

            const prevLogIndex = (this.nextIndex.get(peerId) || 1) - 1;
            let prevLogTerm = 0;
            if (prevLogIndex > 0 && prevLogIndex <= this.log.length) {
                prevLogTerm = this.log[prevLogIndex - 1].term;
            }

            // Entradas a enviar desde nextIndex
            let entriesToSend = [];
            if (!isHeartbeatOnly && this.log.length >= (this.nextIndex.get(peerId) || 1)) {
                entriesToSend = this.log.slice(prevLogIndex);
            }

            // Comportamiento bizantino: Corromper valores o términos si está marcado
            if (this.isByzantine && Math.random() < 0.4) {
                entriesToSend = entriesToSend.map(e => ({
                    ...e,
                    val: 'CORRUPTED_BYZANTINE_' + Math.floor(Math.random() * 9999)
                }));
            }

            const appendRpc = {
                type: 'APPEND_ENTRIES',
                term: this.currentTerm,
                leaderId: this.id,
                prevLogIndex,
                prevLogTerm,
                entries: entriesToSend,
                leaderCommit: this.commitIndex,
                lamportTs: this.vectorClock.lamportTime,
                vectorClock: this.vectorClock.clock
            };

            this.transport.send(this.id, peerId, appendRpc);
        }
    }

    /**
     * Dispatcher principal de mensajes recibidos por la red
     * @param {string} senderId 
     * @param {any} msg 
     */
    onReceiveMessage(senderId, msg) {
        if (this.isCrashed) return;

        // Actualización de reloj causal
        this.vectorClock.updateOnReceive(msg.lamportTs, msg.vectorClock);

        // Regla general de Raft: Si el término remoto es mayor, actualizarse a Seguidor
        if (msg.term > this.currentTerm) {
            this.becomeFollower(msg.term);
        }

        switch (msg.type) {
            case 'REQUEST_VOTE':
                this.handleRequestVote(senderId, msg);
                break;
            case 'REQUEST_VOTE_RESP':
                this.handleRequestVoteResponse(senderId, msg);
                break;
            case 'APPEND_ENTRIES':
                this.handleAppendEntries(senderId, msg);
                break;
            case 'APPEND_ENTRIES_RESP':
                this.handleAppendEntriesResponse(senderId, msg);
                break;
            default:
                break;
        }
    }

    /**
     * Maneja RPC RequestVote entrante
     */
    handleRequestVote(senderId, msg) {
        let voteGranted = false;

        // 1. Si term < currentTerm, rechazar voto
        if (msg.term < this.currentTerm) {
            voteGranted = false;
        } else {
            // 2. Si votedFor es null o es el mismo candidato...
            const canVote = (this.votedFor === null || this.votedFor === msg.candidateId);

            // Invariante de seguridad de Raft: El log del candidato debe estar tan actualizado como el propio
            // Un log es más actualizado si tiene mayor término en su última entrada, o a igual término es más largo.
            const myLastIndex = this.getLastLogIndex();
            const myLastTerm = this.getLastLogTerm();

            const isCandidateUpToDate = 
                (msg.lastLogTerm > myLastTerm) ||
                (msg.lastLogTerm === myLastTerm && msg.lastLogIndex >= myLastIndex);

            if (canVote && isCandidateUpToDate) {
                voteGranted = true;
                this.votedFor = msg.candidateId;
                this.resetElectionTimer(); // Reiniciar timer si concede voto
            }
        }

        const resp = {
            type: 'REQUEST_VOTE_RESP',
            term: this.currentTerm,
            voteGranted,
            candidateId: msg.candidateId,
            lamportTs: this.vectorClock.lamportTime,
            vectorClock: this.vectorClock.clock
        };

        this.transport.send(this.id, senderId, resp);
    }

    /**
     * Maneja respuesta de RequestVote
     */
    handleRequestVoteResponse(senderId, msg) {
        if (this.role !== RaftRole.CANDIDATE || msg.term !== this.currentTerm) {
            return;
        }

        if (msg.voteGranted) {
            this.votesReceived.add(senderId);
            // Verificar quórum de mayoría
            if (this.votesReceived.size >= this.getQuorumSize()) {
                this.becomeLeader();
            }
        }
    }

    /**
     * Maneja RPC AppendEntries entrante (Log Replication & Heartbeats)
     */
    handleAppendEntries(senderId, msg) {
        let success = false;
        let matchIndex = 0;

        // 1. Si term < currentTerm, rechazar
        if (msg.term < this.currentTerm) {
            success = false;
        } else {
            // Reconocer líder y resetear timer
            this.role = RaftRole.FOLLOWER;
            this.resetElectionTimer();

            // 2. Comprobar que el log contenga una entrada en prevLogIndex que coincida con prevLogTerm
            if (msg.prevLogIndex > 0) {
                if (this.log.length < msg.prevLogIndex) {
                    success = false; // El log es más corto
                } else if (this.log[msg.prevLogIndex - 1].term !== msg.prevLogTerm) {
                    success = false; // Conflicto de término en esa posición
                    // Truncar la entrada conflictiva y todas las subsiguientes
                    this.log = this.log.slice(0, msg.prevLogIndex - 1);
                    this.db.truncateWalFrom(this.id, msg.prevLogIndex);
                } else {
                    success = true;
                }
            } else {
                success = true; // prevLogIndex es 0 (inicio de bitácora)
            }

            // 3. Si coincide, anexar nuevas entradas que no estén ya presentes
            if (success && msg.entries && msg.entries.length > 0) {
                for (let i = 0; i < msg.entries.length; i++) {
                    const entry = msg.entries[i];
                    const existingEntry = this.log[entry.index - 1];

                    if (existingEntry) {
                        if (existingEntry.term !== entry.term) {
                            // Conflicto: truncar desde aquí
                            this.log = this.log.slice(0, entry.index - 1);
                            this.db.truncateWalFrom(this.id, entry.index);
                            this.log.push(entry);
                            this.db.appendWalEntry(this.id, entry.index, entry.term, entry.command, entry.key, entry.val, 0);
                        }
                    } else {
                        // Nueva entrada
                        this.log.push(entry);
                        this.db.appendWalEntry(this.id, entry.index, entry.term, entry.command, entry.key, entry.val, 0);
                    }
                }
            }

            // 4. Si leaderCommit > commitIndex, actualizar commitIndex = min(leaderCommit, index de última nueva entrada)
            if (success && msg.leaderCommit > this.commitIndex) {
                const lastNewIndex = this.getLastLogIndex();
                this.commitIndex = Math.min(msg.leaderCommit, lastNewIndex);
                this.applyEntriesToStateMachine();
                this.db.commitUpToIndex(this.id, this.commitIndex);
            }

            matchIndex = this.getLastLogIndex();
        }

        const resp = {
            type: 'APPEND_ENTRIES_RESP',
            term: this.currentTerm,
            success,
            matchIndex,
            lamportTs: this.vectorClock.lamportTime,
            vectorClock: this.vectorClock.clock
        };

        this.transport.send(this.id, senderId, resp);
    }

    /**
     * Maneja respuesta de AppendEntries en el Líder
     */
    handleAppendEntriesResponse(senderId, msg) {
        if (this.role !== RaftRole.LEADER || msg.term !== this.currentTerm) {
            return;
        }

        if (msg.success) {
            // Actualizar nextIndex y matchIndex para el seguidor
            this.nextIndex.set(senderId, msg.matchIndex + 1);
            this.matchIndex.set(senderId, msg.matchIndex);

            // Verificar si una entrada del término actual ha sido replicada en una mayoría de nodos
            this.checkAndAdvanceCommitIndex();
        } else {
            // Si falló por inconsistencia de log, decrementar nextIndex y reintentar
            const currentNext = this.nextIndex.get(senderId) || 1;
            if (currentNext > 1) {
                this.nextIndex.set(senderId, currentNext - 1);
            }
        }
    }

    /**
     * Invariante de seguridad de Raft:
     * Si existe un N tal que N > commitIndex, una mayoría de matchIndex[i] >= N, y log[N].term == currentTerm:
     * commitIndex = N
     */
    checkAndAdvanceCommitIndex() {
        const lastLogIndex = this.getLastLogIndex();

        for (let N = lastLogIndex; N > this.commitIndex; N--) {
            if (this.log[N - 1].term !== this.currentTerm) {
                // Raft nunca compromete entradas de términos anteriores contando réplicas directamente;
                // solo entradas del término actual mediante quórum (Regla de seguridad 5.4.2)
                continue;
            }

            let matchCount = 1; // El líder cuenta como 1 réplica
            for (const peerId of this.clusterNodeIds) {
                if (peerId === this.id) continue;
                if ((this.matchIndex.get(peerId) || 0) >= N) {
                    matchCount++;
                }
            }

            if (matchCount >= this.getQuorumSize()) {
                this.commitIndex = N;
                this.applyEntriesToStateMachine();
                this.db.commitUpToIndex(this.id, this.commitIndex);
                break;
            }
        }
    }

    /**
     * Aplica entradas comprometidas a la State Machine local
     */
    applyEntriesToStateMachine() {
        while (this.commitIndex > this.lastApplied) {
            this.lastApplied++;
            const entry = this.log[this.lastApplied - 1];
            if (entry) {
                entry.committed = true;
                if (entry.key && entry.val !== undefined) {
                    this.stateMachine.set(entry.key, entry.val);
                }
            }
        }
    }

    /**
     * Simula caída del nodo (Crash fault)
     */
    crash() {
        this.isCrashed = true;
        this.stopTimers();
    }

    /**
     * Recupera el nodo de una caída
     */
    recover() {
        this.isCrashed = false;
        this.role = RaftRole.FOLLOWER;
        this.resetElectionTimer();
    }
}
