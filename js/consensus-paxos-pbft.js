// js/consensus-paxos-pbft.js - Motores Comparativos de Consenso:
// 1. Multi-Paxos (Leslie Lamport, 1998): Proposer, Acceptor, Learner con optimización de Stable Leader
// 2. PBFT - Practical Byzantine Fault Tolerance (Castro & Liskov, 1999): Pre-Prepare, Prepare, Commit con quórum 3f+1

import { VectorClock } from './vector-clocks.js';

// ==========================================
// 1. MOTOR MULTI-PAXOS
// ==========================================

export class MultiPaxosNode {
    /**
     * @param {string} id 
     * @param {string[]} clusterNodeIds 
     * @param {import('./network-transport.js').NetworkTransport} transport 
     * @param {import('./db.js').ClusterDatabase} db 
     */
    constructor(id, clusterNodeIds, transport, db) {
        this.id = id;
        this.clusterNodeIds = [...clusterNodeIds];
        this.transport = transport;
        this.db = db;

        // Estado Acceptor por cada Slot de la bitácora
        // slots: Map<slotIndex, { minProposal, acceptedProposal, acceptedValue, isDecided }>
        this.slots = new Map();

        // Estado Proposer
        this.proposalNumber = this.getNodeNumericId(); // Base inicial única
        this.isLeader = (this.id === this.clusterNodeIds[0]); // Proposer estable inicial
        this.promisesReceived = new Map(); // slotIndex -> Set<{ from, maxAcceptedProp, maxVal }>
        this.acceptsReceived = new Map();   // slotIndex -> Set<from>

        // Máquina de estados
        this.stateMachine = new Map();
        this.isCrashed = false;
        this.vectorClock = new VectorClock(this.id, this.clusterNodeIds);
    }

    getNodeNumericId() {
        const num = parseInt(this.id.replace(/\D/g, ''), 10);
        return isNaN(num) ? 1 : num;
    }

    getQuorumSize() {
        return Math.floor(this.clusterNodeIds.length / 2) + 1;
    }

    getSlot(slotIndex) {
        if (!this.slots.has(slotIndex)) {
            this.slots.set(slotIndex, {
                minProposal: 0,
                acceptedProposal: 0,
                acceptedValue: null,
                isDecided: false
            });
        }
        return this.slots.get(slotIndex);
    }

    /**
     * Proponer un nuevo valor en el siguiente slot disponible
     */
    propose(key, val) {
        if (this.isCrashed) return;

        // Encontrar siguiente slot no decidido
        let slotIndex = 1;
        while (this.getSlot(slotIndex).isDecided) {
            slotIndex++;
        }

        this.vectorClock.tickLocalEvent();
        this.proposalNumber += this.clusterNodeIds.length; // Asegura identificador unívoco estrictamente creciente

        const proposalVal = { key, val };

        // Fase 1: Prepare (o si ya es líder estable reconocido, puede saltar a Fase 2 Accept)
        this.promisesReceived.set(slotIndex, new Map());
        this.acceptsReceived.set(slotIndex, new Set());

        const prepareMsg = {
            type: 'PAXOS_PREPARE',
            slotIndex,
            proposalNumber: this.proposalNumber,
            proposerId: this.id,
            lamportTs: this.vectorClock.lamportTime,
            vectorClock: this.vectorClock.clock
        };

        for (const peerId of this.clusterNodeIds) {
            this.transport.send(this.id, peerId, prepareMsg);
        }
    }

    onReceiveMessage(senderId, msg) {
        if (this.isCrashed) return;
        this.vectorClock.updateOnReceive(msg.lamportTs, msg.vectorClock);

        switch (msg.type) {
            case 'PAXOS_PREPARE':
                this.handlePrepare(senderId, msg);
                break;
            case 'PAXOS_PROMISE':
                this.handlePromise(senderId, msg);
                break;
            case 'PAXOS_ACCEPT':
                this.handleAccept(senderId, msg);
                break;
            case 'PAXOS_ACCEPTED':
                this.handleAccepted(senderId, msg);
                break;
            case 'PAXOS_LEARN':
                this.handleLearn(senderId, msg);
                break;
        }
    }

    handlePrepare(senderId, msg) {
        const slot = this.getSlot(msg.slotIndex);
        if (msg.proposalNumber > slot.minProposal) {
            slot.minProposal = msg.proposalNumber;
            const promiseMsg = {
                type: 'PAXOS_PROMISE',
                slotIndex: msg.slotIndex,
                proposalNumber: msg.proposalNumber,
                acceptedProposal: slot.acceptedProposal,
                acceptedValue: slot.acceptedValue,
                lamportTs: this.vectorClock.lamportTime,
                vectorClock: this.vectorClock.clock
            };
            this.transport.send(this.id, senderId, promiseMsg);
        }
    }

    handlePromise(senderId, msg) {
        const slotPromises = this.promisesReceived.get(msg.slotIndex);
        if (!slotPromises) return;

        slotPromises.set(senderId, {
            acceptedProposal: msg.acceptedProposal,
            acceptedValue: msg.acceptedValue
        });

        // Si se alcanza mayoría de promesas
        if (slotPromises.size >= this.getQuorumSize() && !this.acceptsReceived.get(msg.slotIndex)?.size) {
            // Regla de Paxos: Elegir el valor con mayor acceptedProposal reportado, o el propio si ninguno reportó valor
            let highestProp = 0;
            let valueToPropose = null;

            for (const [, p] of slotPromises) {
                if (p.acceptedProposal > highestProp && p.acceptedValue !== null) {
                    highestProp = p.acceptedProposal;
                    valueToPropose = p.acceptedValue;
                }
            }

            if (!valueToPropose) {
                valueToPropose = { key: 'status', val: 'PROPOSED_BY_' + this.id };
            }

            // Emitir Fase 2: Accept Request
            const acceptMsg = {
                type: 'PAXOS_ACCEPT',
                slotIndex: msg.slotIndex,
                proposalNumber: msg.proposalNumber,
                value: valueToPropose,
                lamportTs: this.vectorClock.lamportTime,
                vectorClock: this.vectorClock.clock
            };

            for (const peerId of this.clusterNodeIds) {
                this.transport.send(this.id, peerId, acceptMsg);
            }
        }
    }

    handleAccept(senderId, msg) {
        const slot = this.getSlot(msg.slotIndex);
        if (msg.proposalNumber >= slot.minProposal) {
            slot.minProposal = msg.proposalNumber;
            slot.acceptedProposal = msg.proposalNumber;
            slot.acceptedValue = msg.value;

            const acceptedMsg = {
                type: 'PAXOS_ACCEPTED',
                slotIndex: msg.slotIndex,
                proposalNumber: msg.proposalNumber,
                value: msg.value,
                lamportTs: this.vectorClock.lamportTime,
                vectorClock: this.vectorClock.clock
            };

            // Notificar al proponente (y learners)
            this.transport.send(this.id, senderId, acceptedMsg);
        }
    }

    handleAccepted(senderId, msg) {
        let accepts = this.acceptsReceived.get(msg.slotIndex);
        if (!accepts) {
            accepts = new Set();
            this.acceptsReceived.set(msg.slotIndex, accepts);
        }
        accepts.add(senderId);

        // Si se alcanza mayoría de aceptaciones: ¡Valor Decidido!
        if (accepts.size >= this.getQuorumSize()) {
            const slot = this.getSlot(msg.slotIndex);
            if (!slot.isDecided) {
                slot.isDecided = true;
                slot.acceptedValue = msg.value;
                if (msg.value && msg.value.key) {
                    this.stateMachine.set(msg.value.key, msg.value.val);
                }

                // Broadcast mensaje de aprendizaje (Learn)
                const learnMsg = {
                    type: 'PAXOS_LEARN',
                    slotIndex: msg.slotIndex,
                    value: msg.value,
                    lamportTs: this.vectorClock.lamportTime,
                    vectorClock: this.vectorClock.clock
                };
                for (const peerId of this.clusterNodeIds) {
                    if (peerId !== this.id) {
                        this.transport.send(this.id, peerId, learnMsg);
                    }
                }
            }
        }
    }

    handleLearn(senderId, msg) {
        const slot = this.getSlot(msg.slotIndex);
        slot.isDecided = true;
        slot.acceptedValue = msg.value;
        if (msg.value && msg.value.key) {
            this.stateMachine.set(msg.value.key, msg.value.val);
        }
    }
}

// ==========================================
// 2. MOTOR PBFT (Practical Byzantine Fault Tolerance)
// Tolera f fallos bizantinos con N >= 3f + 1 nodos
// Quórum requerido: 2f + 1 mensajes de validación
// ==========================================

export class PBFTNode {
    /**
     * @param {string} id 
     * @param {string[]} clusterNodeIds 
     * @param {import('./network-transport.js').NetworkTransport} transport 
     * @param {import('./db.js').ClusterDatabase} db 
     */
    constructor(id, clusterNodeIds, transport, db) {
        this.id = id;
        this.clusterNodeIds = [...clusterNodeIds];
        this.transport = transport;
        this.db = db;

        this.viewNumber = 0;
        this.sequenceNumber = 0;
        this.isByzantine = false;
        this.isCrashed = false;

        // Votos de Prepare y Commit recibidos por (view, seq)
        // key: `${view}:${seq}` -> Set<senderId>
        this.prepareVotes = new Map();
        this.commitVotes = new Map();
        this.preparedCertificates = new Set();
        this.committedCertificates = new Set();

        this.stateMachine = new Map();
        this.vectorClock = new VectorClock(this.id, this.clusterNodeIds);
    }

    /**
     * El número máximo de nodos bizantinos tolerables es f = floor((N - 1) / 3)
     */
    getMaxFaultyNodes() {
        return Math.floor((this.clusterNodeIds.length - 1) / 3);
    }

    /**
     * Quórum PBFT: 2f + 1 réplicas correctas
     */
    getPBFTQuorum() {
        const f = this.getMaxFaultyNodes();
        return 2 * f + 1;
    }

    isPrimary() {
        const primaryIndex = this.viewNumber % this.clusterNodeIds.length;
        return this.clusterNodeIds[primaryIndex] === this.id;
    }

    /**
     * Cliente envía petición al nodo Primario
     */
    clientRequest(key, val) {
        if (!this.isPrimary() || this.isCrashed) return;

        this.sequenceNumber++;
        this.vectorClock.tickLocalEvent();

        const digest = `digest-${key}-${val}-${this.sequenceNumber}`;
        const operation = { key, val };

        // 1. Fase Pre-Prepare (iniciada por el Primario)
        const prePrepareMsg = {
            type: 'PBFT_PRE_PREPARE',
            viewNumber: this.viewNumber,
            sequenceNumber: this.sequenceNumber,
            digest,
            operation,
            primaryId: this.id,
            lamportTs: this.vectorClock.lamportTime,
            vectorClock: this.vectorClock.clock
        };

        for (const peerId of this.clusterNodeIds) {
            this.transport.send(this.id, peerId, prePrepareMsg);
        }
    }

    onReceiveMessage(senderId, msg) {
        if (this.isCrashed) return;
        this.vectorClock.updateOnReceive(msg.lamportTs, msg.vectorClock);

        // Si el nodo es bizantino, puede alterar o descartar selectivamente
        if (this.isByzantine && Math.random() < 0.4) {
            // Nódulo bizantino: Envía mentira o mensaje forjado
            return;
        }

        switch (msg.type) {
            case 'PBFT_PRE_PREPARE':
                this.handlePrePrepare(senderId, msg);
                break;
            case 'PBFT_PREPARE':
                this.handlePrepare(senderId, msg);
                break;
            case 'PBFT_COMMIT':
                this.handleCommit(senderId, msg);
                break;
        }
    }

    handlePrePrepare(senderId, msg) {
        const key = `${msg.viewNumber}:${msg.sequenceNumber}`;
        if (!this.prepareVotes.has(key)) {
            this.prepareVotes.set(key, new Set());
        }

        // Si el mensaje viene del primario válido, hacer broadcast de PREPARE
        this.vectorClock.tickLocalEvent();
        const prepareMsg = {
            type: 'PBFT_PREPARE',
            viewNumber: msg.viewNumber,
            sequenceNumber: msg.sequenceNumber,
            digest: msg.digest,
            operation: msg.operation,
            senderId: this.id,
            lamportTs: this.vectorClock.lamportTime,
            vectorClock: this.vectorClock.clock
        };

        for (const peerId of this.clusterNodeIds) {
            this.transport.send(this.id, peerId, prepareMsg);
        }
    }

    handlePrepare(senderId, msg) {
        const key = `${msg.viewNumber}:${msg.sequenceNumber}`;
        if (!this.prepareVotes.has(key)) {
            this.prepareVotes.set(key, new Set());
        }
        const votes = this.prepareVotes.get(key);
        votes.add(senderId);

        // Si alcanza Quórum de Preparación (2f + 1), emite COMMIT
        if (votes.size >= this.getPBFTQuorum() && !this.preparedCertificates.has(key)) {
            this.preparedCertificates.add(key);
            this.vectorClock.tickLocalEvent();

            const commitMsg = {
                type: 'PBFT_COMMIT',
                viewNumber: msg.viewNumber,
                sequenceNumber: msg.sequenceNumber,
                digest: msg.digest,
                operation: msg.operation,
                senderId: this.id,
                lamportTs: this.vectorClock.lamportTime,
                vectorClock: this.vectorClock.clock
            };

            for (const peerId of this.clusterNodeIds) {
                this.transport.send(this.id, peerId, commitMsg);
            }
        }
    }

    handleCommit(senderId, msg) {
        const key = `${msg.viewNumber}:${msg.sequenceNumber}`;
        if (!this.commitVotes.has(key)) {
            this.commitVotes.set(key, new Set());
        }
        const votes = this.commitVotes.get(key);
        votes.add(senderId);

        // Si alcanza Quórum de Commit (2f + 1), ejecuta y compromete en la máquina de estados
        if (votes.size >= this.getPBFTQuorum() && !this.committedCertificates.has(key)) {
            this.committedCertificates.add(key);

            if (msg.operation && msg.operation.key) {
                this.stateMachine.set(msg.operation.key, msg.operation.val);
            }
        }
    }
}
