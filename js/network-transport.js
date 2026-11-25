// js/network-transport.js - Bus de Mensajería Asíncrona Conmutable
// Simula retardo en enlaces (latencia ms), fluctuación (jitter), tasa de descarte (drop-rate)
// y particiones de red bidireccionales / unidireccionales (Split-Brain)

export class NetworkTransport {
    constructor() {
        this.nodes = new Map(); // nodeId -> NodeInstance
        this.baseLatencyMs = 60; // ms base
        this.jitterMs = 20;     // fluctuación +/- ms
        this.dropRate = 0.0;    // 0.0 a 1.0 (0% - 100%)
        
        // Conjunto de enlaces particionados o cortados: 'nodeA<->nodeB'
        this.partitions = new Set();
        
        // Paquetes en tránsito para visualización en Canvas
        // { id, senderId, receiverId, payload, progress (0..1), durationMs, startTime, fromPos, toPos, status, color }
        this.inFlightPackets = [];
        this.listeners = [];
        this.packetCounter = 0;
    }

    /**
     * Registra un nodo en la red
     * @param {string} nodeId 
     * @param {any} nodeInstance 
     */
    registerNode(nodeId, nodeInstance) {
        this.nodes.set(nodeId, nodeInstance);
    }

    /**
     * Desregistra un nodo
     * @param {string} nodeId 
     */
    unregisterNode(nodeId) {
        this.nodes.delete(nodeId);
    }

    /**
     * Define partición de red entre dos nodos (o lista de nodos)
     * @param {string} nodeA 
     * @param {string} nodeB 
     * @param {boolean} isolated 
     */
    setLinkPartition(nodeA, nodeB, isolated = true) {
        const key = [nodeA, nodeB].sort().join('<->');
        if (isolated) {
            this.partitions.add(key);
        } else {
            this.partitions.delete(key);
        }
        this.emitNetworkEvent({
            type: 'PARTITION_CHANGED',
            link: key,
            isolated
        });
    }

    /**
     * Crea partición en grupos de partición (Split-Brain)
     * e.g., group1 = ['N1', 'N2'], group2 = ['N3', 'N4', 'N5']
     * @param {string[][]} groups 
     */
    setPartitionGroups(groups) {
        this.partitions.clear();
        for (let i = 0; i < groups.length; i++) {
            for (let j = i + 1; j < groups.length; j++) {
                const groupA = groups[i];
                const groupB = groups[j];
                for (const a of groupA) {
                    for (const b of groupB) {
                        const key = [a, b].sort().join('<->');
                        this.partitions.add(key);
                    }
                }
            }
        }
        this.emitNetworkEvent({
            type: 'GROUPS_PARTITIONED',
            groups,
            partitionsCount: this.partitions.size
        });
    }

    /**
     * Restablece la conectividad total
     */
    healNetwork() {
        this.partitions.clear();
        this.emitNetworkEvent({ type: 'NETWORK_HEALED' });
    }

    /**
     * Verifica si el enlace entre A y B está cortado
     * @param {string} nodeA 
     * @param {string} nodeB 
     * @returns {boolean}
     */
    isPartitioned(nodeA, nodeB) {
        if (nodeA === nodeB) return false;
        const key = [nodeA, nodeB].sort().join('<->');
        return this.partitions.has(key);
    }

    /**
     * Envía un mensaje RPC asíncrono a través de la red
     * @param {string} senderId 
     * @param {string} receiverId 
     * @param {any} message - Objeto con { type, term, ... }
     * @param {Object} options - { fromPos, toPos, color }
     */
    send(senderId, receiverId, message, options = {}) {
        const sender = this.nodes.get(senderId);
        const receiver = this.nodes.get(receiverId);

        // Si emisor o receptor no existen o están caídos
        if (!sender || !receiver) return;
        if (sender.isCrashed) return;

        this.packetCounter++;
        const packetId = `pkt-${Date.now()}-${this.packetCounter}`;

        // Cálculo de latencia con Jitter
        const jitter = (Math.random() * 2 - 1) * this.jitterMs;
        const latency = Math.max(10, Math.round(this.baseLatencyMs + jitter));

        // Validación de partición de red
        const partitioned = this.isPartitioned(senderId, receiverId);

        // Simulación de pérdida de paquetes por dropRate
        const droppedByRate = Math.random() < this.dropRate;

        const isDropped = partitioned || droppedByRate || receiver.isCrashed;

        // Registro de paquete en tránsito para animación UI
        const packet = {
            id: packetId,
            senderId,
            receiverId,
            payload: message,
            progress: 0,
            durationMs: latency,
            startTime: performance.now(),
            fromPos: options.fromPos || { x: 0, y: 0 },
            toPos: options.toPos || { x: 0, y: 0 },
            isDropped,
            dropReason: partitioned ? 'PARTITION' : (droppedByRate ? 'DROP_RATE' : (receiver.isCrashed ? 'NODE_CRASHED' : null)),
            color: options.color || this.getPacketColor(message.type)
        };

        this.inFlightPackets.push(packet);

        // Notificar emisión
        this.emitNetworkEvent({
            type: 'PACKET_SENT',
            packet
        });

        // Programar entrega o descarte asíncrono
        setTimeout(() => {
            // Remover de paquetes en vuelo
            const idx = this.inFlightPackets.findIndex(p => p.id === packetId);
            if (idx !== -1) {
                this.inFlightPackets.splice(idx, 1);
            }

            if (isDropped) {
                this.emitNetworkEvent({
                    type: 'PACKET_DROPPED',
                    packet,
                    reason: packet.dropReason
                });
                return;
            }

            // Entrega al nodo receptor
            if (receiver && !receiver.isCrashed && typeof receiver.onReceiveMessage === 'function') {
                try {
                    receiver.onReceiveMessage(senderId, message, packetId);
                    this.emitNetworkEvent({
                        type: 'PACKET_DELIVERED',
                        packet
                    });
                } catch (err) {
                    console.error(`Error procesando mensaje en ${receiverId}:`, err);
                }
            }
        }, latency);
    }

    /**
     * Broadcast a todos los demás nodos
     * @param {string} senderId 
     * @param {any} message 
     * @param {Map<string, {x:number, y:number}>} nodePositions 
     */
    broadcast(senderId, message, nodePositions) {
        const senderPos = nodePositions ? nodePositions.get(senderId) : null;
        for (const [nodeId] of this.nodes) {
            if (nodeId === senderId) continue;
            const targetPos = nodePositions ? nodePositions.get(nodeId) : null;
            this.send(senderId, nodeId, message, {
                fromPos: senderPos,
                toPos: targetPos
            });
        }
    }

    /**
     * Asigna un color estético acorde al protocolo y tipo de RPC
     * @param {string} msgType 
     */
    getPacketColor(msgType) {
        switch (msgType) {
            case 'REQUEST_VOTE': return 'hsl(45, 95%, 60%)';       // Dorado neón
            case 'REQUEST_VOTE_RESP': return 'hsl(35, 90%, 55%)';
            case 'APPEND_ENTRIES': return 'hsl(185, 90%, 55%)';     // Cyan neón
            case 'APPEND_ENTRIES_RESP': return 'hsl(160, 85%, 50%)';// Esmeralda
            case 'PAXOS_PREPARE': return 'hsl(275, 85%, 65%)';      // Púrpura
            case 'PAXOS_PROMISE': return 'hsl(290, 80%, 60%)';
            case 'PAXOS_ACCEPT': return 'hsl(215, 90%, 60%)';       // Azul zafiro
            case 'PAXOS_ACCEPTED': return 'hsl(200, 85%, 55%)';
            case 'PBFT_PRE_PREPARE': return 'hsl(330, 85%, 65%)';   // Magenta
            case 'PBFT_PREPARE': return 'hsl(15, 90%, 60%)';       // Naranja coral
            case 'PBFT_COMMIT': return 'hsl(140, 80%, 55%)';       // Verde quórum
            default: return 'hsl(210, 80%, 65%)';
        }
    }

    /**
     * Suscripción a eventos de red
     * @param {Function} cb 
     */
    subscribe(cb) {
        this.listeners.push(cb);
        return () => {
            this.listeners = this.listeners.filter(l => l !== cb);
        };
    }

    emitNetworkEvent(event) {
        this.listeners.forEach(cb => cb(event));
    }
}
