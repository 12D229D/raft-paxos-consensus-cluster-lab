// js/app.js - Orquestador Principal del Laboratorio de Consenso
// Conecta los motores de consenso (Raft, Multi-Paxos, PBFT), la red simulada,
// la persistencia SQLite en db.js y el renderizado a 60 FPS en HTML5 Canvas.

import { RaftNode, RaftRole } from './consensus-raft.js';
import { MultiPaxosNode, PBFTNode } from './consensus-paxos-pbft.js';
import { NetworkTransport } from './network-transport.js';
import { ClusterDatabase } from './db.js';
import { VectorClock } from './vector-clocks.js';

class ClusterLabApp {
    constructor() {
        this.protocol = 'raft'; // 'raft' | 'paxos' | 'pbft'
        this.nodeCount = 5;
        this.nodeIds = ['N1', 'N2', 'N3', 'N4', 'N5'];
        
        this.transport = new NetworkTransport();
        this.db = new ClusterDatabase();
        this.nodes = new Map(); // nodeId -> NodeInstance

        // Posiciones espaciales en Canvas 2D
        this.nodePositions = new Map(); // nodeId -> { x, y, radius }
        this.selectedNodeId = 'N1';

        // Trazas de eventos para línea temporal de Lamport
        this.eventTimeline = [];

        // Canvas y contextos
        this.canvas = document.getElementById('cluster-canvas');
        this.ctx = this.canvas.getContext('2d');
        this.timelineCanvas = document.getElementById('timeline-canvas');
        this.timelineCtx = this.timelineCanvas.getContext('2d');

        this.initCluster();
        this.setupDOMEventListeners();
        this.setupNetworkObserver();
        this.resizeCanvases();
        window.addEventListener('resize', () => this.resizeCanvases());

        // Bucle de animación a 60 FPS
        this.lastFrameTime = performance.now();
        requestAnimationFrame((t) => this.renderLoop(t));

        this.logMessage('info', 'Laboratorio de Consenso Distribuido inicializado.');
    }

    /**
     * Inicializa o reinicia el clúster con el protocolo seleccionado
     */
    initCluster() {
        // Limpiar timers de nodos existentes
        for (const [, node] of this.nodes) {
            if (typeof node.stopTimers === 'function') node.stopTimers();
        }
        this.nodes.clear();

        this.nodeIds.forEach(id => {
            let instance;
            if (this.protocol === 'raft') {
                instance = new RaftNode(id, this.nodeIds, this.transport, this.db);
            } else if (this.protocol === 'paxos') {
                instance = new MultiPaxosNode(id, this.nodeIds, this.transport, this.db);
            } else if (this.protocol === 'pbft') {
                instance = new PBFTNode(id, this.nodeIds, this.transport, this.db);
            }
            this.nodes.set(id, instance);
            this.transport.registerNode(id, instance);
        });

        this.computeNodeLayout();
        this.updateInspectorUI();
    }

    /**
     * Calcula la disposición circular de los nodos en el Canvas
     */
    computeNodeLayout() {
        const rect = this.canvas.getBoundingClientRect();
        const centerX = this.canvas.width / 2;
        const centerY = this.canvas.height / 2;
        const radius = Math.min(centerX, centerY) * 0.65;

        this.nodeIds.forEach((id, idx) => {
            const angle = (idx / this.nodeIds.length) * (2 * Math.PI) - (Math.PI / 2);
            const x = centerX + radius * Math.cos(angle);
            const y = centerY + radius * Math.sin(angle);
            this.nodePositions.set(id, { x, y, radius: 26 });
        });
    }

    resizeCanvases() {
        const dpr = window.devicePixelRatio || 1;
        
        // Canvas del Clúster
        const rect = this.canvas.parentElement.getBoundingClientRect();
        this.canvas.width = rect.width * dpr;
        this.canvas.height = rect.height * dpr;
        this.ctx.scale(dpr, dpr);

        // Canvas de la Línea Temporal
        const timeRect = this.timelineCanvas.parentElement.getBoundingClientRect();
        this.timelineCanvas.width = (timeRect.width - 28) * dpr;
        this.timelineCanvas.height = 160 * dpr;
        this.timelineCtx.scale(dpr, dpr);

        this.computeNodeLayout();
    }

    /**
     * Configuración de eventos de red y registro causal
     */
    setupNetworkObserver() {
        this.transport.subscribe(event => {
            if (event.type === 'PACKET_SENT') {
                const pkt = event.packet;
                const senderPos = this.nodePositions.get(pkt.senderId);
                const receiverPos = this.nodePositions.get(pkt.receiverId);
                if (senderPos && receiverPos) {
                    pkt.fromPos = { ...senderPos };
                    pkt.toPos = { ...receiverPos };
                }

                // Registrar en la línea temporal de Lamport
                this.recordTimelineEvent(pkt.senderId, pkt.receiverId, pkt.payload.type, pkt.payload.lamportTs || 0);
            } else if (event.type === 'PACKET_DELIVERED') {
                this.updateInspectorUI();
            } else if (event.type === 'PARTITION_CHANGED' || event.type === 'NETWORK_HEALED') {
                this.updateInspectorUI();
            }
        });
    }

    /**
     * Registrar evento en la línea temporal espacio-temporal
     */
    recordTimelineEvent(senderId, receiverId, label, lamportTs) {
        this.eventTimeline.push({
            senderId,
            receiverId,
            label,
            lamportTs,
            time: performance.now()
        });

        if (this.eventTimeline.length > 50) {
            this.eventTimeline.shift();
        }
    }

    /**
     * Listeners de interacción del usuario (botones, sliders, particiones)
     */
    setupDOMEventListeners() {
        // Selector de Protocolo
        const protocolSelect = document.getElementById('protocol-select');
        protocolSelect.addEventListener('change', (e) => {
            this.protocol = e.target.value;
            this.logMessage('info', `Protocolo cambiado a: ${this.protocol.toUpperCase()}`);
            this.initCluster();
        });

        // Controles de Red: Latencia & Drop Rate
        const latencySlider = document.getElementById('latency-slider');
        const latencyVal = document.getElementById('latency-val');
        latencySlider.addEventListener('input', (e) => {
            const val = parseInt(e.target.value, 10);
            this.transport.baseLatencyMs = val;
            latencyVal.textContent = `${val} ms`;
        });

        const dropSlider = document.getElementById('drop-slider');
        const dropVal = document.getElementById('drop-val');
        dropSlider.addEventListener('input', (e) => {
            const val = parseInt(e.target.value, 10);
            this.transport.dropRate = val / 100;
            dropVal.textContent = `${val}%`;
        });

        // Inyector de Partición de Red (Split-Brain: N1,N2 vs N3,N4,N5)
        document.getElementById('btn-partition-split').addEventListener('click', () => {
            this.transport.setPartitionGroups([['N1', 'N2'], ['N3', 'N4', 'N5']]);
            this.logMessage('warn', '⚡ Split-Brain activo: Partición [N1, N2] <---> [N3, N4, N5]');
        });

        // Restaurar conectividad
        document.getElementById('btn-heal-network').addEventListener('click', () => {
            this.transport.healNetwork();
            this.logMessage('success', '🌐 Red restaurada por completo. Conectividad 100%.');
        });

        // Caída de Nodo Seleccionado (Crash / Recover)
        document.getElementById('btn-crash-node').addEventListener('click', () => {
            const node = this.nodes.get(this.selectedNodeId);
            if (node) {
                if (node.isCrashed) {
                    node.recover ? node.recover() : (node.isCrashed = false);
                    this.logMessage('info', `Nodo ${this.selectedNodeId} RECUPERADO.`);
                } else {
                    node.crash ? node.crash() : (node.isCrashed = true);
                    this.logMessage('error', `Nodo ${this.selectedNodeId} COLAPSADO (CRASH FAULT).`);
                }
                this.updateInspectorUI();
            }
        });

        // Degradar a Bizantino
        document.getElementById('btn-byzantine-toggle').addEventListener('click', () => {
            const node = this.nodes.get(this.selectedNodeId);
            if (node) {
                node.isByzantine = !node.isByzantine;
                const status = node.isByzantine ? 'BIZANTINO (Traicionero)' : 'HONESTO';
                this.logMessage('warn', `Nodo ${this.selectedNodeId} ahora es: ${status}`);
                this.updateInspectorUI();
            }
        });

        // Envío de Comandos de Cliente (SET key=val)
        document.getElementById('btn-send-command').addEventListener('click', () => {
            const keyInput = document.getElementById('cmd-key');
            const valInput = document.getElementById('cmd-val');
            const k = keyInput.value.trim() || 'counter';
            const v = valInput.value.trim() || Math.floor(Math.random() * 100).toString();

            this.dispatchClientCommand(k, v);
        });

        // Exportar SQLite Dump
        document.getElementById('btn-export-sql').addEventListener('click', () => {
            const dump = this.db.exportSqlDump();
            const blob = new Blob([dump], { type: 'text/sql' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `wal_consensus_dump_${Date.now()}.sql`;
            a.click();
            URL.revokeObjectURL(url);
            this.logMessage('success', 'Dump SQL exportado correctamente.');
        });

        // Interacción con Canvas (Click para seleccionar nodo)
        this.canvas.addEventListener('click', (e) => {
            const rect = this.canvas.getBoundingClientRect();
            const clickX = e.clientX - rect.left;
            const clickY = e.clientY - rect.top;

            for (const [nodeId, pos] of this.nodePositions) {
                const dist = Math.hypot(pos.x - clickX, pos.y - clickY);
                if (dist <= pos.radius + 6) {
                    this.selectedNodeId = nodeId;
                    this.updateInspectorUI();
                    this.logMessage('info', `Nodo seleccionado: ${nodeId}`);
                    break;
                }
            }
        });
    }

    /**
     * Envía comando de cliente al Líder (Raft), Proponente (Paxos) o Primario (PBFT)
     */
    dispatchClientCommand(k, v) {
        if (this.protocol === 'raft') {
            let leader = null;
            for (const [, n] of this.nodes) {
                if (n.role === RaftRole.LEADER && !n.isCrashed) {
                    leader = n;
                    break;
                }
            }
            if (leader) {
                const res = leader.executeClientCommand(`SET ${k}=${v}`, k, v);
                this.logMessage('info', `Comando SET ${k}=${v} enviado al Líder (${leader.id})`);
            } else {
                this.logMessage('error', 'No hay un Líder disponible para procesar la escritura.');
            }
        } else if (this.protocol === 'paxos') {
            const node = this.nodes.get(this.selectedNodeId) || this.nodes.get('N1');
            node.propose(k, v);
            this.logMessage('info', `Multi-Paxos: Propuesto SET ${k}=${v} en ${node.id}`);
        } else if (this.protocol === 'pbft') {
            let primary = null;
            for (const [, n] of this.nodes) {
                if (n.isPrimary && n.isPrimary() && !n.isCrashed) {
                    primary = n;
                    break;
                }
            }
            if (primary) {
                primary.clientRequest(k, v);
                this.logMessage('info', `PBFT: Petición cliente enviada al Primario (${primary.id})`);
            } else {
                this.logMessage('error', 'Primario PBFT no disponible.');
            }
        }
        this.updateInspectorUI();
    }

    /**
     * Render Loop a 60 FPS
     */
    renderLoop(timestamp) {
        const dt = timestamp - this.lastFrameTime;
        this.lastFrameTime = timestamp;

        this.drawClusterTopology();
        this.drawTimeline();

        requestAnimationFrame((t) => this.renderLoop(t));
    }

    /**
     * Dibuja los enlaces, particiones, halos neón y nodos en Canvas 2D
     */
    drawClusterTopology() {
        const ctx = this.ctx;
        const rect = this.canvas.getBoundingClientRect();
        ctx.clearRect(0, 0, rect.width, rect.height);

        // 1. Dibujar enlaces entre nodos (Malla completa)
        for (let i = 0; i < this.nodeIds.length; i++) {
            for (let j = i + 1; j < this.nodeIds.length; j++) {
                const idA = this.nodeIds[i];
                const idB = this.nodeIds[j];
                const posA = this.nodePositions.get(idA);
                const posB = this.nodePositions.get(idB);
                if (!posA || !posB) continue;

                const isCut = this.transport.isPartitioned(idA, idB);

                ctx.beginPath();
                ctx.moveTo(posA.x, posA.y);
                ctx.lineTo(posB.x, posB.y);

                if (isCut) {
                    ctx.strokeStyle = 'hsla(0, 75%, 55%, 0.6)';
                    ctx.setLineDash([6, 6]); // Línea punteada de partición
                    ctx.lineWidth = 2;
                } else {
                    ctx.strokeStyle = 'hsla(220, 25%, 25%, 0.7)';
                    ctx.setLineDash([]);
                    ctx.lineWidth = 1.5;
                }
                ctx.stroke();
                ctx.setLineDash([]);
            }
        }

        // 2. Dibujar paquetes RPC volando entre nodos
        const now = performance.now();
        this.transport.inFlightPackets.forEach(pkt => {
            const elapsed = now - pkt.startTime;
            pkt.progress = Math.min(1.0, elapsed / pkt.durationMs);

            const curX = pkt.fromPos.x + (pkt.toPos.x - pkt.fromPos.x) * pkt.progress;
            const curY = pkt.fromPos.y + (pkt.toPos.y - pkt.fromPos.y) * pkt.progress;

            // Halo del paquete
            ctx.beginPath();
            ctx.arc(curX, curY, 6, 0, Math.PI * 2);
            ctx.fillStyle = pkt.color;
            ctx.shadowColor = pkt.color;
            ctx.shadowBlur = 10;
            ctx.fill();
            ctx.shadowBlur = 0; // Reset

            // Línea de estela
            ctx.beginPath();
            ctx.moveTo(pkt.fromPos.x, pkt.fromPos.y);
            ctx.lineTo(curX, curY);
            ctx.strokeStyle = pkt.color.replace(')', ', 0.35)').replace('hsl', 'hsla');
            ctx.lineWidth = 2;
            ctx.stroke();
        });

        // 3. Dibujar nodos del Clúster
        this.nodeIds.forEach(id => {
            const node = this.nodes.get(id);
            const pos = this.nodePositions.get(id);
            if (!node || !pos) return;

            const isSelected = (id === this.selectedNodeId);

            // Obtener color según estado y rol
            let nodeColor = 'hsl(195, 90%, 55%)'; // Follower default
            let roleLabel = 'Follower';

            if (node.isCrashed) {
                nodeColor = 'hsl(0, 75%, 55%)';
                roleLabel = 'CRASHED';
            } else if (node.isByzantine) {
                nodeColor = 'hsl(30, 95%, 55%)';
                roleLabel = 'BYZANTINE';
            } else if (this.protocol === 'raft') {
                if (node.role === RaftRole.LEADER) {
                    nodeColor = 'hsl(45, 95%, 55%)';
                    roleLabel = 'LEADER';
                } else if (node.role === RaftRole.CANDIDATE) {
                    nodeColor = 'hsl(275, 85%, 65%)';
                    roleLabel = 'CANDIDATE';
                }
            } else if (this.protocol === 'paxos') {
                if (node.isLeader) {
                    nodeColor = 'hsl(45, 95%, 55%)';
                    roleLabel = 'PROPOSER*';
                } else {
                    roleLabel = 'ACCEPTOR';
                }
            } else if (this.protocol === 'pbft') {
                if (node.isPrimary && node.isPrimary()) {
                    nodeColor = 'hsl(45, 95%, 55%)';
                    roleLabel = 'PRIMARY';
                } else {
                    roleLabel = 'REPLICA';
                }
            }

            // Halo neón
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, pos.radius + (isSelected ? 7 : 4), 0, Math.PI * 2);
            ctx.fillStyle = nodeColor.replace(')', ', 0.2)').replace('hsl', 'hsla');
            ctx.shadowColor = nodeColor;
            ctx.shadowBlur = isSelected ? 22 : 12;
            ctx.fill();
            ctx.shadowBlur = 0;

            // Cuerpo del nodo
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, pos.radius, 0, Math.PI * 2);
            ctx.fillStyle = 'hsl(222, 42%, 13%)';
            ctx.fill();
            ctx.lineWidth = isSelected ? 3 : 2;
            ctx.strokeStyle = nodeColor;
            ctx.stroke();

            // Etiqueta de ID
            ctx.font = 'bold 13px Outfit, sans-serif';
            ctx.fillStyle = '#fff';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(id, pos.x, pos.y - 3);

            // Sub-etiqueta de rol
            ctx.font = '9px "JetBrains Mono", monospace';
            ctx.fillStyle = nodeColor;
            ctx.fillText(roleLabel, pos.x, pos.y + 12);

            // Término / Vista
            let termText = '';
            if (this.protocol === 'raft') termText = `T:${node.currentTerm}`;
            else if (this.protocol === 'paxos') termText = `P:${node.proposalNumber}`;
            else if (this.protocol === 'pbft') termText = `V:${node.viewNumber}`;

            ctx.font = '10px "JetBrains Mono", monospace';
            ctx.fillStyle = 'hsl(215, 20%, 70%)';
            ctx.fillText(termText, pos.x, pos.y + pos.radius + 14);
        });
    }

    /**
     * Dibuja el diagrama espacio-temporal de Lamport y eventos concurrentes
     */
    drawTimeline() {
        const ctx = this.timelineCtx;
        const rect = this.timelineCanvas.getBoundingClientRect();
        ctx.clearRect(0, 0, rect.width, rect.height);

        // Líneas de vida de cada nodo (horizontales)
        const rowHeight = rect.height / (this.nodeIds.length + 1);
        ctx.font = '10px "JetBrains Mono", monospace';

        this.nodeIds.forEach((id, idx) => {
            const y = rowHeight * (idx + 1);

            // Etiqueta de nodo
            ctx.fillStyle = (id === this.selectedNodeId) ? 'hsl(45, 95%, 55%)' : 'hsl(215, 20%, 70%)';
            ctx.textAlign = 'left';
            ctx.fillText(id, 8, y + 3);

            // Línea de vida
            ctx.beginPath();
            ctx.moveTo(36, y);
            ctx.lineTo(rect.width - 12, y);
            ctx.strokeStyle = 'hsla(220, 20%, 20%, 0.8)';
            ctx.lineWidth = 1;
            ctx.stroke();
        });

        // Eventos y flechas de paso de mensajes causales
        const now = performance.now();
        const maxTimeWindow = 10000; // 10 segundos de ventana

        this.eventTimeline.forEach(ev => {
            const age = now - ev.time;
            if (age > maxTimeWindow) return;

            // X proporcional
            const x = rect.width - 20 - ((age / maxTimeWindow) * (rect.width - 60));
            if (x < 36) return;

            const senderIdx = this.nodeIds.indexOf(ev.senderId);
            const receiverIdx = this.nodeIds.indexOf(ev.receiverId);
            if (senderIdx === -1 || receiverIdx === -1) return;

            const y1 = rowHeight * (senderIdx + 1);
            const y2 = rowHeight * (receiverIdx + 1);

            // Flecha causal
            ctx.beginPath();
            ctx.moveTo(x - 15, y1);
            ctx.lineTo(x, y2);
            ctx.strokeStyle = this.transport.getPacketColor(ev.label);
            ctx.lineWidth = 1.2;
            ctx.stroke();

            // Punto de evento con Lamport TS
            ctx.beginPath();
            ctx.arc(x, y2, 3, 0, Math.PI * 2);
            ctx.fillStyle = this.transport.getPacketColor(ev.label);
            ctx.fill();
        });
    }

    /**
     * Actualiza la tabla del WAL inspector, Key-Value store y estado del nodo seleccionado
     */
    updateInspectorUI() {
        const node = this.nodes.get(this.selectedNodeId);
        if (!node) return;

        // Título del inspector
        document.getElementById('selected-node-title').textContent = `Nodo ${this.selectedNodeId} (${this.protocol.toUpperCase()})`;
        
        // Reloj Vectorial
        const vClock = node.vectorClock ? VectorClock.formatVector(node.vectorClock.clock) : '[]';
        document.getElementById('vector-clock-display').textContent = vClock;
        document.getElementById('lamport-time-display').textContent = node.vectorClock ? node.vectorClock.lamportTime : 0;

        // WAL / Log Table
        const tbody = document.getElementById('wal-table-body');
        tbody.innerHTML = '';

        if (this.protocol === 'raft') {
            const walEntries = this.db.getNodeWal(this.selectedNodeId);
            walEntries.forEach(entry => {
                const tr = document.createElement('tr');
                const isCommitted = entry.isCommitted || (node.commitIndex >= entry.logIndex);
                tr.innerHTML = `
                    <td><strong>#${entry.logIndex}</strong></td>
                    <td>T:${entry.term}</td>
                    <td><code>${entry.command}</code></td>
                    <td>
                        <span class="${isCommitted ? 'badge-committed' : 'badge-uncommitted'}">
                            ${isCommitted ? 'COMMITTED' : 'UNCOMMITTED'}
                        </span>
                    </td>
                `;
                tbody.appendChild(tr);
            });
        }

        // Replicated State Machine (Key-Value)
        const kvContainer = document.getElementById('kv-store-container');
        kvContainer.innerHTML = '';
        if (node.stateMachine) {
            if (node.stateMachine.size === 0) {
                kvContainer.innerHTML = '<div style="color: var(--text-muted); font-size: 0.78rem;">(Almacén vacío)</div>';
            } else {
                for (const [k, v] of node.stateMachine) {
                    const div = document.createElement('div');
                    div.className = 'kv-item';
                    div.innerHTML = `<span><strong>${k}</strong></span><span>${v}</span>`;
                    kvContainer.appendChild(div);
                }
            }
        }
    }

    logMessage(type, text) {
        const stream = document.getElementById('log-stream');
        if (!stream) return;

        const time = new Date().toLocaleTimeString();
        const div = document.createElement('div');
        div.className = `log-entry ${type}`;
        div.textContent = `[${time}] ${text}`;
        stream.appendChild(div);
        stream.scrollTop = stream.scrollHeight;
    }
}

// Iniciar aplicación al cargar el DOM
window.addEventListener('DOMContentLoaded', () => {
    window.app = new ClusterLabApp();
});
