// js/vector-clocks.js - Motor de Relojes Lógicos de Lamport y Relojes Vectoriales
// Implementa formalmente la relación happens-before (a -> b) definida por Leslie Lamport (1978)
// y el cálculo de concurrencia e independencia causal entre nodos distribuidos.

export class VectorClock {
    /**
     * @param {string} nodeId - Identificador del nodo dueño
     * @param {string[]} allNodeIds - Lista completa de identificadores de nodos en el clúster
     */
    constructor(nodeId, allNodeIds = []) {
        this.nodeId = nodeId;
        this.lamportTime = 0;
        this.clock = {};
        
        // Inicializa el reloj vectorial en 0 para cada nodo conocido
        allNodeIds.forEach(id => {
            this.clock[id] = 0;
        });
        if (!this.clock[nodeId]) {
            this.clock[nodeId] = 0;
        }
    }

    /**
     * Asegura que el reloj conozca nuevos nodos agregados dinámicamente
     * @param {string[]} allNodeIds 
     */
    syncNodes(allNodeIds) {
        allNodeIds.forEach(id => {
            if (this.clock[id] === undefined) {
                this.clock[id] = 0;
            }
        });
    }

    /**
     * Incrementa los relojes ante un evento interno local o antes de emitir un mensaje
     * Regla de Lamport: L_i = L_i + 1
     * Regla de Reloj Vectorial: V_i[i] = V_i[i] + 1
     * @returns {{ lamport: number, vector: Object<string, number> }}
     */
    tickLocalEvent() {
        this.lamportTime += 1;
        this.clock[this.nodeId] = (this.clock[this.nodeId] || 0) + 1;
        return this.getSnapshot();
    }

    /**
     * Actualiza los relojes al recibir un mensaje remoto con timestamp causal
     * Regla de Lamport: L_i = max(L_i, L_msg) + 1
     * Regla de Vector: V_i[k] = max(V_i[k], V_msg[k]) para todo k, luego V_i[i] = V_i[i] + 1
     * @param {number} remoteLamport 
     * @param {Object<string, number>} remoteVector 
     * @returns {{ lamport: number, vector: Object<string, number> }}
     */
    updateOnReceive(remoteLamport, remoteVector) {
        // Regla Lamport
        this.lamportTime = Math.max(this.lamportTime, remoteLamport || 0) + 1;

        // Sincronizar dimensiones vectoriales
        if (remoteVector) {
            Object.keys(remoteVector).forEach(k => {
                if (this.clock[k] === undefined) {
                    this.clock[k] = 0;
                }
                this.clock[k] = Math.max(this.clock[k], remoteVector[k]);
            });
        }

        // Incremento del nodo receptor
        this.clock[this.nodeId] = (this.clock[this.nodeId] || 0) + 1;

        return this.getSnapshot();
    }

    /**
     * Retorna una copia inmutable del estado actual
     */
    getSnapshot() {
        return {
            lamport: this.lamportTime,
            vector: { ...this.clock }
        };
    }

    /**
     * Compara dos relojes vectoriales (V1 y V2) para determinar relación causal:
     * - 'BEFORE': V1 sucedió antes de V2 (V1 -> V2)
     * - 'AFTER': V2 sucedió antes de V1 (V2 -> V1)
     * - 'CONCURRENT': Eventos concurrentes e independientes (V1 || V2)
     * - 'EQUAL': Mismo instante vectorial
     * 
     * @param {Object<string, number>} v1 
     * @param {Object<string, number>} v2 
     * @returns {'BEFORE' | 'AFTER' | 'CONCURRENT' | 'EQUAL'}
     */
    static compareVectors(v1, v2) {
        const keys = Array.from(new Set([...Object.keys(v1 || {}), ...Object.keys(v2 || {})]));
        let hasLess = false;
        let hasGreater = false;

        for (const k of keys) {
            const val1 = v1[k] || 0;
            const val2 = v2[k] || 0;
            if (val1 < val2) hasLess = true;
            if (val1 > val2) hasGreater = true;
        }

        if (hasLess && !hasGreater) return 'BEFORE';
        if (hasGreater && !hasLess) return 'AFTER';
        if (!hasLess && !hasGreater) return 'EQUAL';
        return 'CONCURRENT';
    }

    /**
     * Formatea un reloj vectorial como texto matemático [N1:1, N2:3, ...]
     * @param {Object<string, number>} vector 
     */
    static formatVector(vector) {
        if (!vector) return '[]';
        return '[' + Object.entries(vector)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([k, v]) => `${k}:${v}`)
            .join(', ') + ']';
    }
}
