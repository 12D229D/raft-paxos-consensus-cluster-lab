# Raft, Multi-Paxos & Byzantine Consensus Cluster Lab

> **Simulador interactivo formal de algoritmos de consenso distribuido, ordenación causal y tolerancia a fallos.**  
> Desarrollado bajo los principios y fundamentos del **Premio Edsger W. Dijkstra en Cómputo Distribuido (PODC/DISC)**.

---

## 📌 Visión General

En los sistemas distribuidos modernos, lograr que múltiples computadoras independientes acuerden un estado unificado en presencia de caídas de nodos, retardos de red impredecibles o ataques bizantinos es uno de los problemas más complejos de la informática teórica (Imposibilidad de FLP - Fischer, Lynch, Paterson, 1985).

Este laboratorio interactivo proporciona una plataforma visual de alta fidelidad para experimentar en tiempo real con:
1. **Raft (Diego Ongaro & John Ousterhout, Stanford 2014)**: Elección de líder determinista, replicación de bitácoras (*Replicated State Machine*) y quórum de mayoría simple.
2. **Multi-Paxos (Leslie Lamport, 1998)**: Propuestas por slots de log, promesas y transiciones de fase *Prepare/Accept/Learn*.
3. **PBFT - Practical Byzantine Fault Tolerance (Miguel Castro & Barbara Liskov, OSDI 1999)**: Consenso bajo fallos bizantinos arbitrarios con un quórum de $3f + 1$ réplicas.
4. **Relojes Lógicos de Lamport & Relojes Vectoriales (1978)**: Seguimiento formal de la relación *happens-before* ($a \to b$) sin sincronización de relojes físicos globales.
5. **Write-Ahead Log (WAL) persistente en SQLite**: Integración declarativa vía [`schema.sql`](./schema.sql).

---

## 🏛️ Invariantes de Seguridad Formales (*Safety & Liveness*)

El laboratorio valida rigurosamente los siguientes invariantes:

### 1. Invariantes de Raft:
- **Election Safety**: Como máximo un líder puede ser elegido por término dado.
- **Leader Append-Only**: Un líder jamás sobreescribe ni trunca sus propias entradas; únicamente añade nuevas entradas al final de su bitácora.
- **Log Matching Property**: Si dos bitácoras contienen una entrada con el mismo índice y término, entonces son idénticas en todas las entradas desde el índice 1 hasta ese punto.
- **Leader Completeness**: Si una entrada de bitácora es comprometida (*committed*) en un término dado, estará presente en las bitácoras de los líderes de todos los términos superiores.
- **State Machine Safety**: Si un servidor ha aplicado una entrada de bitácora en un índice determinado a su máquina de estados, ningún otro servidor aplicará jamás una entrada diferente para ese mismo índice.

### 2. Quórums en PBFT:
Para tolerar $f$ nodos bizantinos o maliciosos, el sistema exige un clúster de tamaño:
$$N \ge 3f + 1$$
El quórum necesario para validar certificados de *Prepared* y *Committed* es:
$$Q = 2f + 1$$

---

## 🧪 Inyector de Caos y Escenarios de Prueba

1. **Partición de Red (*Split-Brain*)**:
   - Activa el botón **"Partir Clúster"** para segmentar los nodos en $\{N_1, N_2\}$ vs $\{N_3, N_4, N_5\}$.
   - El subconjunto minoritario no puede alcanzar quórum ($2/5 < 3$), suspendiendo las confirmaciones. El subconjunto mayoritario continúa progresando ($3/5 \ge 3$).
2. **Latencia & Pérdida de Paquetes**:
   - Ajusta los deslizadores de retardo base (10 ms - 500 ms) y drop rate (0% - 80%) para observar la degradación de *heartbeats* y re-elecciones de candidatos.
3. **Colapso de Nodos (*Crash-Fault*)**:
   - Selecciona cualquier nodo en el canvas interactivo y pulsa **"Colapsar / Levantar"**. Si colapsas al líder actual, los seguidores iniciarán una nueva elección tras agotar su *election timeout* aleatorio (150-300 ms).
4. **Modo Bizantino**:
   - Degrada un nodo a comportamiento bizantino; enviará información contradictoria o corromperá entradas de bitácora simulando fallos bizantinos.

---

## 📂 Estructura de Archivos

```text
raft-paxos-consensus-cluster-lab/
├── index.html                  # Interfaz semántica con 1 único <h1>, canvas y paneles de control
├── css/
│   └── style.css               # Sistema de diseño HSL Cyber-Distributed y animaciones de paquetes RPC
├── js/
│   ├── app.js                  # Orquestador del laboratorio, eventos de UI y renderizado a 60 FPS
│   ├── consensus-raft.js       # Máquina de estados finitos determinista de Raft
│   ├── consensus-paxos-pbft.js # Motores comparativos de Multi-Paxos y PBFT
│   ├── network-transport.js    # Bus de mensajería con retardos, jitter y particiones
│   ├── vector-clocks.js        # Relojes lógicos de Lamport y relojes vectoriales causales
│   └── db.js                   # Persistencia relacional SQLite del WAL y snapshots
├── schema.sql                  # Contrato declarativo de base de datos relacional SQLite
├── README.md                   # Documentación técnica, invariantes y fundamentos de Dijkstra
└── plan.md                     # Bitácora de desarrollo y especificación arquitectónica
```

---

## 🚀 Ejecución Local

Dado que la aplicación está construida enteramente en Vanilla JavaScript estándar (ES Modules), HTML5 Canvas y CSS3:
1. Abre `index.html` en cualquier navegador web moderno que soporte ES Modules (Chrome, Edge, Firefox, Safari).
2. Para una experiencia óptima con módulos locales, puedes servir la carpeta mediante cualquier servidor estático:
   ```bash
   npx serve .
   # o bien:
   python -m http.server 8000
   ```
