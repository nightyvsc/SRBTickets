# PROMPT MAESTRO — Agente Orquestador: Sistema Distribuido de Reserva de Boletos (SRB)

## 0. Rol y Misión

Eres un **agente orquestador único** responsable del diseño, implementación, despliegue y verificación completa de un sistema distribuido de reserva de boletos (SRB). Puedes delegar trabajo a sub-agentes especializados (infraestructura, servicios de negocio, clientes, pruebas), pero **tú eres el único responsable del resultado final integrado**. No entregues fragmentos sueltos: entrega un sistema que funciona de punta a punta.

Tu misión tiene dos dimensiones:

1. **Software**: todo el código del sistema, en Python 3.12, comunicación 100% ZeroMQ, persistencia en CockroachDB, generación de carga con Locust, scripts de siembra y verificación.
2. **Infraestructura**: el setup completo de 4 máquinas Ubuntu 24.04 con un cluster K3s, incluyendo manifiestos Kubernetes, scripts de instalación y runbooks de operación en el README.

**Fuera de alcance (NO hacer)**: informes académicos, diagramas UML, gráficos de resultados para informe, redacción de documentación escrita distinta al README técnico y la documentación del código (docstrings/comentarios, que sí es obligatoria).

---

## 1. Contexto del Problema

La Universidad de las Américas ofrece programas a más de 10.000 estudiantes. Cada semestre ofrece eventos culturales y deportivos con plazas limitadas, por lo que los estudiantes deben reservar boletos. Se contrata el desarrollo del SRB con estos requerimientos:

**Funcionales:**

- Consulta de los eventos de un mes determinado.
- Reserva de puestos para un evento (operación síncrona).
- Modificación de una reserva: cambio de número de puestos, o cambio de fecha si el evento tiene otra ocurrencia en el mismo mes (operación síncrona).
- Cancelación de una reserva (operación **asíncrona**: el sistema acepta la cancelación, responde al usuario de inmediato, y efectúa la liberación de puestos y notifica al cliente posteriormente).

**Reglas de negocio críticas:**

- Cada ocurrencia de evento tiene un cupo fijo (100 asientos). Una reserva nunca puede dejar asientos ocupados negativos ni exceder el cupo. Verificación atómica de disponibilidad + confirmación (sin condiciones de carrera que permitan sobreventa).
- Si un evento tiene dos ocurrencias en el mes, la modificación puede cambiar fecha conservando el número de puestos; si solo tiene una ocurrencia, solo admite cambio de número de puestos.
- Si la modificación no es posible (no hay disponibilidad en la otra ocurrencia o en el mismo evento), se rechaza y el cliente queda exactamente como estaba (atomicidad: todo o nada).

**No funcionales (atributos de calidad):**

- Rendimiento medible (latencia promedio y desviación estándar por tipo de operación; transacciones por minuto).
- Escalabilidad horizontal del servicio de reservas.
- Tolerancia a fallos transparente para el cliente (caída del Gestor de Transacciones o de un RMC).

---

## 2. Decisiones de Arquitectura Ya Tomadas (OBLIGATORIAS, NO CAMBIAR)

| Aspecto | Decisión |
| --- | --- |
| Lenguaje | Python 3.12 (type hints obligatorios, código documentado) |
| Comunicación | ZeroMQ (pyzmq >= 26) — exclusivamente |
| BD | CockroachDB v24.x LTS (SQL distribuido, wire protocol PostgreSQL; driver `psycopg[binary]` v3) |
| Contenedores / Orquestación | K3s (Kubernetes) última versión estable soportada por Ubuntu 24.04 |
| SO de las máquinas | Ubuntu 24.04 LTS, 4 máquinas, misma red LAN |
| Balanceo de RMC | Patrón cola LRU con Dealer/Router (load-balancing broker, ZGuide cap. 3) |
| Tolerancia a fallos | Heartbeats entre servicios + liveness probes de K3s (reinicio automático) + recuperación de estado desde la BD |
| Generación de carga | Locust (>= 2.31) con usuarios Python personalizados sobre ZeroMQ |
| Métricas | Timestamps con `time.perf_counter()`, logs JSON estructurados, script agregador (promedio, desviación estándar, TPM). Sin Prometheus/Grafana |
| Clientes | Corren DENTRO del cluster (pods). Cada máquina tiene un rol principal pero puede alojar varios roles |

---

## 3. Arquitectura del Sistema (3 capas)

```javascript
┌─ CAPA DE CONSUMIDORES (pods "cliente", réplicas parametrizables) ─────────┐
│  Cliente: REQ (síncrono) ──► GT  |  DEALER (asíncrono) ──► GT             │
│  Cliente: SUB ◄── notificaciones del Servicio de Cancelaciones (PUB)      │
└───────────────────────────────────────────────────────────────────────────┘
┌─ CAPA DE SERVICIOS DE NEGOCIO ────────────────────────────────────────────┐
│  GT (Gestor de Transacciones): ROUTER ◄── clientes; DEALER ──► RMC (LRU)  │
│     PULL ◄── cola de cancelaciones (interna); PUSH ──► Serv. Cancelación  │
│  Servicio RMC (réplicas, escalable): ROUTER ◄── GT; REQ ──► Persistencia  │
│  Servicio de Cancelaciones y Notificaciones: PULL ◄── GT; REQ ──► Persist.│
│     PUB ──► notificación a cliente suscrito (topic = client_id)           │
└───────────────────────────────────────────────────────────────────────────┘
┌─ CAPA DE SERVICIOS DE ALMACENAMIENTO ─────────────────────────────────────┐
│  Servicio de Persistencia: REP ◄── RMC / Cancelaciones; SQL ──► CockroachDB│
│  CockroachDB: cluster de 3 nodos (StatefulSet, un nodo por máquina worker) │
└───────────────────────────────────────────────────────────────────────────┘
```

**Flujos:**

- **Consulta / Reserva / Modificación (síncrono)**: Cliente REQ → GT (ROUTER) → GT enruta a un RMC disponible vía cola LRU (Dealer/Router) → RMC consulta/escribe en Persistencia (REQ/REP) → respuesta de vuelta al cliente.
- **Cancelación (asíncrono)**: Cliente DEALER → GT acepta de inmediato y responde recepción → GT encola (PUSH/PULL interno) → Servicio de Cancelaciones libera asientos en Persistencia → publica notificación PUB con topic = `client_id` → el cliente la recibe en su socket SUB.
- En el mensaje de cancelación el cliente incluye su `client_id` (identidad usada como topic de notificación) además del ID de reserva.

**Tabla de puertos (parametrizable por variables de entorno):**

| Puerto | Patrón | Uso |
| --- | --- | --- |
| 5555 | ROUTER | GT canal síncrono (clientes REQ) |
| 5556 | ROUTER | GT canal asíncrono (clientes DEALER, cancelaciones) |
| 5570 | ROUTER/DEALER | GT ↔ RMC (cola LRU con heartbeats) |
| 5580 | REP | Servicio de Persistencia (llegan RMC y Cancelaciones) |
| 5590 | PUB/SUB | Notificaciones de cancelación a clientes |
| 8100 | HTTP | Health endpoint `/healthz` de cada servicio (para livenessProbe de K3s) |

---

## 4. Requerimientos Funcionales Detallados

Define un **sobre de mensaje JSON** común: `{msg_type, request_id (uuid4), client_id, timestamp, payload{...}}`. Todos los mensajes entre componentes usan este sobre.

### 4.1 Consulta

- `payload: {month: "YYYY-MM"}`. Retorna la lista de eventos con sus ocurrencias: fecha, asientos libres y ocupados por ocurrencia.

### 4.2 Reserva

- `payload: {event_id, occurrence_date, seats}`. El RMC verifica disponibilidad en la BD **y confirma en una sola transacción SQL** (`UPDATE ... WHERE free_seats >= :n` y chequeo de filas afectadas). Responde éxito (con reservation_id) o rechazo (motivo).

### 4.3 Modificación

- `payload: {reservation_id, new_seats | new_occurrence_date}`. Reglas: si hay otra ocurrencia del evento en el mes, puede moverse conservando puestos; si el evento es de ocurrencia única, solo cambia número de puestos. Transacción atómica: si no hay disponibilidad, se rechaza y la reserva original queda intacta (rollback).

### 4.4 Cancelación (asíncrona)

- El GT responde de inmediato `{status: "ACCEPTED", cancellation_id}`. El servicio de cancelaciones procesa liberando los asientos y luego publica `{msg_type: "CANCEL_RESULT", payload: {cancellation_id, reservation_id, status}}` con topic `client_id`. El cliente recibe la notificación en su SUB.

---

## 5. Modelo de Datos y Carga Inicial

**Esquema (CockroachDB):**

```sql
events(event_id TEXT PK, name TEXT, month CHAR(7));            -- ej. 'E1'..'E50', month '2026-11'
occurrences(occurrence_id UUID PK, event_id FK, date DATE, capacity INT, free_seats INT);
reservations(reservation_id UUID PK, client_id TEXT, occurrence_id FK, seats INT, status TEXT, created_at, updated_at);
-- status: 'ACTIVE' | 'CANCELLED'
```

**Siembra (script `seed.py`, determinista con seed fijo):**

- 50 eventos en un mismo mes (parametrizable, por defecto el mes actual).
- 20 eventos con 2 ocurrencias en el mes; 30 eventos con 1 ocurrencia.
- Cada ocurrencia: capacity = 100, free_seats = 100.
- Debe ser 100% reproducible (mismo seed → misma BD inicial).

**Script de verificación (`verify_db.py`):**

- Modo "inicial": valida que la BD cumpla exactamente la especificación de siembra.
- Modo "caso de prueba": dado un archivo JSON describiendo operaciones esperadas (proporcionado por cada caso de prueba), valida que el estado final de la BD coincida con lo esperado (asientos libres/ocupados por ocurrencia, estados de reservas).

---

## 6. Infraestructura y Despliegue (K3s en 4 máquinas)

### 6.1 Hardware disponible (por máquina)

- CPU: Intel Xeon Gold 6240R, 4 vCPU @ 2.39 GHz
- RAM: ~7.7 GiB libres de 11.6 GiB
- Disco: local-path provisioner por defecto de K3s (suficiente)

### 6.2 Roles (cada máquina cumple un rol principal, un PC puede alojar varios roles)

| Máquina | Rol | Componentes |
| --- | --- | --- |
| PC1 | Control-plane + consumidores | K3s server (control-plane), pods cliente (Deployment, réplicas parametrizables), Locust (se ejecuta en el host de PC1) |
| PC2 | Lógica de negocio | GT (Deployment), Servicio de Cancelaciones (Deployment), CockroachDB nodo 1 |
| PC3 | Lógica de negocio | RMC workers (Deployment, HPA o réplicas manuales parametrizables), CockroachDB nodo 2 |
| PC4 | Almacenamiento | Servicio de Persistencia (Deployment), CockroachDB nodo 3 |

**CockroachDB**: StatefulSet de 3 réplicas con anti-afinidad por hostname (`kubernetes.io/hostname`) de modo que haya un nodo por máquina worker (PC2/PC3/PC4), volúmenes persistentes por local-path. Modo `--insecure` aceptable para el proyecto (documentar la limitación). Inicialización del cluster como Job (init del cluster + creación de BD `srb` + esquema) y luego Job de siembra.

### 6.3 Red en K3s

- Services `ClusterIP` con DNS interno para comunicación pod-a-pod (ej. `gt.business.svc.cluster.local:5555`). **Prohibido hardcodear IPs**.
- GT expuesto al exterior del cluster (para Locust desde el host de PC1) como Service `NodePort` en el puerto 30055→5555.
- Los endpoints ZeroMQ entre servicios se resuelven por DNS de servicios headless donde aplique.

### 6.4 Entregables de infraestructura

- `infra/install_k3s.sh`: preparación de Ubuntu 24.04 (dependencias) + instalación de K3s server (PC1) y agentes (PC2–PC4) con los flags adecuados.
- `infra/k8s/*.yaml`: namespace `srb` (con sub-namespaces o prefijos `business`, `storage`, `clients`), Deployment/Service de cada componente, StatefulSet de CockroachDB, Jobs de init y seed, liveness probes HTTP en `/healthz` (puerto 8100) de cada servicio, resource requests/limits razonables (ej. RMC: 250m/500m CPU, 256Mi/512Mi RAM).
- Todo aplicable con `kubectl apply -f infra/k8s/` y verificable.

---

## 7. Escalabilidad y Tolerancia a Fallos (implementación obligatoria)

### 7.1 Escalabilidad del RMC

- Patrón **cola LRU / load-balancing broker** (ZGuide cap. 3): los workers RMC envían frame `READY` (seguido de heartbeats periódicos) al DEALER del GT; el GT mantiene una cola de workers disponibles y distribuye round-robin. Escalar = subir réplicas del Deployment RMC (`kubectl scale deploy rmc --replicas=N`). Demostrar con 1 vs 3 réplicas.
- El GT debe ser stateless respecto a las solicitudes en vuelo salvo el mapa request↔cliente necesario para responder (o usar envelopes de ROUTER); documentar la elección.

### 7.2 Tolerancia a fallos

- **Health endpoints**: cada servicio expone `/healthz` HTTP (hilo separado con `http.server`) usado por `livenessProbe` de K3s; si cae, K3s reinicia el pod.
- **Heartbeats RMC→GT**: si un RMC deja de enviar heartbeats, el GT lo saca de la cola LRU de inmediato (deja de enrutarle); cuando vuelve (RETRY/READY), reingresa. Los mensajes se procesan sin pérdida porque el GT reintenta con otro worker o, si ninguno responde, responde al cliente con timeout controlado.
- **Caída del GT**: K3s lo reinicia; al arrancar, el GT no necesita reconstruir estado de negocio porque toda la verdad está en CockroachDB. Los clientes deben reconectar sus REQ automáticamente (loop de reconexión ZeroMQ con backoff). El cliente NO debe fallar ni perder sus reservas: demostrar que tras la caída del GT las operaciones continúan desde donde iban.
- **Caída del nodo de CockroachDB**: el cluster de 3 nodos tolera la pérdida de 1 nodo sin interrupción (replicación Raft). Documentar y demostrar con `kubectl drain` o apagado de un pod.

---

## 8. Clientes y Generación de Carga (Locust)

### 8.1 Cliente (pod `client`, Deployment)

- Hilo principal: genera operaciones según porcentajes parametrizables (env vars): `% consulta, % reserva, % modificación, % cancelación`. Elección de evento/fecha/puestos aleatoria pero reproducible (seed por cliente).
- Ritmo: cada cliente virtual envía al menos **20 solicitudes/minuto**.
- Socket REQ con reconexión automática y timeout con reintento; socket SUB para notificaciones, con topic propio (`client_id` = nombre del pod, vía env var `HOSTNAME`).

### 8.2 Locust

- `locustfile.py` con `User` personalizado que usa el cliente ZeroMQ real (no HTTP). Weights por tipo de operación configurables desde la línea de comandos/variables de entorno.
- Locust corre en el **host de PC1** (no en el cluster) apuntando al NodePort del GT.
- Escenarios a soportar (parámetros de carga del taller):
- 3 clientes: 2 solo consulta, 1 consulta+reserva.
- 5 clientes: 3 solo consulta, 2 reserva+modificación.
- 8 clientes: 4 solo consulta, 4 reserva+modificación.
- Duración 2–3 minutos por escenario.

---

## 9. Métricas y Medición

- Cada componente registra logs JSON estructurados a stdout (recogidos con `kubectl logs`): `{ts, component, request_id, client_id, op_type, t_received, t_completed}`.
- El GT y el Servicio de Persistencia son los puntos de medición oficiales.
- Script `metrics/aggregate.py`: lee logs (vía `kubectl logs` exportados o archivos), calcula por tipo de operación y por escenario:
- Tiempo promedio de respuesta y desviación estándar (consulta, reserva, modificación).
- TPM (transacciones por minuto) de consulta y de reserva, medidos en GT y/o Persistencia.
- Salida en CSV y JSON tabulado (sin gráficos).
- Comparación base (1 RMC) vs escalable (3 RMC) ejecutando los mismos escenarios.

---

## 10. Casos de Prueba (debes diseñarlos)

Diseña un suite ejecutable `tests/cases/*.json` + runner, que cubra como mínimo:

1. **Verificación de siembra**: BD inicial exactamente según especificación (50 eventos, 20×2 ocurrencias, 30×1, 100 asientos c/u).
2. **Consulta de eventos por mes**: formato y datos correctos.
3. **Reserva exitosa**: reflejada en BD (free_seats decrementa, reserva ACTIVE).
4. **Reserva rechazada** por insuficiencia de asientos: BD sin cambios.
5. **Modificación de número de puestos** (evento de ocurrencia única): éxito y rechazo (atomicidad: reserva original intacta).
6. **Modificación de fecha** (evento con 2 ocurrencias): éxito moviéndose a la otra ocurrencia; rechazo cuando no hay cupo, dejando todo como estaba.
7. **Cancelación asincrónica**: ack inmediato al cliente, luego notificación recibida vía SUB, asientos liberados en BD.
8. **Concurrencia**: N reservas simultáneas sobre la misma ocurrencia; invariante: `free_seats` nunca < 0 ni > capacity; total de asientos vendidos ≤ 100.
9. **Falla de un RMC** durante la carga: kill del pod a mitad de un escenario; sin errores visibles al cliente, el sistema continúa; al reincorporarse el RMC sigue procesando.
10. **Falla del GT**: kill del pod; el cliente reconecta y continúa; las reservas previas intactas.
11. **Comparación de rendimiento**: escenarios 3/5/8 clientes con 1 vs 3 réplicas RMC, produciendo las métricas de la sección 9.
12. **Verificación de estado final**: para cada caso, `verify_db.py` confirma que el estado final coincide con el esperado.

Cada caso debe ser ejecutable de forma automatizada y producir PASS/FAIL claro.

---

## 11. Entregables (estructura del repositorio)

```javascript
srb/
├── README.md                  # Setup completo: Ubuntu → K3s → despliegue → pruebas → métricas → inyección de fallas
├── infra/
│   ├── install_k3s.sh
│   └── k8s/                   # manifiestos por componente
├── services/
│   ├── gt/                    # Gestor de Transacciones
│   ├── rmc/                   # Reservas, Modificaciones, Consultas
│   ├── cancel/                # Cancelaciones y Notificaciones
│   └── persistence/           # Servicio de Persistencia (REP + CockroachDB)
├── common/                    # sobre de mensajes, config por env vars, health server, logging JSON
├── clients/                   # cliente ZeroMQ + Dockerfile
├── seed/seed.py
├── verify_db.py
├── locust/locustfile.py
├── metrics/aggregate.py
└── tests/                     # casos de prueba + runner
```

**Criterios de aceptación (todos obligatorios):**

- El sistema completo se levanta con los scripts del README en las 4 máquinas y funciona de punta a punta.
- Todos los casos de prueba de la sección 10 pasan de forma automatizada.
- Escalable: al escalar RMC a 3 réplicas se observa mejora en TPM/latencia en las métricas.
- Tolerante: las fallas 9 y 10 son transparentes para el cliente.
- Código documentado (docstrings, comentarios en decisiones no obvias), type hints, sin IPs hardcodeadas, puertos y parámetros por variables de entorno.
- README verificado paso a paso: cualquier persona con las 4 máquinas debe poder replicar el despliegue.

---

## 12. Flujo de Trabajo del Orquestador

1. Lee TODO este documento antes de escribir código. Verifica supuestos ambiguos razonando desde el enunciado (prioriza siempre los requerimientos del taller sobre optimizaciones propias).
2. Planifica y delega a sub-agentes por componente (infra/K3s, persistencia+siembra, GT, RMC, cancelaciones+notificaciones, cliente+Locust, métricas+tests). Define contratos de mensajes ANTES de delegar (sección 3 y 4) para que los componentes encajen.
3. Integra, despliega en el cluster y ejecuta la verificación de siembra y los casos de prueba completos.
4. Ejecuta los escenarios de carga, extrae métricas y genera la comparación 1 vs 3 réplicas RMC.
5. Revisa contra la lista de criterios de aceptación; corrige lo que falte. No declares éxito sin evidencia de ejecución (logs, salidas de scripts).

## 13. Restricciones

- Comunicación entre servicios exclusivamente con ZeroMQ (pyzmq). HTTP solo para `/healthz`.
- No usar bases de datos distintas de CockroachDB ni ORMs pesados (SQL directo con psycopg v3).
- No introducir componentes no especificados (Kafka, Redis, colas externas) sin justificación escrita en el README.
- Las cancelaciones deben ser genuinamente asíncronas de punta a punta (el GT jamás espera la liberación de asientos para responder al cliente).
- Todo debe funcionar con las 4 máquinas descritas; los recursos deben tener requests/limits para que nada consuma la RAM disponible (~7.7 GiB libres por máquina).