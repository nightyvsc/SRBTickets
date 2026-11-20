# Sistema Distribuido de Reserva de Boletos (SRB)

Sistema distribuido de alta disponibilidad, tolerancia a fallos y concurrencia estricta para la gestión y reserva de boletos de eventos culturales y deportivos en la Universidad de las Américas.

Diseñado con arquitectura de microservicios en 3 capas, comunicación 100% ZeroMQ (sin HTTP entre servicios internos), base de datos relacional distribuida CockroachDB v24.x LTS (protocolo PostgreSQL / Raft) y orquestación con K3s (Kubernetes ligero) sobre 4 máquinas virtuales Ubuntu 24.04 LTS.

---

## 1. Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. CAPA DE CONSUMIDORES (Pods en Kubernetes o Locust en host PC1)           │
│    • Cliente Sync: ZeroMQ REQ ──► GT:5555 (NodePort 30055)                  │
│    • Cliente Async: ZeroMQ DEALER ──► GT:5556                               │
│    • Cliente Notificaciones: ZeroMQ SUB ◄── Cancel Service:5590             │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│ 2. CAPA DE SERVICIOS DE NEGOCIO                                             │
│    • Gestor de Transacciones (GT):                                          │
│        - ROUTER :5555 (Front-end síncrono para clientes REQ)                │
│        - ROUTER :5556 (Front-end asíncrono para cancelaciones DEALER)       │
│        - ROUTER :5570 (Back-end LRU broker para RMC workers)                │
│        - PUSH :5575   (Envío asíncrono de cancelaciones a Cancel Service)   │
│    • Servicio RMC (Reservas, Modificaciones, Consultas):                    │
│        - DEALER ◄──► GT :5570 (Cola balanceada LRU + Heartbeats periódicos) │
│        - REQ    ───► Persistencia :5580                                     │
│        - Horizontalmente escalable (1 a N réplicas con HPA o manual)        │
│    • Servicio de Cancelaciones y Notificaciones:                            │
│        - PULL :5575  ◄── GT (Recepción asíncrona inmediata)                 │
│        - REQ :5580   ──► Persistencia (Liberación atómica de cupos en BD)   │
│        - PUB :5590   ──► Notificación al cliente con topic = client_id      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│ 3. CAPA DE ALMACENAMIENTO Y PERSISTENCIA                                    │
│    • Servicio de Persistencia:                                              │
│        - REP :5580 ◄── RMC y Cancelaciones (Lazy Pirate con reintentos)     │
│        - Pool de conexiones psycopg v3 hacia CockroachDB                    │
│        - Reintentos automáticos ante contención transaccional (código 40001)│
│    • CockroachDB Cluster (3 Nodos - StatefulSet):                           │
│        - Replicación Raft de 3 vías (tolera pérdida de 1 nodo sin caída)    │
│        - Consistencia serializable y aislamiento estricto                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Tabla de Puertos y Protocolos

| Puerto | Protocolo / Patrón | Origen | Destino | Propósito |
|---|---|---|---|---|
| **5555** | ZeroMQ ROUTER / REQ | Clientes / Locust | GT | Canal síncrono: QUERY, RESERVE, MODIFY (NodePort 30055) |
| **5556** | ZeroMQ ROUTER / DEALER | Clientes | GT | Canal asíncrono: Solicitud de CANCEL (ACK inmediato) |
| **5570** | ZeroMQ ROUTER / DEALER | RMC Workers | GT | Broker LRU con cola de workers activos y heartbeats |
| **5575** | ZeroMQ PUSH / PULL | GT | Cancel Service | Cola interna de despacho de cancelaciones |
| **5580** | ZeroMQ REP / REQ | RMC / Cancel | Persistencia | Ejecución atómica de queries y transacciones en BD |
| **5590** | ZeroMQ PUB / SUB | Cancel Service | Clientes | Notificaciones de cancelación completada (filtro: client_id) |
| **8100** | HTTP GET `/healthz` | Kubelet (K3s) | Todos los Pods | Liveness probes para reinicio automático |
| **26257** | TCP SQL / gRPC | Persistencia / Seed | CockroachDB | Motor SQL distribuido de CockroachDB |
| **8080** | HTTP | Administrador | CockroachDB | Consola web de CockroachDB |

---

## 2. Asignación de Máquinas Virtuales y Roles

El clúster K3s está distribuido en 4 máquinas virtuales con Ubuntu 24.04 LTS (usuario `estudiante`):

| Máquina | Dirección IP | Rol Principal | Componentes Asignados |
|---|---|---|---|
| **PC1** | `10.43.99.216` | **Control-plane + Consumidores** | K3s Server, Pods `client` (Deployment), Locust (host) |
| **PC2** | `10.43.100.175` | **Lógica de Negocio** | `gt` (Deployment), `cancel` (Deployment), `cockroachdb-0` |
| **PC3** | `10.43.99.199` | **Lógica de Negocio** | `rmc` (Deployment escalable 1..N), `cockroachdb-1` |
| **PC4** | `10.43.99.206` | **Almacenamiento** | `persistence` (Deployment), `cockroachdb-2` |

---

## 3. Guía Paso a Paso: Setup de las 4 Máquinas

### Paso 1: Clonar el Repositorio en PC1

Conéctate vía SSH a **PC1**:
```bash
ssh estudiante@10.43.99.216
```

Clona o descarga el repositorio en `/home/estudiante/SRBTickets`:
```bash
cd /home/estudiante
git clone <URL_DEL_REPO> SRBTickets || cp -r /home/juanma/SRBTickets .
cd SRBTickets
```

---

### Paso 2: Instalar K3s Server en PC1 (Control-Plane)

En **PC1**, ejecuta el script de instalación con rol `server`:
```bash
chmod +x infra/install_k3s.sh infra/build_images.sh
./infra/install_k3s.sh server
```

El script:
1. Instala dependencias y deshabilita swap.
2. Habilita el reenvío de paquetes IP (`net.ipv4.ip_forward = 1`).
3. Instala K3s Server fijando la IP `10.43.99.216`, deshabilitando Traefik y asignando la interfaz de red adecuada.
4. Muestra en pantalla el **Token de nodo** (`K3S_TOKEN`).

Copia el token generado:
```bash
K3S_TOKEN=$(sudo cat /var/lib/rancher/k3s/server/node-token)
echo "El token es: ${K3S_TOKEN}"
```

---

### Paso 3: Instalar K3s Agents en PC2, PC3 y PC4

Copia el script `install_k3s.sh` a las otras 3 máquinas o descárgalo en ellas:

#### En PC2 (10.43.100.175 - Business / CockroachDB-0):
```bash
ssh estudiante@10.43.100.175
./install_k3s.sh agent 10.43.99.216 <K3S_TOKEN> pc2 business
```

#### En PC3 (10.43.99.199 - Business / RMC / CockroachDB-1):
```bash
ssh estudiante@10.43.99.199
./install_k3s.sh agent 10.43.99.216 <K3S_TOKEN> pc3 business
```

#### En PC4 (10.43.99.206 - Storage / CockroachDB-2):
```bash
ssh estudiante@10.43.99.206
./install_k3s.sh agent 10.43.99.216 <K3S_TOKEN> pc4 storage
```

---

### Paso 4: Verificar Clúster y Aplicar Labels de Roles (en PC1)

Regresa a **PC1** y verifica que los 4 nodos estén en estado `Ready`:
```bash
kubectl get nodes -o wide
```

Salida esperada:
```
NAME   STATUS   ROLES                  AGE   VERSION        INTERNAL-IP
pc1    Ready    control-plane,master   2m    v1.30.x+k3s1   10.43.99.216
pc2    Ready    <none>                 1m    v1.30.x+k3s1   10.43.100.175
pc3    Ready    <none>                 1m    v1.30.x+k3s1   10.43.99.199
pc4    Ready    <none>                 1m    v1.30.x+k3s1   10.43.99.206
```

Aplica las etiquetas definitivas de afinidad:
```bash
./infra/install_k3s.sh labels
```

---

### Paso 5: Compilar e Importar Imágenes de Contenedores

En **PC1**, compila las imágenes Docker de los microservicios e impórtalas a containerd:
```bash
./infra/build_images.sh --distribute
```

Esto compilará:
- `srb-gt:latest`
- `srb-rmc:latest`
- `srb-cancel:latest`
- `srb-persistence:latest`
- `srb-client:latest`
- `srb-seed:latest`

Y las distribuirá a las máquinas worker para que K3s no necesite descargarlas de internet (`imagePullPolicy: IfNotPresent`).

---

### Paso 6: Desplegar CockroachDB y Esquema Relacional

Desde **PC1**:
```bash
# 1. Crear namespace srb
kubectl apply -f infra/k8s/00-namespace.yaml

# 2. Desplegar StatefulSet de CockroachDB (3 réplicas)
kubectl apply -f infra/k8s/01-cockroachdb.yaml

# 3. Monitorear inicio de pods de base de datos
kubectl get pods -n srb -w
```

Una vez que los pods `cockroachdb-0`, `cockroachdb-1` y `cockroachdb-2` estén en `Running`, el Job `cockroachdb-init` completará la inicialización del cluster y la creación de la base de datos `srb`.

Verifica el estado del cluster de BD:
```bash
kubectl exec -it -n srb cockroachdb-0 -- ./cockroach node status --insecure
```

---

### Paso 7: Ejecutar Siembra Inicial Determinista

Aplica el Job de siembra:
```bash
kubectl apply -f infra/k8s/06-seed-job.yaml
kubectl wait --for=condition=complete --timeout=60s job/seed-db -n srb
```

Verifica la siembra ejecutando el script de verificación desde PC1:
```bash
python3 verify_db.py --mode initial
```

Debe reportar `[PASS]` en todas las comprobaciones:
- 50 eventos en `2026-11` (E01..E50).
- 20 eventos con 2 ocurrencias (E01..E20).
- 30 eventos con 1 ocurrencia (E21..E50).
- 70 ocurrencias en total, cada una con 100 asientos de capacidad y 100 asientos libres.
- 0 reservas.

---

### Paso 8: Desplegar los Microservicios de Negocio y Almacenamiento

Desde **PC1**:
```bash
# Desplegar Persistencia en PC4
kubectl apply -f infra/k8s/02-persistence.yaml

# Desplegar Cancelaciones en PC2
kubectl apply -f infra/k8s/03-cancel.yaml

# Desplegar Gestor de Transacciones en PC2
kubectl apply -f infra/k8s/04-gt.yaml

# Desplegar RMC (1 réplica inicial) en PC3
kubectl apply -f infra/k8s/05-rmc.yaml
```

Verifica que todos los pods estén en estado `Running` y saludables:
```bash
kubectl get pods -n srb -o wide
```

---

### Paso 9: Desplegar Pods Cliente

Para poner a generar carga autónoma interna en el clúster:
```bash
kubectl apply -f infra/k8s/07-client.yaml
```

Monitorea los logs estructurados JSON de los clientes:
```bash
kubectl logs -n srb -l app=client --tail=50 -f
```

---

## 4. Ejecución del Suite de Pruebas Automatizadas

El archivo `tests/run_tests.py` ejecuta automáticamente los **12 casos de prueba obligatorios** de punta a punta:

```bash
python3 tests/run_tests.py --gt-host 10.43.99.216 --gt-port 30055 --db-connstr "postgresql://root@10.43.99.216:26257/srb?sslmode=disable"
```

### Detalle de los 12 Casos de Prueba

| Caso | Nombre | Validación Realizada |
|---|---|---|
| **01** | Verificación de siembra | 50 eventos, 70 ocurrencias, 100 asientos libres c/u, 0 reservas iniciales. |
| **02** | Consulta de eventos por mes | Estructura JSON válida, conteo de 50 eventos y 70 ocurrencias en `2026-11`. |
| **03** | Reserva exitosa | Respuesta `OK`, asignación de `reservation_id`, decremento atómico de `free_seats`. |
| **04** | Reserva rechazada por cupo | Intento de reservar > 100 puestos; respuesta `REJECTED`, asientos de BD intactos. |
| **05** | Modificación de puestos | Éxito incrementando cupos disponibles; rechazo atómico y rollback si excede capacidad. |
| **06** | Modificación de fecha | Cambio de ocurrencia en evento dual; rechazo en eventos de ocurrencia única. |
| **07** | Cancelación asincrónica | ACK `ACCEPTED` inmediato en DEALER; notificación `CANCELLED` vía SUB; liberación en BD. |
| **08** | Concurrencia estricta | 25 clientes simultáneos pidiendo 5 asientos (125 solicitados sobre cupo 100). Invariante: 0 sobreventas, `free_seats >= 0`. |
| **09** | Tolerancia a fallos: RMC | Eliminación del pod RMC a mitad de proceso; GT re-enruta automáticamente vía LRU. |
| **10** | Tolerancia a fallos: GT | Eliminación del pod GT; clientes reconectan vía Lazy Pirate; estado íntegro en BD. |
| **11** | Comparación de rendimiento | Ejecución de escenarios 3, 5 y 8 clientes sobre 1 RMC vs 3 RMC con Locust. |
| **12** | Verificación final de BD | Invariantes matemáticas: `free_seats` >= 0, suma de asientos activos coincide con ocupados. |

---

## 5. Pruebas de Carga y Métricas de Rendimiento (Locust)

Locust se ejecuta **en el host de PC1** (fuera del cluster) apuntando al puerto `NodePort 30055` del GT.

### Instalación de Locust en PC1:
```bash
pip install locust pyzmq
```

### Ejecutar los 3 Escenarios de Carga:

#### Escenario A (3 clientes: 2 solo consulta, 1 consulta + reserva):
```bash
export GT_HOST="10.43.99.216:30055"
LOCUST_SCENARIO=3 locust -f locust/locustfile.py --headless -u 3 -r 3 -t 2m --csv=locust_scen3_1rmc
```

#### Escenario B (5 clientes: 3 solo consulta, 2 reserva + modificación):
```bash
LOCUST_SCENARIO=5 locust -f locust/locustfile.py --headless -u 5 -r 5 -t 2m --csv=locust_scen5_1rmc
```

#### Escenario C (8 clientes: 4 solo consulta, 4 reserva + modificación):
```bash
LOCUST_SCENARIO=8 locust -f locust/locustfile.py --headless -u 8 -r 8 -t 2m --csv=locust_scen8_1rmc
```

---

### Demostración de Escalabilidad Horizontal (1 vs 3 RMC)

1. Extrae los logs de GT de la ejecución base (1 RMC):
   ```bash
   kubectl logs -n srb -l app=gt --tail=10000 > gt_1rmc.log
   ```

2. Escala el servicio RMC a 3 réplicas en PC3:
   ```bash
   kubectl scale deployment rmc -n srb --replicas=3
   kubectl get pods -n srb -l app=rmc -o wide
   ```

3. Vuelve a ejecutar los escenarios de Locust con 3 réplicas RMC:
   ```bash
   LOCUST_SCENARIO=8 locust -f locust/locustfile.py --headless -u 8 -r 8 -t 2m --csv=locust_scen8_3rmc
   kubectl logs -n srb -l app=gt --tail=10000 > gt_3rmc.log
   ```

4. Agrega y compara métricas con el script `metrics/aggregate.py`:
   ```bash
   python3 -m metrics.aggregate --compare gt_1rmc.log gt_3rmc.log --labels "1 RMC" "3 RMC" --csv-out comparison_metrics.csv
   ```

Salida esperada del agregador:
- Latencia promedio (ms) y desviación estándar por tipo de operación (QUERY, RESERVE, MODIFY).
- Transacciones por Minuto (TPM).
- Ganancia porcentual de TPM y reducción de latencia con 3 RMC.

---

## 6. Runbook de Tolerancia a Fallos e Inyección de Fallas

### Falla 1: Caída de un RMC Worker durante Carga Activa
Mientras Locust está enviando solicitudes:
```bash
# Matar un pod de RMC
kubectl delete pod -n srb -l app=rmc --force --grace-period=0
```
**Comportamiento verificado:**
- El GT detecta la ausencia de heartbeats en 3 intervalos (`HEARTBEAT_LIVENESS = 3`, 6 segundos).
- El worker es desalojado de la cola LRU.
- Las solicitudes en vuelo son re-encoladas al inicio de la cola LRU y asignadas a otro worker disponible.
- Los clientes reciben sus respuestas sin errores visibles.
- K3s reinicia automáticamente el pod de RMC, el cual envía su frame `READY` al GT y se reincorpora al pool.

---

### Falla 2: Caída y Reinicio del Gestor de Transacciones (GT)
```bash
kubectl delete pod -n srb -l app=gt --force --grace-period=0
```
**Comportamiento verificado:**
- El GT es completamente stateless respecto a la persistencia (toda la verdad está en CockroachDB).
- Los clientes detectan timeout (`zmq.RCVTIMEO`) y aplican el patrón **Lazy Pirate**: cierran el socket con `LINGER=0`, esperan un backoff exponencial y reconectan el socket `REQ`.
- Al levantar el nuevo pod de GT, las operaciones pendientes se completan con éxito.
- Ninguna reserva previa se pierde o duplica.

---

### Falla 3: Pérdida de un Nodo de CockroachDB
```bash
# Apagar temporalmente el nodo 2 en PC3
kubectl exec -it -n srb cockroachdb-1 -- /cockroach/cockroach node drain 2 --insecure || kubectl delete pod cockroachdb-1 -n srb
```
**Comportamiento verificado:**
- El clúster de 3 nodos mantiene quórum Raft (2/3 nodos activos: `cockroachdb-0` y `cockroachdb-2`).
- Las transacciones de reservas, modificaciones y cancelaciones continúan ejecutándose normalmente sin interrupción ni pérdida de datos.
- Al restaurar el pod, se sincroniza el log Raft automáticamente.

---

## 7. Verificación del Estado Final de la Base de Datos

Para verificar la integridad matemática y la ausencia total de sobreventas tras todas las pruebas:
```bash
python3 verify_db.py --mode case --case-file tests/cases/12_final_db_verification.json
```

Verificaciones ejecutadas:
1. `free_seats >= 0` en todas las ocurrencias (nunca negativo).
2. `free_seats <= capacity` (nunca excede el aforo de 100).
3. Suma exacta: `capacity - free_seats == SUM(seats de reservas con status ACTIVE)`.

---

## 8. Estructura del Repositorio

```
SRBTickets/
├── README.md                  # Documentación maestra y guía de despliegue
├── ARCHITECTURE.md            # Especificación de arquitectura y protocolos
├── requirements.txt           # Dependencias Python
├── verify_db.py               # Script de verificación de base de datos
├── common/                    # Módulo compartido
│   ├── config.py              # Configuración centralizada vía variables de entorno
│   ├── envelope.py            # Sobre de mensajes JSON y señales de control
│   ├── messages.py            # Enums MsgType y dataclass Envelope
│   ├── health.py              # Servidor HTTP /healthz para livenessProbe
│   └── logging_json.py        # Logging estructurado en formato JSON a stdout
├── services/
│   ├── gt/                    # Gestor de Transacciones (LRU Broker ZeroMQ)
│   ├── rmc/                   # Reservas, Modificaciones, Consultas (Worker LRU)
│   ├── cancel/                # Servicio Asíncrono de Cancelaciones y Notificaciones
│   └── persistence/           # Servicio de Persistencia (psycopg v3 + CockroachDB)
├── clients/                   # Pod cliente autónomo con patrón Lazy Pirate y SUB
├── seed/                      # Script y Dockerfile de siembra determinista
├── locust/                    # Suite de pruebas de carga Locust sobre ZeroMQ
├── metrics/                   # Agregador estadístico de métricas y comparativa 1 vs 3 RMC
├── infra/                     # Infraestructura K3s y manifiestos Kubernetes
│   ├── install_k3s.sh         # Script de instalación y configuración de K3s en 4 máquinas
│   ├── build_images.sh        # Script de compilación y distribución de imágenes
│   └── k8s/                   # Manifiestos YAML por componente (00 al 07)
└── tests/                     # Suite de pruebas automatizada
    ├── run_tests.py           # Runner de las 12 pruebas de punta a punta
    └── cases/                 # Especificaciones JSON de los 12 casos de prueba
```
