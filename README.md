# mapa-mesh

Mapa local de red Meshtastic con traceroute activo, desarrollado para [MeshArg](https://mesharg.com.ar).

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Version](https://img.shields.io/badge/version-1.1-blue)
![Version](https://img.shields.io/badge/version-1.1-orange)
![License](https://img.shields.io/badge/license-MIT-green)
![Meshtastic](https://img.shields.io/badge/meshtastic-compatible-brightgreen)

---

## Descripción

**mapa-mesh** conecta un nodo Meshtastic por puerto serial y muestra en un mapa web local (Leaflet + OpenStreetMap) todos los nodos de la red que difunden su posición GPS.

Cuando un nodo manda su posición, el programa realiza automáticamente un traceroute activo hacia ese nodo y dibuja las rutas de ida y vuelta sobre el mapa. Toda la información se actualiza en tiempo real vía Socket.IO sin necesidad de recargar el navegador.

---

## Características

- 🗺️ Mapa en tiempo real con nodos GPS sobre OpenStreetMap
- 📡 Traceroute activo con rutas de ida (azul) y vuelta (naranja)
- 🔵🔴🟡 Íconos coloreados según rol del nodo (CLIENT / ROUTER / CLIENT_BASE)
- 💬 Panel de mensajes de texto recibidos (todos los canales)
- 📋 Exportación de rutas a CSV con un click
- 🔔 Heartbeat periódico de NodeInfo para descubrir nodos
- 🧹 Auto-limpieza de nodos inactivos (configurable, default 36 horas)

---

## Requisitos

- Linux (probado en Linux Mint Debian Edition)
- Python 3.10 o superior
- Un nodo Meshtastic conectado por USB (puerto serial)
- Navegador web moderno (Chrome, Firefox, Edge)

---

## Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/TenoTrash/mapa-mesh.git
cd mapa-mesh

# 2. Instalar dependencias en entorno virtual
bash install.sh

# 3. Activar el entorno e iniciar
source venv/bin/activate
python3 mapa_mesh.py
```

Abrir en el navegador: `http://127.0.0.1:8080`

Desde otros dispositivos en la misma red: `http://<IP-de-tu-PC>:8080`

---

## Configuración

Al inicio de `mapa_mesh.py` hay una sección `CONFIGURACION` con las variables a editar antes de cada uso:

| Variable | Default | Descripción |
|---|---|---|
| `SERIAL_PORT` | `/dev/ttyACM0` | Puerto serial del nodo. Cambiar a `/dev/ttyUSB0` si corresponde. |
| `HOME_LAT` / `HOME_LON` | `-34.606615, -58.4355` | Coordenadas de tu nodo (sin GPS). Editar cada vez que cambies de lugar. |
| `MAP_CENTER_ZOOM` | `12` | Zoom inicial del mapa. |
| `TRACEROUTE_COOLDOWN_SEC` | `60` | Tiempo mínimo entre traceroutes al mismo nodo (segundos). |
| `PRUNE_AFTER_SEC` | `129600` | Tiempo hasta borrar un nodo inactivo del mapa (36 horas). |
| `NODEINFO_INTERVAL_SEC` | `600` | Frecuencia del heartbeat de NodeInfo (10 minutos). |
| `BIND_PORT` | `8080` | Puerto del servidor web local. |

---

## Dependencias

```
meshtastic>=2.3.0      # SDK oficial de Meshtastic
flask>=3.0.0           # Servidor web
flask-socketio>=5.3.6  # WebSocket para tiempo real
pypubsub>=4.0.3        # Eventos para callbacks de Meshtastic
```

---

## Funcionamiento interno

El programa corre tres hilos en paralelo:

**`meshtastic_thread`**
Mantiene la conexión serial con el nodo. Al conectar, carga la base de datos local del nodo (`nodesByNum`) para obtener nombres y roles conocidos. Se reconecta automáticamente ante cualquier error.

**`traceroute_worker`**
Cola serializada de traceroutes. Procesa de a un nodo por vez respetando el cooldown de 30 segundos del firmware. Usa un timeout duro de 20 segundos por traceroute para evitar bloqueos.

**`nodeinfo_heartbeat_thread`**
Al arrancar y cada 10 minutos envía un heartbeat al nodo serial. Esto hace que el nodo difunda su NodeInfo al mesh con `want_response=True`, incentivando a los otros nodos a responder con sus datos.

---

## Interfaz web

El sidebar derecho muestra:

- Logo de MeshArg y estado de conexión (verde = conectado, rojo = error)
- Contadores: total de nodos, nodos con GPS, rutas trazadas
- Leyenda de rutas y botón **↓ CSV**
- Lista de nodos escuchados ordenados por actividad reciente
- Panel de mensajes recibidos (30% inferior), todos los canales

**Color de marcadores según rol:**

| Color | Roles |
|---|---|
| 🔵 Azul | `CLIENT`, `CLIENT_MUTE`, `CLIENT_HIDDEN`, desconocido |
| 🔴 Rojo | `ROUTER`, `ROUTER_LATE` |
| 🟡 Amarillo | `CLIENT_BASE` |

Al hacer click en un marcador se muestra: node_id, rol, nombre, RSSI, SNR, saltos, distancia, posición GPS y ruta de traceroute.

---

## Exportación CSV

El botón **↓ CSV** descarga `rutas_mesh.csv` con las rutas actualmente en memoria:

| Columna | Descripción |
|---|---|
| `timestamp` | Fecha y hora del traceroute |
| `node_id` | ID del nodo destino |
| `short_name` / `long_name` | Nombre del nodo |
| `role` | Rol del nodo |
| `hops_forward` | Saltos de ida (separados por `>`) |
| `hop_count_fwd` | Cantidad de saltos ida |
| `hops_back` | Saltos de vuelta (separados por `>`) |
| `hop_count_back` | Cantidad de saltos vuelta |
| `rssi` | RSSI del último paquete (dBm) |
| `snr` | SNR del último paquete (dB) |
| `dist_km` | Distancia estimada (km) |

---

## Notas técnicas

- El cooldown de 30 segundos entre traceroutes es una limitación del firmware (`TraceRouteModule.h: cooldownMs = 30000`). El worker lo respeta midiendo el tiempo desde que el envío anterior terminó.
- Los nodos intermedios sin GPS se omiten del trazado visual pero aparecen en el tooltip de la línea con su nombre.
- El heartbeat usa `ToRadio.Heartbeat(nonce=1)`, que activa la ruta `shorterTimeout` del firmware (60s en lugar de 10 minutos). Los nodos que respondieron en las últimas 12 horas pueden ignorarlo.
- Todo el estado es en memoria. Al reiniciar se pierde el historial de rutas y mensajes.

---

## Archivos

```
mapa-mesh/
├── mapa_mesh.py        # Script principal
├── logo_mesharg.png    # Logo de MeshArg
├── requirements.txt    # Dependencias Python
├── install.sh          # Instalador del entorno virtual
├── credits.txt         # Créditos
└── README.md           # Este archivo
```

---

## SSL / HTTPS

mapa-mesh soporta HTTPS con certificado real o autofirmado.

**Con certificado real (recomendado):**

Colocá los archivos en la carpeta `ssl/` dentro del proyecto:
```
mapa-mesh/
└── ssl/
    ├── mapa-mesh_hopto_org.pem   # certificado (PEM Chain)
    └── mapa-mesh.key             # clave privada generada con openssl
```

El programa detecta automáticamente los certificados al arrancar. Si no los encuentra, usa un certificado autofirmado como fallback.

**Generar la clave privada y CSR:**
```bash
mkdir -p ssl
openssl req -new -newkey rsa:2048 -nodes \
  -keyout ssl/mapa-mesh.key \
  -out ssl/mapa-mesh.csr \
  -subj "/C=AR/ST=Buenos Aires/L=Buenos Aires/O=MeshArg/CN=tu-dominio.hopto.org"
```

---

## Créditos

Basado en ideas de grumpy_bot, mapa de LW7DFM, firmware de Meshtastic y cartel de led "marquee".

---

## Referencias

- [Meshtastic Python SDK](https://python.meshtastic.org)
- [Meshtastic firmware](https://github.com/meshtastic/firmware)
- [Leaflet.js](https://leafletjs.com)
- [OpenStreetMap](https://www.openstreetmap.org)
- [Flask](https://flask.palletsprojects.com)
- [Flask-SocketIO](https://flask-socketio.readthedocs.io)
- [MeshArg](https://mesharg.com.ar)

---

## Licencia

MIT © [TenoTrash](https://github.com/TenoTrash)
