================================================================================
  mapa-mesh — Mapa local de red Meshtastic con traceroute activo
  Desarrollado para MeshArg
================================================================================

DESCRIPCIÓN
-----------
mapa-mesh es una aplicación Python que conecta un nodo Meshtastic por puerto
serial y muestra en un mapa web local (Leaflet + OpenStreetMap) todos los nodos
de la red que difunden su posición GPS. Cuando un nodo manda su posición, el
programa realiza automáticamente un traceroute activo hacia ese nodo y dibuja
las rutas de ida y vuelta sobre el mapa.

Toda la información se actualiza en tiempo real vía Socket.IO sin necesidad de
recargar el navegador.


REQUISITOS
----------
- Linux (probado en Linux Mint Debian Edition)
- Python 3.10 o superior
- Un nodo Meshtastic conectado por USB (puerto serial)
- Navegador web moderno (Chrome, Firefox, Edge)


INSTALACIÓN
-----------
1. Crear carpeta del proyecto y copiar los archivos:

     mapa_mesh.py
     logo_mesharg.png
     requirements.txt
     install.sh

2. Ejecutar el instalador (crea un entorno virtual y descarga dependencias):

     bash install.sh

3. Activar el entorno e iniciar:

     source venv/bin/activate
     python3 mapa_mesh.py

4. Abrir en el navegador:

     http://127.0.0.1:8080

   Desde otros dispositivos en la misma red:

     http://<IP-de-tu-PC>:8080


CONFIGURACIÓN
-------------
Al inicio del archivo mapa_mesh.py hay una sección CONFIGURACION con las
variables que hay que editar antes de cada uso:

  SERIAL_PORT            Puerto serial del nodo (default: /dev/ttyACM0)
                         Cambiar a /dev/ttyUSB0 si corresponde.

  HOME_LAT / HOME_LON    Coordenadas de tu nodo (no tiene GPS).
                         Editarlas cada vez que cambiés de ubicación.

  MAP_CENTER_ZOOM        Zoom inicial del mapa (default: 12).

  TRACEROUTE_COOLDOWN_SEC  Tiempo mínimo entre traceroutes al mismo nodo
                           (default: 60 segundos).

  PRUNE_AFTER_SEC        Tiempo hasta borrar un nodo inactivo del mapa
                         (default: 36 horas = 129600 segundos).

  NODEINFO_INTERVAL_SEC  Frecuencia del heartbeat de NodeInfo al mesh
                         (default: 10 minutos = 600 segundos).

  BIND_PORT              Puerto del servidor web local (default: 8080).


DEPENDENCIAS (requirements.txt)
--------------------------------
  meshtastic>=2.3.0      SDK oficial de Meshtastic para Python
  flask>=3.0.0           Servidor web liviano
  flask-socketio>=5.3.6  WebSocket para actualizaciones en tiempo real
  pypubsub>=4.0.3        Sistema de eventos para callbacks de Meshtastic


FUNCIONAMIENTO INTERNO
----------------------
El programa corre tres hilos en paralelo:

  1. meshtastic_thread
     Mantiene la conexión serial con el nodo. Al conectar, carga la base de
     datos local del nodo (nodesByNum) para obtener nombres y roles conocidos.
     Se reconecta automáticamente si hay un error.

  2. traceroute_worker
     Cola serializada de traceroutes. Procesa de a un nodo por vez y respeta
     el cooldown de 30 segundos del firmware de Meshtastic entre envíos.
     Usa un timeout duro de 20 segundos por traceroute para evitar bloqueos.

  3. nodeinfo_heartbeat_thread
     Al arrancar (espera que el nodo esté listo) y cada 10 minutos, envía un
     heartbeat al nodo serial. Esto hace que el nodo difunda su NodeInfo al
     mesh con want_response=True, incentivando a los otros nodos a responder
     con sus datos (nombre, rol, posición).

Cuando llega un paquete con posición GPS de un nodo nuevo o conocido:
  - Se actualiza su posición en el mapa
  - Se encola un traceroute activo hacia ese nodo
  - Al llegar la respuesta, se dibujan las rutas de ida (azul) y vuelta
    (naranja punteada) pasando por los nodos intermedios que tengan GPS

Cuando llega un paquete NODEINFO_APP:
  - Se actualizan nombre y role del nodo

Cuando llega un mensaje de texto (TEXT_MESSAGE_APP):
  - Se muestra en el panel de mensajes del sidebar


INTERFAZ WEB
------------
El sidebar derecho muestra:

  - Logo de MeshArg y estado de conexión (verde = conectado, rojo = error)
  - Contadores: total de nodos, nodos con GPS, rutas trazadas
  - Leyenda: línea azul (ida), naranja punteada (vuelta)
  - Botón "↓ CSV": descarga las rutas actuales en formato CSV
  - Lista de nodos escuchados (con y sin GPS), ordenados por actividad
  - Panel de mensajes recibidos (30% inferior), todos los canales

Los marcadores en el mapa tienen color según el rol del nodo:
  - Azul   → CLIENT, CLIENT_MUTE, CLIENT_HIDDEN, desconocido
  - Rojo   → ROUTER, ROUTER_LATE
  - Amarillo → CLIENT_BASE

Al hacer click en un marcador se muestra un popup con:
  node_id, rol, nombre, RSSI, SNR, saltos de radio, distancia,
  último paquete, posición GPS, y ruta de traceroute (ida / vuelta).

El hover sobre una línea de ruta muestra los nombres de los nodos
intermedios en el tooltip.


EXPORTACIÓN CSV
---------------
El botón "↓ CSV" en la barra de leyenda descarga un archivo rutas_mesh.csv
con las rutas traceroute actualmente en memoria. Columnas:

  timestamp       Fecha y hora del traceroute (YYYY-MM-DD HH:MM:SS)
  node_id         ID del nodo destino
  short_name      Nombre corto
  long_name       Nombre largo
  role            Rol del nodo
  hops_forward    Saltos de ida (separados por >)
  hop_count_fwd   Cantidad de saltos ida
  hops_back       Saltos de vuelta (separados por >)
  hop_count_back  Cantidad de saltos vuelta
  rssi            RSSI del último paquete (dBm)
  snr             SNR del último paquete (dB)
  dist_km         Distancia estimada al nodo (km)


ARCHIVOS DEL PROYECTO
---------------------
  mapa_mesh.py       Script principal
  logo_mesharg.png   Logo de MeshArg (servido en /logo)
  requirements.txt   Dependencias Python
  install.sh         Script de instalación del entorno virtual
  README.txt         Este archivo


FUENTES Y REFERENCIAS
---------------------
  Meshtastic Python SDK
    https://python.meshtastic.org

  Meshtastic firmware (fuente del protocolo traceroute y NodeInfo)
    https://github.com/meshtastic/firmware

  Leaflet.js — librería de mapas
    https://leafletjs.com

  OpenStreetMap — tiles del mapa
    https://www.openstreetmap.org

  Flask — servidor web
    https://flask.palletsprojects.com

  Flask-SocketIO — WebSocket
    https://flask-socketio.readthedocs.io

  MeshArg
    https://mesharg.com.ar


NOTAS TÉCNICAS
--------------
- El cooldown de 30 segundos entre traceroutes es una limitación del firmware
  de Meshtastic (TraceRouteModule.h: cooldownMs = 30000). El worker lo respeta
  midiendo el tiempo desde que el envío anterior terminó (no desde que empezó).

- Los nodos intermedios sin GPS se omiten del trazado visual pero aparecen
  en el tooltip de la línea con su nombre si está disponible.

- El heartbeat de NodeInfo usa ToRadio.Heartbeat(nonce=1), que activa la
  ruta shorterTimeout del firmware (ventana de 60s en lugar de 10 minutos).
  Los nodos que respondieron en las últimas 12 horas pueden ignorarlo
  (NodeInfoReplySuppressSeconds = 12h, hardcodeado en el firmware).

- Todo el estado es en memoria. Al reiniciar el programa se pierde el historial
  de rutas y mensajes. Los nodos vuelven a aparecer cuando mandan nuevos
  paquetes.


================================================================================
  mapa-mesh — hecho con ☆ para MeshArg
================================================================================
