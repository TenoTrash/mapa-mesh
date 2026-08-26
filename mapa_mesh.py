#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# mapa-mesh — v1.1
# Muestra nodos Meshtastic con GPS en un mapa local.
# Realiza traceroute activo cuando llega un paquete de posicion,
# dibuja ruta de ida (azul) y vuelta (naranja) sobre Leaflet/OSM.
# =============================================================================

import time
import json
import math
import threading
import logging
import logging.handlers
from dataclasses import dataclass, asdict, field
from typing import Dict, Optional, List, Tuple

from flask import Flask, Response, jsonify
from flask_socketio import SocketIO
from pubsub import pub

import meshtastic.serial_interface
from meshtastic.protobuf import mesh_pb2, portnums_pb2

import os as _os_log
_LOG_FILE = _os_log.path.join(_os_log.path.dirname(_os_log.path.abspath(__file__)), "mapa_mesh.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                              # consola
        logging.handlers.RotatingFileHandler(                # archivo rotativo
            _LOG_FILE,
            maxBytes=5 * 1024 * 1024,   # 5 MB por archivo
            backupCount=3,               # hasta 3 archivos de backup
            encoding="utf-8",
        ),
    ]
)
log = logging.getLogger("mapa-mesh")
log.info(f"Log guardado en: {_LOG_FILE}")

# =============================================================================
#                               CONFIGURACION
# =============================================================================

SERIAL_PORT   = "/dev/ttyUSB0"
BIND_HOST     = "0.0.0.0"
BIND_PORT     = 8080

# Coordenadas de TU nodo (sin GPS — editá esto cada vez que cambies de lugar)
HOME_LAT      = -34.606615
HOME_LON      = -58.4355

# Centro inicial del mapa y zoom
MAP_CENTER_LAT  = HOME_LAT
MAP_CENTER_LON  = HOME_LON
MAP_CENTER_ZOOM = 11

# Tiempo mínimo entre traceroutes al mismo nodo (segundos)
TRACEROUTE_COOLDOWN_SEC = 5 * 60 #cada 5 minutos

# Timeout para esperar respuesta de traceroute (segundos)
TRACEROUTE_TIMEOUT_SEC  = 15

# Borrar nodos no escuchados después de N horas
PRUNE_AFTER_SEC = 36 * 60 * 60

# Intervalo de polling del frontend (segundos)
POLL_REFRESH_SEC = 10

# Intervalo del ciclo periódico de traceroute a todos los nodos con GPS (segundos)
TRACEROUTE_PERIODIC_SEC = 5 * 60   # cada 5 minutos

# Archivo de backup de estado (mismo directorio que el script)
import os as _os
BACKUP_FILE = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "state_backup.json")

# =============================================================================
#                               MODELO DE DATOS
# =============================================================================

@dataclass
class RouteInfo:
    """Resultado de un traceroute hacia un nodo."""
    # Lista de node_ids intermedios (sin incluir origen ni destino)
    hops_forward: List[str]  = field(default_factory=list)
    hops_back:    List[str]  = field(default_factory=list)
    hop_count_fwd: int       = 0
    hop_count_back: int      = 0
    timestamp: float         = 0.0


@dataclass
class MessageEntry:
    """Un mensaje de texto recibido."""
    from_id:    str
    from_name:  str   = ""
    channel:    int   = 0
    text:       str   = ""
    timestamp:  float = 0.0


@dataclass
class NodeEntry:
    node_id:    str
    short_name: str           = ""
    long_name:  str           = ""
    lat:        Optional[float] = None
    lon:        Optional[float] = None
    alt:        Optional[float] = None
    last_seen:  float         = 0.0
    rssi:       Optional[float] = None
    snr:        Optional[float] = None
    hops:       Optional[int]   = None
    dist_km:    Optional[float] = None
    role:       str           = ""   # ROUTER, ROUTER_LATE, CLIENT_MUTE, CLIENT, CLIENT_HIDDEN, etc.
    route:      Optional[RouteInfo] = None
    # Control de traceroute
    last_traceroute_ts: float = 0.0
    traceroute_pending: bool  = False


# =============================================================================
#                               ESTADO GLOBAL
# =============================================================================

nodes_lock = threading.Lock()
nodes: Dict[str, NodeEntry] = {}

# Mensajes de texto recibidos (todos los canales)
messages_lock = threading.Lock()
messages: List[MessageEntry] = []
MAX_MESSAGES = 300  # cuántos mensajes conservar en memoria

state_lock    = threading.Lock()
last_packet_ts = 0.0
connected      = False
last_error     = ""

# Referencia global a la interfaz Meshtastic (necesaria para enviar traceroute)
iface_lock  = threading.Lock()
iface_ref: Optional[meshtastic.serial_interface.SerialInterface] = None

# ── Cola serializada de traceroutes ─────────────────────────────────────────
# Un único worker consume esta cola de a un nodo por vez.
# Esto garantiza: sin paralelismo, sin colisiones de respuesta.
# La cola es una cola de prioridad por distancia (ver DistancePriorityQueue
# más abajo): se procesan primero los nodos más cercanos y se va "expandiendo"
# hacia los más lejanos, en vez de simple orden de llegada (FIFO).
traceroute_queue: "DistancePriorityQueue"  # se instancia después de definir la clase

# Timestamp del último traceroute enviado (global, no por nodo)
last_traceroute_sent_ts: float = 0.0
traceroute_global_lock = threading.Lock()

# Cooldown mínimo forzado por el firmware (30s hardcodeado en TraceRouteModule.h)
FIRMWARE_COOLDOWN_SEC = 31  # 1s de margen sobre los 30s del firmware


# =============================================================================
#                               HELPERS
# =============================================================================

def now() -> float:
    return time.time()


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl   = math.radians(lon2 - lon1)
    a    = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def normalize_coord(x):
    if x is None:
        return None
    if isinstance(x, int):
        if abs(x) > 10_000_000:
            x = x / 1e7
        elif abs(x) > 1_000_000:
            x = x / 1e6
        else:
            x = float(x)
    else:
        x = float(x)
    # Descartar null island (0,0) — coordenadas inválidas típicas de GPS sin fix
    if abs(x) < 0.001:
        return None
    return x


def update_node(node_id: str, **kwargs):
    with nodes_lock:
        entry = nodes.get(node_id)
        if not entry:
            entry = NodeEntry(node_id=node_id, last_seen=now())
            nodes[node_id] = entry
        for k, v in kwargs.items():
            if hasattr(entry, k) and v is not None:
                setattr(entry, k, v)
        entry.last_seen = now()
    return entry


def prune_nodes():
    cutoff = now() - PRUNE_AFTER_SEC
    with nodes_lock:
        to_del = [nid for nid, e in nodes.items() if e.last_seen < cutoff]
        for nid in to_del:
            del nodes[nid]
            log.info(f"Nodo purgado por inactividad: {nid}")


def serialize_nodes() -> list:
    """Serializa todos los nodos para enviar al frontend."""
    with nodes_lock:
        result = []
        for nid, e in nodes.items():
            d = {
                "node_id":     e.node_id,
                "short_name":  e.short_name,
                "long_name":   e.long_name,
                "lat":         e.lat,
                "lon":         e.lon,
                "alt":         e.alt,
                "last_seen":   e.last_seen,
                "rssi":        e.rssi,
                "snr":         e.snr,
                "hops":        e.hops,
                "dist_km":     e.dist_km,
                "traceroute_pending": e.traceroute_pending,
                "role":  e.role,
                "route": None,
            }
            if e.route:
                d["route"] = {
                    "hops_forward":    e.route.hops_forward,
                    "hops_back":       e.route.hops_back,
                    "hop_count_fwd":   e.route.hop_count_fwd,
                    "hop_count_back":  e.route.hop_count_back,
                    "timestamp":       e.route.timestamp,
                }
            result.append(d)
    return result


def serialize_messages() -> list:
    """Serializa los últimos mensajes para enviar al frontend."""
    with messages_lock:
        return [
            {
                "from_id":   m.from_id,
                "from_name": m.from_name,
                "channel":   m.channel,
                "text":      m.text,
                "timestamp": m.timestamp,
            }
            for m in messages
        ]


# =============================================================================
#                         TRACEROUTE ACTIVO — WORKER SERIALIZADO
# =============================================================================
#
# Diseño:
#   - Un único hilo (traceroute_worker) consume traceroute_queue de a uno.
#   - sendTraceRoute() bloquea hasta recibir respuesta o lanzar excepción
#     por timeout (MeshInterface.MeshInterfaceError). No hay eventos manuales.
#   - Entre cada envío se respeta FIRMWARE_COOLDOWN_SEC (31s) a nivel global.
#   - maybe_schedule_traceroute() solo encola si el nodo no está ya pendiente
#     y pasó TRACEROUTE_COOLDOWN_SEC desde su último traceroute individual.
#   - La cola descarta duplicados: si un nodo ya está encolado, no se vuelve
#     a encolar aunque lleguen más paquetes suyos.
#   - La cola prioriza por distancia (dist_km al nodo home): los nodos más
#     cercanos se traceroutean primero y la "frontera" de traceroutes se va
#     expandiendo hacia nodos más lejanos. Esto tiene sentido porque los
#     nodos cercanos suelen tener mejor enlace y responden más rápido/
#     confiablemente, y porque sus rutas frecuentemente son prerequisito
#     (hops intermedios) para resolver las rutas de nodos lejanos.
#   - Cada TRACEROUTE_QUEUE_REORG_SEC (15 min) se recalculan las prioridades
#     de todo lo que sigue en cola y se reconstruye el heap, para reflejar
#     posiciones actualizadas o nodos nuevos que se colaron con más prioridad.
#
# =============================================================================

import heapq
import itertools


class DistancePriorityQueue:
    """
    Cola de prioridad thread-safe para traceroutes, ordenada por distancia
    (menor distancia = mayor prioridad = se procesa antes). Nodos sin
    distancia conocida van al final (prioridad infinita).

    Es "reorganizable": reorganize() recalcula la prioridad de cada nodo
    que sigue en cola (sin tocar los que ya se sacaron) y reconstruye el
    heap, para que la cola refleje distancias actualizadas con el tiempo.
    """

    def __init__(self):
        self._heap: List[Tuple[float, int, str]] = []   # (priority, seq, node_id)
        self._members: set = set()                       # node_ids en cola (evita duplicados)
        self._counter = itertools.count()                 # desempate estable entre iguales
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def put(self, node_id: str, priority: float) -> bool:
        """Encola node_id. Devuelve False si ya estaba en la cola (no-op)."""
        with self._not_empty:
            if node_id in self._members:
                return False
            self._members.add(node_id)
            heapq.heappush(self._heap, (priority, next(self._counter), node_id))
            self._not_empty.notify()
            return True

    def get(self) -> str:
        """Bloquea hasta que haya un nodo disponible y devuelve el de mayor prioridad."""
        with self._not_empty:
            while not self._heap:
                self._not_empty.wait()
            _priority, _seq, node_id = heapq.heappop(self._heap)
            self._members.discard(node_id)
            return node_id

    def contains(self, node_id: str) -> bool:
        with self._lock:
            return node_id in self._members

    def qsize(self) -> int:
        with self._lock:
            return len(self._heap)

    def reorganize(self, priority_func):
        """
        Recalcula la prioridad de cada nodo actualmente en cola (llamando a
        priority_func(node_id) -> float) y reconstruye el heap desde cero.
        No afecta al nodo que el worker ya sacó de la cola y está procesando.
        """
        with self._not_empty:
            if not self._heap:
                return
            rebuilt = [
                (priority_func(node_id), next(self._counter), node_id)
                for (_old_priority, _old_seq, node_id) in self._heap
            ]
            heapq.heapify(rebuilt)
            self._heap = rebuilt
            self._not_empty.notify_all()


traceroute_queue = DistancePriorityQueue()


def _node_priority(node_id: str) -> float:
    """Prioridad de traceroute de un nodo: su dist_km actual (o infinito si se desconoce)."""
    with nodes_lock:
        entry = nodes.get(node_id)
        if entry and entry.dist_km is not None:
            return entry.dist_km
    return float("inf")


def traceroute_worker():
    """
    Hilo único que procesa traceroutes de forma serializada.
    Corre para siempre como daemon.
    """
    global last_traceroute_sent_ts

    while True:
        # Bloquea hasta que haya un nodo en la cola (el de mayor prioridad, i.e. más cercano)
        node_id = traceroute_queue.get()

        # Verificar que el nodo sigue existiendo y tiene GPS
        with nodes_lock:
            entry = nodes.get(node_id)
            if not entry or entry.lat is None or entry.lon is None:
                log.info(f"Traceroute descartado (nodo sin GPS o eliminado): {node_id}")
                with nodes_lock:
                    if node_id in nodes:
                        nodes[node_id].traceroute_pending = False
                continue

        # Respetar cooldown global del firmware entre envíos consecutivos
        with traceroute_global_lock:
            elapsed = now() - last_traceroute_sent_ts
            wait_sec = FIRMWARE_COOLDOWN_SEC - elapsed
            if wait_sec > 0:
                log.info(f"Esperando cooldown de firmware: {wait_sec:.1f}s antes de traceroute a {node_id}")
                time.sleep(wait_sec)

        # Obtener interfaz
        with iface_lock:
            iface = iface_ref

        if iface is None:
            log.warning(f"Traceroute cancelado: sin interfaz ({node_id})")
            with nodes_lock:
                if node_id in nodes:
                    nodes[node_id].traceroute_pending = False
            continue

        log.info(f"Traceroute → {node_id}")

        # Usamos sendData() directamente en lugar de sendTraceRoute().
        #
        # sendTraceRoute() internamente hace:
        #   1. sendData(... TRACEROUTE_APP ...)   ← lo que queremos
        #   2. waitForTraceRoute(waitFactor)       ← NO lo queremos
        #
        # El waitFactor = min(len(nodes)-1, hopLimit) = min(144, 7) = 7
        # expireTimeout = 20s * 7 = 140s de bloqueo por traceroute fallido.
        #
        # La respuesta del traceroute llega por pubsub (on_receive TRACEROUTE_APP)
        # de forma completamente asíncrona — waitForTraceRoute() es redundante
        # para nuestro uso y solo causa bloqueo y fuga de hilos.
        #
        # Usando sendData() directamente el worker retorna INMEDIATAMENTE
        # después de enviar el paquete, sin esperar respuesta. La respuesta
        # llega cuando llega, por on_receive.
        try:
            r = mesh_pb2.RouteDiscovery()
            iface.sendData(
                r,
                destinationId=node_id,
                portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                wantResponse=True,
                hopLimit=7,
            )
        except Exception as e:
            log.warning(f"sendData traceroute excepcion para {node_id}: {e}")

        # Registrar timestamp después del envío (retorna inmediatamente)
        with traceroute_global_lock:
            last_traceroute_sent_ts = now()

        with nodes_lock:
            if node_id in nodes:
                nodes[node_id].traceroute_pending = False
                nodes[node_id].last_traceroute_ts = now()

        # Empujar actualización al frontend
        socketio.emit("nodes_update", {"nodes": serialize_nodes(), "status": get_status()})


def maybe_schedule_traceroute(node_id: str):
    """
    Encola un traceroute para node_id si:
      - Tiene GPS
      - No está ya encolado o en ejecución (traceroute_pending)
      - Pasó TRACEROUTE_COOLDOWN_SEC desde su último traceroute individual
    La prioridad en la cola es su distancia actual (dist_km) al nodo home:
    a menor distancia, antes se procesa.
    """
    with nodes_lock:
        entry = nodes.get(node_id)
        if not entry:
            return
        has_pos     = entry.lat is not None and entry.lon is not None
        pending     = entry.traceroute_pending
        elapsed     = now() - entry.last_traceroute_ts
        cooldown_ok = elapsed >= TRACEROUTE_COOLDOWN_SEC
        dist_km     = entry.dist_km

    if not has_pos:
        return
    if pending:
        log.debug(f"Traceroute ya pendiente para {node_id}, ignorando")
        return
    if not cooldown_ok:
        log.debug(f"Cooldown activo para {node_id} ({elapsed:.0f}s < {TRACEROUTE_COOLDOWN_SEC}s)")
        return

    priority = dist_km if dist_km is not None else float("inf")

    # put() es atómico: si el nodo ya estaba en cola, no hace nada y devuelve False
    if not traceroute_queue.put(node_id, priority):
        log.debug(f"Nodo {node_id} ya en cola, ignorando")
        return

    # Marcar como pendiente
    with nodes_lock:
        if node_id in nodes:
            nodes[node_id].traceroute_pending = True

    dist_str = f"{priority:.1f} km" if priority != float("inf") else "desconocida"
    log.info(f"Encolando traceroute para {node_id} (dist={dist_str}, cola: {traceroute_queue.qsize()})")


# =============================================================================
#                    NODEINFO HEARTBEAT — BROADCAST PERIÓDICO
# =============================================================================
#
# Al arrancar y cada NODEINFO_INTERVAL_SEC, enviamos un heartbeat al nodo
# conectado por serial. Esto lo hace broadcastear su propio NodeInfo al mesh
# con want_response=True, lo que incentiva a los otros nodos a responder
# con el suyo (nombre, posición, etc).
#
# Mecanismo: ToRadio.Heartbeat(nonce=1) → firmware llama a
#   NodeInfoModule::sendOurNodeInfo(..., shorterTimeout=true)
# con ventana de supresión de 60s en lugar de los 10 minutos habituales.
# Referencia: firmware-develop/mcp-server/tests/mesh/_receive.py::nudge_nodeinfo()
#
# =============================================================================

NODEINFO_INTERVAL_SEC = 10 * 60  # cada 10 minutos


def send_nodeinfo_heartbeat():
    """Envía heartbeat para que el nodo broadcastee su NodeInfo al mesh."""
    with iface_lock:
        iface = iface_ref
    if iface is None:
        log.debug("Heartbeat omitido: sin interfaz")
        return
    try:
        from meshtastic.protobuf import mesh_pb2
        tr = mesh_pb2.ToRadio()
        tr.heartbeat.nonce = 1
        iface._sendToRadio(tr)
        log.info("NodeInfo heartbeat enviado al mesh")
    except Exception as e:
        log.warning(f"Error enviando NodeInfo heartbeat: {e}")


def nodeinfo_heartbeat_thread():
    """
    Hilo daemon que envía un heartbeat al arrancar y luego cada 10 minutos.
    Espera a que la interfaz esté disponible antes del primer envío.
    """
    # Esperar a que el nodo esté conectado (máx 60s)
    deadline = now() + 60
    while now() < deadline:
        with iface_lock:
            ready = iface_ref is not None
        if ready:
            break
        time.sleep(2)

    send_nodeinfo_heartbeat()  # envío inicial al arrancar

    while True:
        time.sleep(NODEINFO_INTERVAL_SEC)
        send_nodeinfo_heartbeat()


# =============================================================================
#                  TRACEROUTE PERIÓDICO — ciclo cada 5 minutos
# =============================================================================
#
# Además de traceroutear cuando llega un paquete de posición, este hilo
# revisa periódicamente todos los nodos con GPS conocidos y los reencola
# si ya pasó el cooldown individual. Así las rutas se mantienen actualizadas
# aunque los nodos dejen de mandar posición frecuentemente.
#
# El cooldown por nodo (TRACEROUTE_COOLDOWN_SEC) evita que se saturen nodos
# que sí mandan posición seguido. La cola serializada garantiza que nunca
# hay dos traceroutes en vuelo simultáneos.
#
# =============================================================================

def periodic_traceroute_thread():
    """
    Hilo daemon que cada TRACEROUTE_PERIODIC_SEC encola traceroutes
    para todos los nodos con GPS conocidos que hayan superado el cooldown.
    """
    # Esperar a que el nodo esté conectado y haya algo de datos antes del primer ciclo
    time.sleep(TRACEROUTE_PERIODIC_SEC)

    while True:
        with nodes_lock:
            nodos_con_gps = [
                nid for nid, e in nodes.items()
                if e.lat is not None and e.lon is not None
            ]

        log.info(f"Ciclo periódico de traceroute: {len(nodos_con_gps)} nodos con GPS")

        encolados = 0
        for nid in nodos_con_gps:
            maybe_schedule_traceroute(nid)
            encolados += 1

        if encolados > 0:
            log.info(f"Ciclo periódico: {encolados} nodos evaluados para traceroute")

        time.sleep(TRACEROUTE_PERIODIC_SEC)


# =============================================================================
#         REORGANIZACIÓN PERIÓDICA DE LA COLA — ciclo cada 15 minutos
# =============================================================================
#
# La cola de traceroutes prioriza por distancia (dist_km) al momento en que
# cada nodo fue encolado. Con el tiempo esa distancia puede quedar desactua-
# lizada (el nodo se movió, o se le resolvió el GPS después de encolarse sin
# posición). Este hilo recalcula periódicamente la prioridad de todo lo que
# sigue esperando en la cola y reconstruye el heap, para que la cola siga
# reflejando "cercanos primero, expandiendo hacia los lejanos" con datos
# frescos.
#
# =============================================================================

TRACEROUTE_QUEUE_REORG_SEC = 15 * 60  # cada 15 minutos


def traceroute_queue_reorg_thread():
    """Hilo daemon que reorganiza la cola de traceroutes por distancia cada TRACEROUTE_QUEUE_REORG_SEC."""
    while True:
        time.sleep(TRACEROUTE_QUEUE_REORG_SEC)

        size = traceroute_queue.qsize()
        if size == 0:
            continue

        traceroute_queue.reorganize(_node_priority)
        log.info(f"Cola de traceroute reorganizada por distancia ({size} nodos en espera)")


# =============================================================================
#                       HANDLERS MESHTASTIC
# =============================================================================

def on_receive(packet: dict, interface):
    global last_packet_ts

    with state_lock:
        last_packet_ts = now()

    from_id = str(packet.get("fromId") or packet.get("from") or "")
    if not from_id:
        return

    # ── Traceroute response ──────────────────────────────────────────────────
    decoded  = packet.get("decoded") or {}
    portnum  = decoded.get("portnum", "")

    if portnum == "TRACEROUTE_APP":
        tr = decoded.get("traceroute") or {}

        def nodeid_to_hex(x):
            """
            El SDK puede entregar IDs de nodos intermedios como enteros decimales
            (ej: 4012112832) o como strings con !hex (ej: !ef23fbc0).
            Normalizamos todo a formato !hex para que matchee con los nodos del mapa.
            """
            try:
                n = int(x)
                return f"!{n:08x}"
            except (ValueError, TypeError):
                s = str(x)
                return s if s.startswith("!") else f"!{s}"

        route_fwd  = [nodeid_to_hex(x) for x in tr.get("route",     [])]
        route_back = [nodeid_to_hex(x) for x in tr.get("routeBack", [])]

        # from_id es el nodo que respondió (el destino original del traceroute)
        target_id = from_id

        log.info(f"Traceroute response de {target_id}: fwd={route_fwd} back={route_back}")

        with nodes_lock:
            if target_id in nodes:
                nodes[target_id].route = RouteInfo(
                    hops_forward   = route_fwd,
                    hops_back      = route_back,
                    hop_count_fwd  = len(route_fwd),
                    hop_count_back = len(route_back),
                    timestamp      = now(),
                )
        # Empujar al frontend inmediatamente al llegar la respuesta
        socketio.emit("nodes_update", {"nodes": serialize_nodes(), "status": get_status()})
        return

    # ── NodeInfo — captura nombre y role ────────────────────────────────────
    if portnum == "NODEINFO_APP":
        user = decoded.get("user") or {}
        role = str(user.get("role") or "")
        update_node(
            from_id,
            short_name = user.get("shortName") or "",
            long_name  = user.get("longName")  or "",
            role       = role,
        )
        log.info(f"NodeInfo de {from_id}: role={role}")
        return

    # ── Mensaje de texto ─────────────────────────────────────────────────────
    if portnum == "TEXT_MESSAGE_APP":
        try:
            raw     = decoded.get("payload") or b""
            text_msg = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            channel  = packet.get("channel", 0)

            # Resolver nombre del remitente desde nodos conocidos
            with nodes_lock:
                sender = nodes.get(from_id)
                from_name = (sender.short_name or sender.long_name) if sender else ""
            from_name = from_name or from_id

            entry = MessageEntry(
                from_id   = from_id,
                from_name = from_name,
                channel   = channel,
                text      = text_msg,
                timestamp = now(),
            )
            with messages_lock:
                messages.append(entry)
                if len(messages) > MAX_MESSAGES:
                    messages.pop(0)

            log.info(f"Mensaje [{channel}] {from_name}: {text_msg}")
            socketio.emit("nodes_update", {"nodes": serialize_nodes(), "status": get_status(), "messages": serialize_messages()})
        except Exception as e:
            log.warning(f"Error procesando mensaje de texto: {e}")
        return

    # ── Paquete de posición ──────────────────────────────────────────────────
    rssi = packet.get("rxRssi")
    snr  = packet.get("rxSnr")

    hop_limit = packet.get("hopLimit")
    hop_start = packet.get("hopStart")
    hl = int(hop_limit) if hop_limit is not None else None
    hs = int(hop_start) if hop_start is not None else None
    hops = None
    if hl is not None:
        hops = (hs if hs is not None else 7) - hl
        if hops < 0:
            hops = None

    user = decoded.get("user") or {}
    pos  = decoded.get("position") or {}

    lat = normalize_coord(pos.get("latitude"))
    lon = normalize_coord(pos.get("longitude"))
    alt = float(pos.get("altitude")) if pos.get("altitude") is not None else None

    dist_km = None
    if lat is not None and lon is not None:
        try:
            dist_km = haversine_km(HOME_LAT, HOME_LON, lat, lon)
        except Exception:
            pass

    update_node(
        from_id,
        short_name = user.get("shortName") or "",
        long_name  = user.get("longName")  or "",
        lat        = lat,
        lon        = lon,
        alt        = alt,
        rssi       = float(rssi) if rssi is not None else None,
        snr        = float(snr)  if snr  is not None else None,
        hops       = hops,
        dist_km    = dist_km,
    )

    # Solo disparar traceroute si llegó posición
    if lat is not None and lon is not None:
        maybe_schedule_traceroute(from_id)


def on_connection_changed(is_connected: bool):
    global connected
    with state_lock:
        connected = is_connected


# =============================================================================
#                        HILO MESHTASTIC
# =============================================================================

def meshtastic_thread():
    global iface_ref, last_error

    while True:
        try:
            on_connection_changed(False)
            with state_lock:
                last_error = ""

            log.info(f"Conectando a {SERIAL_PORT}...")
            iface = meshtastic.serial_interface.SerialInterface(
                devPath=SERIAL_PORT,
                debugOut=False,
            )

            with iface_lock:
                iface_ref = iface

            on_connection_changed(True)
            log.info("Conectado a Meshtastic.")

            # Cargar datos iniciales desde la base de nodos local del dispositivo
            try:
                nodes_db = iface.nodesByNum or {}
                for num, rec in nodes_db.items():
                    nid  = f"!{num:08x}"
                    user = rec.get("user") or {}
                    pos  = rec.get("position") or {}
                    role = str(user.get("role") or "")
                    lat  = normalize_coord(pos.get("latitude"))
                    lon  = normalize_coord(pos.get("longitude"))
                    dist_km = None
                    if lat is not None and lon is not None:
                        try:
                            dist_km = haversine_km(HOME_LAT, HOME_LON, lat, lon)
                        except Exception:
                            pass
                    update_node(
                        nid,
                        short_name = user.get("shortName") or "",
                        long_name  = user.get("longName")  or "",
                        role       = role,
                        lat        = lat,
                        lon        = lon,
                        dist_km    = dist_km,
                    )
                log.info(f"Cargados {len(nodes_db)} nodos desde nodesByNum")
            except Exception as e:
                log.warning(f"Error cargando nodesByNum: {e}")

            try:
                pub.unsubscribe(on_receive, "meshtastic.receive")
            except Exception:
                pass
            pub.subscribe(on_receive, "meshtastic.receive")

            while True:
                prune_nodes()
                time.sleep(5)

        except Exception as e:
            on_connection_changed(False)
            with iface_lock:
                iface_ref = None
            with state_lock:
                last_error = str(e)
            log.error(f"Error Meshtastic: {e}. Reintentando en 5s...")
            time.sleep(5)


# =============================================================================
#                           FLASK + SOCKETIO
# =============================================================================

app      = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Contador de usuarios activos: cada cliente hace polling cada POLL_REFRESH_SEC.
# Registramos la última vez que cada IP hizo un GET /api/nodes.
# Si no pidió en los últimos 15s, se considera desconectado.
active_connections_lock = threading.Lock()
active_connections: dict = {}   # ip → last_seen timestamp
VIEWER_TIMEOUT_SEC = 15

# Permite reutilizar el puerto inmediatamente después de un reinicio
import socket as _socket
app.config["PROPAGATE_EXCEPTIONS"] = True
_reuse = getattr(_socket, "SO_REUSEPORT", None) or _socket.SO_REUSEADDR


def get_status() -> dict:
    with state_lock:
        age = (now() - last_packet_ts) if last_packet_ts else None

    # RSSI y SNR promedio de nodos con datos
    rssi_vals, snr_vals = [], []
    with nodes_lock:
        for e in nodes.values():
            if e.rssi is not None: rssi_vals.append(e.rssi)
            if e.snr  is not None: snr_vals.append(e.snr)

    avg_rssi = round(sum(rssi_vals) / len(rssi_vals), 1) if rssi_vals else None
    avg_snr  = round(sum(snr_vals)  / len(snr_vals),  1) if snr_vals  else None

    # Mensajes en las últimas 24hs
    cutoff_24h = now() - 86400
    with messages_lock:
        msgs_24h = sum(1 for m in messages if m.timestamp >= cutoff_24h)

    # Usuarios activos (conexiones SocketIO abiertas)
    with active_connections_lock:
        viewers = len(active_connections)

    return {
        "connected":           connected,
        "last_packet_age_sec": age,
        "last_error":          last_error or None,
        "refresh_sec":         POLL_REFRESH_SEC,
        "prune_after_sec":     PRUNE_AFTER_SEC,
        "avg_rssi":            avg_rssi,
        "avg_snr":             avg_snr,
        "msgs_24h":            msgs_24h,
        "viewers":             viewers,
    }


def _csv_safe(value):
    """
    Neutraliza CSV/Formula Injection (CWE-1236).
    Si un nombre de nodo remoto empieza con =, +, -, @, tab o CR, Excel/
    LibreOffice puede interpretarlo como fórmula al abrir el CSV exportado,
    permitiendo ejecución de comandos en la máquina de quien lo abre.
    Se neutraliza prefijando con un apóstrofo, igual que hace Google Sheets.
    """
    if value is None:
        return ""
    s = str(value)
    if s and s[0] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + s
    return s


@app.get("/export/routes.csv")
def export_routes_csv():
    import io
    import csv
    import datetime

    rows = []
    with nodes_lock:
        for nid, e in nodes.items():
            if not e.route:
                continue
            ts = datetime.datetime.fromtimestamp(e.route.timestamp).strftime("%Y-%m-%d %H:%M:%S")
            rows.append([
                ts,
                _csv_safe(e.node_id),
                _csv_safe(e.short_name),
                _csv_safe(e.long_name),
                _csv_safe(e.role),
                round(e.lat, 7) if e.lat is not None else "",
                round(e.lon, 7) if e.lon is not None else "",
                _csv_safe(" > ".join(e.route.hops_forward) if e.route.hops_forward else "directo"),
                e.route.hop_count_fwd,
                _csv_safe(" > ".join(e.route.hops_back) if e.route.hops_back else "directo"),
                e.route.hop_count_back,
                e.rssi if e.rssi is not None else "",
                e.snr  if e.snr  is not None else "",
                round(e.dist_km, 2) if e.dist_km is not None else "",
            ])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "timestamp", "node_id", "short_name", "long_name", "role",
        "lat", "lon",
        "hops_forward", "hop_count_fwd",
        "hops_back", "hop_count_back",
        "rssi", "snr", "dist_km"
    ])
    writer.writerows(rows)

    resp = Response(output.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = 'attachment; filename="rutas_mesh.csv"'
    return resp


@app.get("/export/nodes.csv")
def export_nodes_csv():
    """
    Exporta todos los nodos conocidos (con o sin ruta) como CSV.
    Incluye lat/lon para permitir cálculo externo de convex hull y cobertura.
    Solo exporta nodos con GPS válido (lat y lon no nulos).
    """
    import io
    import csv
    import datetime

    rows = []
    with nodes_lock:
        for nid, e in nodes.items():
            if e.lat is None or e.lon is None:
                continue
            last_seen_str = datetime.datetime.fromtimestamp(e.last_seen).strftime("%Y-%m-%d %H:%M:%S")
            rows.append([
                last_seen_str,
                _csv_safe(e.node_id),
                _csv_safe(e.short_name),
                _csv_safe(e.long_name),
                _csv_safe(e.role),
                round(e.lat, 7),
                round(e.lon, 7),
                round(e.alt, 1) if e.alt is not None else "",
                e.rssi if e.rssi is not None else "",
                e.snr  if e.snr  is not None else "",
                e.hops if e.hops is not None else "",
                round(e.dist_km, 2) if e.dist_km is not None else "",
            ])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "last_seen", "node_id", "short_name", "long_name", "role",
        "lat", "lon", "alt_m",
        "rssi", "snr", "hops", "dist_km",
    ])
    writer.writerows(rows)

    resp = Response(output.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = 'attachment; filename="nodos_mesh.csv"'
    return resp


@app.get("/logo")
def serve_logo():
    import os
    logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo_mesharg.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            data = f.read()
        return Response(data, mimetype="image/png")
    return Response("", status=404)


@app.get("/api/nodes")
def api_nodes():
    from flask import request as flask_req
    client_ip = flask_req.remote_addr or "unknown"
    if client_ip != "127.0.0.1":
        with active_connections_lock:
            active_connections[client_ip] = now()
        # Limpiar IPs que no pidieron en los últimos VIEWER_TIMEOUT_SEC
        cutoff = now() - VIEWER_TIMEOUT_SEC
        with active_connections_lock:
            stale = [ip for ip, ts in active_connections.items() if ts < cutoff]
            for ip in stale:
                del active_connections[ip]
    return jsonify({"status": get_status(), "nodes": serialize_nodes(), "messages": serialize_messages()})


@app.get("/")
def index():
    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>mapa-mesh</title>
  <link rel="me" href="https://mas.to/@teno"/>
  <link rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      --bg:       #0f1117;
      --surface:  #181c25;
      --border:   #252a38;
      --text:     #d4d8e8;
      --muted:    #5a6080;
      --accent:   #3b82f6;
      --fwd:      #3b82f6;   /* azul — ruta ida */
      --back:     #f97316;   /* naranja — ruta vuelta */
      --ok:       #22c55e;
      --bad:      #ef4444;
      --pending:  #facc15;
      --mono:     "JetBrains Mono", "Fira Code", ui-monospace, monospace;
      --sans:     "Inter", system-ui, sans-serif;
    }}

    html, body {{ height: 100%; background: var(--bg); color: var(--text);
                  font-family: var(--sans); font-size: 15px; }}

    .wrap {{ display: flex; height: 100%; }}

    /* ── MAPA ── */
    #map {{ flex: 1; }}

    /* ── SIDEBAR ── */
    #side {{
      width: 300px;
      background: var(--surface);
      border-left: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }}

    .side-header {{
      padding: 16px 16px 12px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }}

    .side-title {{
      font-size: 17px;
      font-weight: 700;
      letter-spacing: .03em;
      color: #fff;
      margin-bottom: 4px;
    }}

    .pill {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 999px;
      font-size: 13px;
      font-family: var(--mono);
      background: var(--border);
      color: var(--muted);
      margin-top: 4px;
    }}
    .pill.ok  {{ background: #14532d; color: var(--ok); }}
    .pill.bad {{ background: #450a0a; color: var(--bad); }}

    /* ── STATS BAR ── */
    .stats-bar {{
      display: flex;
      gap: 8px;
      padding: 10px 16px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }}
    .stat-box {{
      flex: 1;
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 8px 10px;
      text-align: center;
    }}
    .stat-num  {{ font-size: 22px; font-weight: 700; color: #fff; line-height: 1; }}
    .stat-lbl  {{ font-size: 12px; color: var(--muted); margin-top: 3px; }}

    /* ── LEGEND ── */
    .legend {{
      display: flex;
      gap: 16px;
      padding: 10px 16px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }}
    .leg-item {{ display: flex; align-items: center; gap: 6px; font-size: 13px; color: var(--muted); }}
    .leg-line {{ width: 24px; height: 3px; border-radius: 2px; }}
    .leg-line.fwd  {{ background: var(--fwd); }}
    .leg-line.back {{ background: var(--back); }}

    /* ── LISTA ── */
    .list-label {{
      padding: 10px 16px 5px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .08em;
      color: var(--muted);
      flex-shrink: 0;
    }}

    #list {{
      flex: 1;
      overflow-y: auto;
      padding: 0 8px 12px;
    }}
    #list::-webkit-scrollbar {{ width: 4px; }}
    #list::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 2px; }}

    .node-row {{
      padding: 10px 10px;
      border-bottom: 1px solid var(--border);
      cursor: pointer;
      border-radius: 6px;
      transition: background .12s;
    }}
    .node-row:hover {{ background: var(--bg); }}

    .node-id    {{ font-family: var(--mono); font-size: 12px; color: var(--accent); }}
    .node-name  {{ font-weight: 600; color: #fff; font-size: 15px; margin: 2px 0; }}
    .node-meta  {{ color: var(--muted); font-size: 13px; }}
    .node-route {{ font-size: 12px; margin-top: 4px; }}
    .badge-fwd  {{ color: var(--fwd); }}
    .badge-back {{ color: var(--back); }}
    .badge-pending {{ color: var(--pending); }}
    .no-gps     {{ opacity: .55; }}

    /* ── POPUP LEAFLET ── */
    .lf-popup {{
      font-family: var(--sans);
      font-size: 13px;
      color: #1e293b;
      min-width: 220px;
    }}
    .lf-popup .pid   {{ font-family: var(--mono); font-size: 12px; color: #64748b; }}
    .lf-popup .pname {{ font-size: 15px; font-weight: 700; margin: 2px 0 6px; }}
    .lf-popup .prow  {{ display: flex; justify-content: space-between; gap: 8px;
                        padding: 4px 0; border-bottom: 1px solid #f1f5f9; }}
    .lf-popup .prow:last-child {{ border: none; }}
    .lf-popup .pk    {{ color: #64748b; }}
    .lf-popup .pv    {{ font-weight: 600; color: #0f172a; text-align: right; }}
    .lf-popup .prfwd {{ color: #2563eb; font-size: 12px; }}
    .lf-popup .prback{{ color: #ea580c; font-size: 12px; }}

    /* ── MI NODO MARKER ── */
    .home-icon {{
      width: 16px; height: 16px;
      background: var(--ok);
      border: 2px solid #fff;
      border-radius: 50%;
      box-shadow: 0 0 0 3px rgba(34,197,94,.35);
    }}

    /* ── PANEL MENSAJES ── */
    #msg-panel {{
      flex-shrink: 0;
      height: 30%;
      border-top: 2px solid var(--border);
      display: flex;
      flex-direction: column;
      background: var(--bg);
    }}
    .msg-header {{
      padding: 6px 12px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .08em;
      color: var(--muted);
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }}
    #msg-list {{
      flex: 1;
      overflow-y: auto;
      padding: 6px 10px;
      display: flex;
      flex-direction: column;
    }}
    #msg-list::-webkit-scrollbar {{ width: 4px; }}
    #msg-list::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 2px; }}
    .msg-row {{
      padding: 5px 0;
      border-bottom: 1px solid var(--border);
      font-size: 12px;
      line-height: 1.4;
    }}
    .msg-row:last-child {{ border: none; }}
    .msg-meta {{
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 1px;
    }}
    .msg-sender {{ color: var(--accent); font-family: var(--mono); font-size: 11px; }}
    .msg-ch     {{ color: var(--muted); font-size: 10px; margin-left: 4px; }}
    .msg-text   {{ color: var(--text); word-break: break-word; }}
  </style>
</head>
<body>
<div class="wrap">
  <div id="map"></div>
  <div id="side">

    <div class="side-header">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
        <div>
          <div class="side-title">mapa-mesh</div>
          <div id="conn" class="pill">conectando…</div>
        </div>
        <img src="/logo" alt="MeshArg"
             style="height:58px;width:58px;object-fit:contain;border-radius:8px;flex-shrink:0"/>
      </div>
    </div>

    <div class="stats-bar">
      <div class="stat-box">
        <div class="stat-num" id="st-total">0</div>
        <div class="stat-lbl">nodos</div>
      </div>
      <div class="stat-box">
        <div class="stat-num" id="st-gps">0</div>
        <div class="stat-lbl">con GPS</div>
      </div>
      <div class="stat-box">
        <div class="stat-num" id="st-routes">0</div>
        <div class="stat-lbl">rutas</div>
      </div>
    </div>
    <div class="stats-bar">
      <div class="stat-box">
        <div class="stat-num" id="st-rssi" style="font-size:11px;font-weight:700;line-height:1.4">—</div>
        <div class="stat-num" id="st-snr"  style="font-size:11px;font-weight:700;line-height:1.4">—</div>
        <div class="stat-lbl">RSSI / SNR</div>
      </div>
      <div class="stat-box">
        <div class="stat-num" id="st-viewers">0</div>
        <div class="stat-lbl">viendo</div>
      </div>
      <div class="stat-box">
        <div class="stat-num" id="st-msgs24h">0</div>
        <div class="stat-lbl">msgs 6hs</div>
      </div>
    </div>

    <div class="legend">
      <div class="leg-item"><div class="leg-line fwd"></div> ida</div>
      <div class="leg-item"><div class="leg-line back"></div> vuelta</div>
      <div style="margin-left:auto;display:flex;gap:5px;flex-shrink:0">
        <a href="/export/routes.csv" download title="Exportar rutas (traceroutes)"
           style="font-size:11px;color:var(--muted);text-decoration:none;
                  border:1px solid var(--border);border-radius:4px;padding:2px 8px;
                  white-space:nowrap;transition:color .15s,border-color .15s"
           onmouseover="this.style.color='var(--text)';this.style.borderColor='var(--text)'"
           onmouseout="this.style.color='var(--muted)';this.style.borderColor='var(--border)'">
          ↓ rutas
        </a>
        <a href="/export/nodes.csv" download title="Exportar nodos con GPS"
           style="font-size:11px;color:var(--muted);text-decoration:none;
                  border:1px solid var(--border);border-radius:4px;padding:2px 8px;
                  white-space:nowrap;transition:color .15s,border-color .15s"
           onmouseover="this.style.color='var(--text)';this.style.borderColor='var(--text)'"
           onmouseout="this.style.color='var(--muted)';this.style.borderColor='var(--border)'">
          ↓ nodos
        </a>
      </div>
    </div>

    <div class="list-label">Nodos escuchados</div>
    <div id="list"></div>

    <div id="msg-panel">
      <div class="msg-header">Mensajes recibidos</div>
      <div id="msg-list"></div>
    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
  integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>

<script>
// ─── Mapa ────────────────────────────────────────────────────────────────────
const HOME = [{MAP_CENTER_LAT}, {MAP_CENTER_LON}];
const map  = L.map('map').setView(HOME, {MAP_CENTER_ZOOM});

L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '© OpenStreetMap contributors'
}}).addTo(map);

// Marker de MI nodo (fijo, verde)
const homeIcon = L.divIcon({{ className: '', html: '<div class="home-icon"></div>', iconSize: [16,16] }});
L.marker(HOME, {{ icon: homeIcon }})
  .addTo(map)
  .bindPopup('<b>Mi nodo</b><br><small>{HOME_LAT}, {HOME_LON}</small>');

// ─── Estado ──────────────────────────────────────────────────────────────────
const markers = new Map();   // node_id → Leaflet marker
const routes  = new Map();   // node_id → {{ fwd: Polyline, back: Polyline }}
let   nodeIndex = {{}};        // snapshot plano: node_id → {{short_name, long_name, lat, lon}}

// ─── Íconos por role — misma gota de Leaflet, distinto color vía filtro CSS ──
//
// Leaflet usa un PNG azul hardcodeado. Para colorearlo sin reemplazar la imagen
// usamos filter CSS: hue-rotate + saturate sobre un divIcon que envuelve
// la imagen original de Leaflet.
//
// Azul default (sin filtro): CLIENT, CLIENT_HIDDEN, desconocido
// Rojo:                       ROUTER, ROUTER_LATE
// Amarillo:                   CLIENT_BASE
//
const LEAFLET_MARKER_URL = "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png";
const LEAFLET_SHADOW_URL = "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png";

function markerIcon(role) {{
  const r = (role || "").toUpperCase();

  // filtro CSS para llevar el azul de Leaflet a otro color
  // Azul Leaflet ≈ hue 210°
  // → Rojo:     hue-rotate(-210deg) saturate(2)
  // → Amarillo: hue-rotate(-150deg) saturate(2) brightness(1.1)
  let filter = null;
  if (r === "ROUTER" || r === "ROUTER_LATE") {{
    filter = "hue-rotate(-210deg) saturate(2)";
  }} else if (r === "CLIENT_BASE") {{
    filter = "hue-rotate(-150deg) saturate(2) brightness(1.15)";
  }}

  if (!filter) return null;  // null → Leaflet default sin tocar

  return L.divIcon({{
    className: "",
    html: `
      <div style="position:relative;width:25px;height:41px;">
        <img src="${{LEAFLET_MARKER_URL}}"
             style="width:25px;height:41px;filter:${{filter}}"
             draggable="false"/>
        <img src="${{LEAFLET_SHADOW_URL}}"
             style="position:absolute;top:0;left:-10px;width:41px;height:41px;opacity:.4;pointer-events:none"
             draggable="false"/>
      </div>`,
    iconSize:    [25, 41],
    iconAnchor:  [12, 41],
    popupAnchor: [1, -34],
    shadowSize:  [41, 41],
  }});
}}

// ─── Helpers ─────────────────────────────────────────────────────────────────
function fmtAge(sec) {{
  if (sec == null) return "n/a";
  sec = Math.floor(sec);
  if (sec < 60)  return sec + "s";
  const m = Math.floor(sec/60), s = sec%60;
  if (m < 60) return m + "m " + s + "s";
  const h = Math.floor(m/60); return h + "h " + (m%60) + "m";
}}

// Escapa cualquier texto antes de insertarlo en innerHTML.
// CRÍTICO: todo dato que viene del mesh (nombres, roles, mensajes, IDs de
// nodos remotos) es entrada no confiable — cualquier nodo en la red puede
// mandar un shortName/longName/mensaje con HTML o JS embebido. Sin esto,
// un nodo malicioso podría ejecutar script arbitrario en el navegador de
// quien mire el mapa (XSS).
function escapeHtml(str) {{
  if (str == null) return "";
  const div = document.createElement("div");
  div.textContent = String(str);
  return div.innerHTML;
}}

function popupHtml(n) {{
  const sn  = escapeHtml(n.short_name) || "—";
  const ln  = escapeHtml(n.long_name)  || "";
  const ago = fmtAge(Date.now()/1000 - n.last_seen);
  const rssi = n.rssi  != null ? n.rssi.toFixed(0)  + " dBm" : "?";
  const snr  = n.snr   != null ? n.snr.toFixed(2)   + " dB"  : "?";
  const hops = n.hops  != null ? n.hops              : "?";
  const dist = n.dist_km != null ? Math.round(n.dist_km) + " km" : "?";
  const pos  = (n.lat != null && n.lon != null)
    ? n.lat.toFixed(5) + ", " + n.lon.toFixed(5) : "sin posición";

  let routeHtml = "";
  if (n.traceroute_pending) {{
    routeHtml = `<div class="prow"><span class="pk">Traceroute</span>
                 <span class="pv" style="color:#ca8a04">calculando…</span></div>`;
  }} else if (n.route) {{
    const fwdHops  = n.route.hops_forward.length  > 0
      ? n.route.hops_forward.map(resolveHopName).join(" → ") : "directo";
    const backHops = n.route.hops_back.length > 0
      ? n.route.hops_back.map(resolveHopName).join(" → ") : "directo";
    routeHtml = `
      <div class="prow">
        <span class="pk prfwd">↗ Ida</span>
        <span class="pv prfwd">${{fwdHops}}</span>
      </div>
      <div class="prow">
        <span class="pk prback">↙ Vuelta</span>
        <span class="pv prback">${{backHops}}</span>
      </div>`;
  }}

  const roleEsc = escapeHtml(n.role);
  return `<div class="lf-popup">
    <div class="pid">${{escapeHtml(n.node_id)}}${{roleEsc ? ' · <span style="color:#94a3b8">' + roleEsc + '</span>' : ""}}</div>
    <div class="pname">${{sn}}${{ln ? " — " + ln : ""}}</div>
    <div class="prow"><span class="pk">RSSI</span><span class="pv">${{rssi}}</span></div>
    <div class="prow"><span class="pk">SNR</span><span class="pv">${{snr}}</span></div>
    <div class="prow"><span class="pk">Saltos radio</span><span class="pv">${{hops}}</span></div>
    <div class="prow"><span class="pk">Distancia</span><span class="pv">${{dist}}</span></div>
    <div class="prow"><span class="pk">Último</span><span class="pv">${{ago}}</span></div>
    <div class="prow"><span class="pk">Posición</span><span class="pv">${{pos}}</span></div>
    ${{routeHtml}}
  </div>`;
}}

// Resuelve el nombre legible de un nodo a partir del nodeIndex.
// Siempre devuelve texto ya escapado — se usa directamente en innerHTML.
function resolveHopName(node_id) {{
  const safeId = escapeHtml(node_id);
  const info = nodeIndex[node_id];
  if (!info) return safeId;
  return escapeHtml(info.short_name) || escapeHtml(info.long_name) || safeId;
}}

function drawRoutes(n) {{
  // Limpiar rutas anteriores de este nodo
  if (routes.has(n.node_id)) {{
    const old = routes.get(n.node_id);
    if (old.fwd)  map.removeLayer(old.fwd);
    if (old.back) map.removeLayer(old.back);
    routes.delete(n.node_id);
  }}

  if (!n.route || n.lat == null || n.lon == null) return;

  const dest = [n.lat, n.lon];

  // Construir labels con nombres resueltos (todos los saltos, tengan GPS o no)
  function buildLabel(hops, prefix) {{
    if (hops.length === 0) return prefix + ": directo";
    return prefix + ": " + hops.map(resolveHopName).join(" → ");
  }}

  const fwdLabel  = buildLabel(n.route.hops_forward, "Ida");
  const backLabel = buildLabel(n.route.hops_back,    "Vuelta");

  // Construir lista de coordenadas para una ruta:
  // Incluye solo los nodos intermedios que tienen GPS conocido (Opción A).
  // Los que no tienen GPS se omiten del trazado pero siguen en el tooltip.
  function buildCoords(hops, origin, destination, off) {{
    const pts = [origin];
    for (const hop_id of hops) {{
      const info = nodeIndex[hop_id];
      if (info && info.lat != null && info.lon != null) {{
        pts.push([info.lat + off, info.lon + off]);
      }}
    }}
    pts.push(destination);
    return pts;
  }}

  // Offset mínimo para que ida y vuelta no se superpongan exactamente
  const OFF = 0.00008;

  const fwdCoords  = buildCoords(n.route.hops_forward, HOME,                    [dest[0]+OFF, dest[1]+OFF],  OFF);
  const backCoords = buildCoords(n.route.hops_back,    [dest[0]-OFF, dest[1]-OFF], HOME,                   -OFF);

  const fwdLine = L.polyline(fwdCoords,
    {{ color: '#3b82f6', weight: 3, opacity: .85 }}
  ).addTo(map).bindTooltip(fwdLabel, {{ sticky: true }});

  const backLine = L.polyline(backCoords,
    {{ color: '#f97316', weight: 3, opacity: .85, dashArray: '6 4' }}
  ).addTo(map).bindTooltip(backLabel, {{ sticky: true }});

  routes.set(n.node_id, {{ fwd: fwdLine, back: backLine }});
}}

function renderNodes(data) {{
  // Reconstruir índice plano para resolución de nombres en tooltips
  nodeIndex = {{}};
  for (const n of data) {{
    nodeIndex[n.node_id] = {{
      short_name: n.short_name,
      long_name:  n.long_name,
      lat:        n.lat,
      lon:        n.lon,
    }};
  }}

  // Stats
  const total  = data.length;
  const withGPS = data.filter(n => n.lat != null && n.lon != null).length;
  const withRoute = data.filter(n => n.route != null).length;
  document.getElementById("st-total").textContent  = total;
  document.getElementById("st-gps").textContent    = withGPS;
  document.getElementById("st-routes").textContent = withRoute;

  // Markers + rutas
  const seen = new Set();
  for (const n of data) {{
    if (n.lat == null || n.lon == null) continue;
    seen.add(n.node_id);
    const pos = [n.lat, n.lon];

    if (!markers.has(n.node_id)) {{
      const icon = markerIcon(n.role);
      const mk = icon
        ? L.marker(pos, {{ icon }}).addTo(map).bindPopup(popupHtml(n))
        : L.marker(pos).addTo(map).bindPopup(popupHtml(n));
      markers.set(n.node_id, {{ mk, role: n.role || "" }});
    }} else {{
      const entry = markers.get(n.node_id);
      const mk = entry.mk || entry;   // compat con markers previos sin wrapper
      mk.setLatLng(pos);
      mk.setPopupContent(popupHtml(n));
      // Si el role cambió, actualizar el ícono
      const prevRole = entry.role || "";
      if ((n.role || "") !== prevRole) {{
        const icon = markerIcon(n.role);
        if (icon) mk.setIcon(icon);
        if (entry.mk) entry.role = n.role || "";
      }}
    }}
    drawRoutes(n);
  }}

  // Limpiar markers de nodos que desaparecieron
  for (const [id, entry] of markers.entries()) {{
    if (!seen.has(id)) {{
      const mk = entry.mk || entry;
      map.removeLayer(mk);
      markers.delete(id);
      if (routes.has(id)) {{
        const r = routes.get(id);
        if (r.fwd)  map.removeLayer(r.fwd);
        if (r.back) map.removeLayer(r.back);
        routes.delete(id);
      }}

    }}
  }}

  // Lista sidebar — orden: más recientes abajo
  data.sort((a,b) => a.last_seen - b.last_seen);
  const list = document.getElementById("list");
  list.innerHTML = "";

  for (const n of data) {{
    const ago     = fmtAge(Date.now()/1000 - n.last_seen);
    const hasPos  = n.lat != null && n.lon != null;
    const rssi    = n.rssi  != null ? n.rssi.toFixed(0)  : "?";
    const snr     = n.snr   != null ? n.snr.toFixed(1)   : "?";
    const hops    = n.hops  != null ? n.hops              : "?";
    const dist    = n.dist_km != null ? Math.round(n.dist_km) + " km" : "?";

    let routeLine = "";
    if (n.traceroute_pending) {{
      routeLine = `<span class="badge-pending">⏳ traceroute…</span>`;
    }} else if (n.route) {{
      routeLine = `<span class="badge-fwd">↗${{n.route.hop_count_fwd}} saltos</span>
                   <span class="badge-back"> ↙${{n.route.hop_count_back}} saltos</span>`;
    }}

    const safeNodeId = escapeHtml(n.node_id);
    const safeName    = escapeHtml(n.short_name) || escapeHtml(n.long_name) || "(sin nombre)";

    const div = document.createElement("div");
    div.className = "node-row" + (hasPos ? "" : " no-gps");
    div.innerHTML = `
      <div class="node-id">${{safeNodeId}}</div>
      <div class="node-name">${{safeName}}</div>
      <div class="node-meta">
        ${{ago}} · RSSI ${{rssi}} · SNR ${{snr}} · H:${{hops}} · ${{dist}}
        ${{hasPos ? "" : " · <em>sin GPS</em>"}}
      </div>
      ${{routeLine ? '<div class="node-route">' + routeLine + '</div>' : ""}}
    `;
    if (hasPos) {{
      div.onclick = () => {{
        map.setView([n.lat, n.lon], Math.max(map.getZoom(), 14));
        const e = markers.get(n.node_id); (e?.mk || e)?.openPopup();
      }};
    }}
    list.appendChild(div);
  }}

  // Auto-scroll al nodo más reciente solo si el usuario no está scrolleando
  if (!listUserScrolled) {{
    list.scrollTop = list.scrollHeight;
  }}
}}

function updateStatus(st) {{
  const el = document.getElementById("conn");
  if (!el) return;
  if (st.connected) {{
    const age = fmtAge(st.last_packet_age_sec);
    el.className = "pill ok";
    el.textContent = "Conectado · " + age;
  }} else {{
    el.className = "pill bad";
    el.textContent = "Desconectado" + (st.last_error ? ": " + st.last_error : "");
  }}

  // Segunda fila de stats
  const rssiEl = document.getElementById("st-rssi");
  const snrEl  = document.getElementById("st-snr");
  if (rssiEl) rssiEl.textContent = st.avg_rssi != null ? st.avg_rssi + " dBm" : "—";
  if (snrEl)  snrEl.textContent  = st.avg_snr  != null ? st.avg_snr  + " dB"  : "—";

  const viewEl = document.getElementById("st-viewers");
  if (viewEl) viewEl.textContent = st.viewers != null ? st.viewers : "—";

  const msgsEl = document.getElementById("st-msgs24h");
  if (msgsEl) msgsEl.textContent = st.msgs_24h != null ? st.msgs_24h : "—";
}}

function fmtTime(ts) {{
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.getHours().toString().padStart(2,"0") + ":" +
         d.getMinutes().toString().padStart(2,"0") + ":" +
         d.getSeconds().toString().padStart(2,"0");
}}

// Scroll inteligente: auto-scroll al fondo, se suspende si el usuario sube
let msgUserScrolled  = false;
let listUserScrolled = false;

function initMsgScroll() {{
  const msgList = document.getElementById("msg-list");
  if (msgList) {{
    msgList.addEventListener("scroll", () => {{
      const atBottom = msgList.scrollHeight - msgList.scrollTop - msgList.clientHeight < 40;
      msgUserScrolled = !atBottom;
    }});
  }}

  const nodeList = document.getElementById("list");
  if (nodeList) {{
    nodeList.addEventListener("scroll", () => {{
      const atBottom = nodeList.scrollHeight - nodeList.scrollTop - nodeList.clientHeight < 40;
      listUserScrolled = !atBottom;
    }});
  }}
}}

function renderMessages(msgs) {{
  const list = document.getElementById("msg-list");
  if (!list) return;
  if (!msgs || msgs.length === 0) {{
    list.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:8px 0">Sin mensajes aún</div>';
    return;
  }}

  // Ordenar cronológico: el más reciente queda abajo
  const sorted = [...msgs].sort((a, b) => a.timestamp - b.timestamp);
  list.innerHTML = "";

  for (const m of sorted) {{
    const div = document.createElement("div");
    div.className = "msg-row";
    const safeSender = escapeHtml(m.from_name) || escapeHtml(m.from_id);
    const safeText   = escapeHtml(m.text);
    const safeCh     = m.channel > 0 ? `<span class="msg-ch">ch${{escapeHtml(m.channel)}}</span>` : "";
    div.innerHTML = `
      <div class="msg-meta">
        <span class="msg-sender">${{safeSender}}</span>
        ${{safeCh}}
        <span style="float:right;color:var(--muted);font-size:10px">${{fmtTime(m.timestamp)}}</span>
      </div>
      <div class="msg-text">${{safeText}}</div>
    `;
    list.appendChild(div);
  }}

  // Auto-scroll al fondo solo si el usuario no está mirando mensajes anteriores
  if (!msgUserScrolled) {{
    list.scrollTop = list.scrollHeight;
  }}
}}

// Inicializar detector de scroll cuando el DOM esté listo
initMsgScroll();

// ─── Socket.IO — actualizaciones en tiempo real ───────────────────────────────
const socket = io();

socket.on("nodes_update", (payload) => {{
  updateStatus(payload.status || {{}});
  renderNodes(payload.nodes || []);
  renderMessages(payload.messages || []);
}});

// ─── Polling de fallback (si Socket.IO no está disponible) ───────────────────
async function poll() {{
  try {{
    const r    = await fetch("/api/nodes", {{ cache: "no-store" }});
    const data = await r.json();
    updateStatus(data.status || {{}});
    renderNodes(data.nodes  || []);
    renderMessages(data.messages || []);
  }} catch(e) {{
    document.getElementById("conn").className = "pill bad";
    document.getElementById("conn").textContent = "Error: " + e;
  }}
}}

poll();
setInterval(poll, {int(POLL_REFRESH_SEC * 1000)});
</script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


# =============================================================================
#                        BACKUP DE ESTADO PERIÓDICO
# =============================================================================
#
# El reinicio del proceso lo maneja systemd (Restart=always en el .service).
# El watchdog interno fue eliminado porque causaba doble instancia al
# interactuar con el reinicio automático de systemd a medianoche.
#
# En su lugar, el backup se guarda periódicamente cada 15 minutos para
# que al reiniciar (por systemd o manual) se restaure el estado reciente.
#
# =============================================================================

BACKUP_INTERVAL_SEC = 15 * 60  # guardar estado cada 15 minutos


def save_state_backup():
    """
    Guarda el estado actual de nodos (con rutas) en BACKUP_FILE como JSON.
    """
    import json as _json
    try:
        data = []
        with nodes_lock:
            for nid, e in nodes.items():
                entry = {
                    "node_id":    e.node_id,
                    "short_name": e.short_name,
                    "long_name":  e.long_name,
                    "lat":        e.lat,
                    "lon":        e.lon,
                    "alt":        e.alt,
                    "role":       e.role,
                    "rssi":       e.rssi,
                    "snr":        e.snr,
                    "hops":       e.hops,
                    "dist_km":    e.dist_km,
                    "last_seen":  e.last_seen,
                    "route":      None,
                }
                if e.route:
                    entry["route"] = {
                        "hops_forward":   e.route.hops_forward,
                        "hops_back":      e.route.hops_back,
                        "hop_count_fwd":  e.route.hop_count_fwd,
                        "hop_count_back": e.route.hop_count_back,
                        "timestamp":      e.route.timestamp,
                    }
                data.append(entry)

        with open(BACKUP_FILE, "w", encoding="utf-8") as f:
            _json.dump({"saved_at": now(), "nodes": data}, f, ensure_ascii=False, indent=2)

        log.info(f"Backup guardado: {len(data)} nodos → {BACKUP_FILE}")
    except Exception as e:
        log.error(f"Error guardando backup: {e}")


def load_state_backup():
    """
    Carga el backup JSON al arrancar. Restaura nodos y rutas en memoria.
    Si el archivo no existe o está corrupto, arranca limpio.
    """
    import json as _json
    if not _os.path.exists(BACKUP_FILE):
        log.info("Sin backup previo, arrancando limpio")
        return

    try:
        with open(BACKUP_FILE, "r", encoding="utf-8") as f:
            data = _json.load(f)

        saved_at = data.get("saved_at", 0)
        age_h    = (now() - saved_at) / 3600
        entries  = data.get("nodes", [])

        if (now() - saved_at) > PRUNE_AFTER_SEC:
            log.info(f"Backup demasiado viejo ({age_h:.1f}h), ignorando")
            return

        loaded = 0
        with nodes_lock:
            for e in entries:
                nid = e.get("node_id", "")
                if not nid:
                    continue
                entry = NodeEntry(node_id=nid)
                entry.short_name = e.get("short_name") or ""
                entry.long_name  = e.get("long_name")  or ""
                entry.lat        = e.get("lat")
                entry.lon        = e.get("lon")
                entry.alt        = e.get("alt")
                entry.role       = e.get("role")       or ""
                entry.rssi       = e.get("rssi")
                entry.snr        = e.get("snr")
                entry.hops       = e.get("hops")
                entry.dist_km    = e.get("dist_km")
                entry.last_seen  = e.get("last_seen")  or now()

                r = e.get("route")
                if r:
                    entry.route = RouteInfo(
                        hops_forward   = r.get("hops_forward",  []),
                        hops_back      = r.get("hops_back",     []),
                        hop_count_fwd  = r.get("hop_count_fwd",  0),
                        hop_count_back = r.get("hop_count_back", 0),
                        timestamp      = r.get("timestamp",      0.0),
                    )
                nodes[nid] = entry
                loaded += 1

        log.info(f"Backup restaurado: {loaded} nodos (guardado hace {age_h:.1f}h)")
    except Exception as e:
        log.error(f"Error cargando backup: {e}")


def backup_thread():
    """
    Guarda el estado periódicamente cada BACKUP_INTERVAL_SEC.
    Reemplaza al watchdog interno — systemd maneja los reinicios.
    """
    while True:
        time.sleep(BACKUP_INTERVAL_SEC)
        save_state_backup()


# =============================================================================
#                               MAIN
# =============================================================================

def main():
    # Restaurar estado desde backup si existe
    load_state_backup()

    # Hilo de conexión Meshtastic
    t_mesh = threading.Thread(target=meshtastic_thread, daemon=True)
    t_mesh.start()

    # Worker serializado de traceroutes (único hilo, procesa de a uno)
    t_tr = threading.Thread(target=traceroute_worker, daemon=True, name="traceroute-worker")
    t_tr.start()

    # Heartbeat periódico: NodeInfo broadcast al arrancar y cada 10 minutos
    t_hb = threading.Thread(target=nodeinfo_heartbeat_thread, daemon=True, name="nodeinfo-heartbeat")
    t_hb.start()

    # Traceroute periódico: re-traza todos los nodos con GPS cada 5 minutos
    t_ptr = threading.Thread(target=periodic_traceroute_thread, daemon=True, name="traceroute-periodic")
    t_ptr.start()

    # Reorganización de la cola de traceroutes por distancia cada 15 minutos
    t_reorg = threading.Thread(target=traceroute_queue_reorg_thread, daemon=True, name="traceroute-queue-reorg")
    t_reorg.start()

    # Backup periódico cada 15 minutos (systemd maneja los reinicios)
    t_bk = threading.Thread(target=backup_thread, daemon=True, name="backup")
    t_bk.start()

    log.info(f"Servidor en https://{BIND_HOST}:{BIND_PORT}")
    import os as _ssl_os
    _SSL_CERT = _ssl_os.path.join(_ssl_os.path.dirname(_ssl_os.path.abspath(__file__)), "ssl", "mapa-mesh_hopto_org.pem")
    _SSL_KEY  = _ssl_os.path.join(_ssl_os.path.dirname(_ssl_os.path.abspath(__file__)), "ssl", "mapa-mesh.key")

    if _ssl_os.path.exists(_SSL_CERT) and _ssl_os.path.exists(_SSL_KEY):
        log.info(f"Usando certificado SSL: {_SSL_CERT}")
        _ssl_ctx = (_SSL_CERT, _SSL_KEY)
    else:
        log.warning("Certificados SSL no encontrados, usando adhoc (autofirmado)")
        _ssl_ctx = 'adhoc'

    socketio.run(app, host=BIND_HOST, port=BIND_PORT, debug=False, use_reloader=False, ssl_context=_ssl_ctx, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
