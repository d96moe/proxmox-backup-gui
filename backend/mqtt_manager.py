import json
import logging
import queue
import threading
import paho.mqtt.client as paho_mqtt

log = logging.getLogger(__name__)

MQTT_CACHE = {}
MQTT_CACHE_LOCK = threading.Lock()

WS_CLIENTS = set()
WS_LOCK = threading.Lock()

_global_client = None

def _on_connect(prefix):
    def callback(c, _u, _f, rc):
        if rc == 0:
            c.subscribe(f"{prefix}/#", qos=1)
            log.info("MQTT Global Client connected and subscribed to %s/#", prefix)
        else:
            log.error("MQTT Global Client failed to connect, rc=%s", rc)
    return callback

def _on_message(c, _u, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
    except Exception:
        payload = ""

    # Update Cache
    if msg.retain:
        with MQTT_CACHE_LOCK:
            if payload:
                try:
                    MQTT_CACHE[msg.topic] = json.loads(payload)
                except json.JSONDecodeError:
                    MQTT_CACHE[msg.topic] = payload
            else:
                MQTT_CACHE.pop(msg.topic, None)
    elif not payload:
        with MQTT_CACHE_LOCK:
            MQTT_CACHE.pop(msg.topic, None)

    # Broadcast to WebSockets
    with WS_LOCK:
        dead = set()
        for q in WS_CLIENTS:
            try:
                q.put_nowait({"topic": msg.topic, "payload": payload})
            except queue.Full:
                dead.add(q)
        for q in dead:
            WS_CLIENTS.discard(q)

def init_mqtt(host: str, port: int, user: str, password: str, prefix: str):
    global _global_client
    if _global_client is not None:
        return

    _cbv = getattr(paho_mqtt, "CallbackAPIVersion", None)
    client = paho_mqtt.Client(
        *([_cbv.VERSION1] if _cbv else []),
        client_id=f"gui-backend-global",
        protocol=paho_mqtt.MQTTv311,
    )
    if user and password:
        client.username_pw_set(user, password)

    client.on_connect = _on_connect(prefix)
    client.on_message = _on_message
    
    try:
        client.connect(host, port, keepalive=30)
        client.loop_start()
        _global_client = client
    except Exception as e:
        log.error("Failed to connect global MQTT client to %s:%s - %s", host, port, e)

def publish_cmd(topic: str, payload: dict | str):
    if _global_client:
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        _global_client.publish(topic, payload, qos=1)
    else:
        log.warning("publish_cmd failed: Global MQTT client not initialized")
