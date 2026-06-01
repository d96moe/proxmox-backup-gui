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
    with MQTT_CACHE_LOCK:
        if payload:
            if msg.topic.endswith("/log"):
                log_list = MQTT_CACHE.setdefault(msg.topic, [])
                if isinstance(log_list, list):
                    log_list.append(payload)
            else:
                try:
                    MQTT_CACHE[msg.topic] = json.loads(payload)
                except json.JSONDecodeError:
                    MQTT_CACHE[msg.topic] = payload
        else:
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

    # clean_session=False + stable client_id so queued QoS-1 publishes survive
    # a reconnect (with clean_session=True the broker drops the session and any
    # in-flight messages are lost — this caused cmd/restore to vanish in CI).
    client = paho_mqtt.Client(
        client_id="gui-backend-global",
        protocol=paho_mqtt.MQTTv311,
        clean_session=False,
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
    if not _global_client:
        log.warning("publish_cmd failed: Global MQTT client not initialized")
        return
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    # The network loop can drop the connection (e.g. missed keepalive while a
    # long job blocked a thread). A QoS-1 publish on a disconnected client is
    # silently lost with clean_session=True, so reconnect first if needed.
    if not _global_client.is_connected():
        log.warning("publish_cmd: global MQTT client disconnected — reconnecting before publish to %s", topic)
        try:
            _global_client.reconnect()
        except Exception as e:
            log.error("publish_cmd reconnect failed: %s", e)
    info = _global_client.publish(topic, payload, qos=1)
    try:
        info.wait_for_publish(timeout=10)
    except Exception as e:
        log.error("publish_cmd wait_for_publish error on %s: %s", topic, e)
    log.info("publish_cmd: topic=%s rc=%s mid=%s connected=%s",
             topic, info.rc, info.mid, _global_client.is_connected())
