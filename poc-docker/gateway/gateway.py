"""
Gateway de borde de MetalSur 4.0 (una planta).

Genera cada segundo una lectura por sensor (EQUIPOS x 8 sensores) en dos formatos
(v1 y v2), con una pequena proporcion de mensajes corruptos y de valores anomalos,
y las publica en el stream `telemetria` del broker.

  MODO=asis  envia directo al broker, sin almacenamiento local: si no hay enlace,
             la lectura se pierde (perdida silenciosa).
  MODO=tobe  store-and-forward: guarda primero en SQLite (volumen persistente),
             envia el dato en vivo con prioridad y reenvia el acumulado a una tasa
             limitada (TASA_VACIADO). Una fraccion de confirmaciones se pierde
             (PROB_ACK_PERDIDO), lo que provoca reenvios duplicados.

El estado (contadores, corte observado y la "verdad" por ventana para verificar
los consolidados) se escribe en /data/estado.json. Crear /data/detener hace que
deje de generar lecturas; el vaciado del acumulado continua.
"""
import json
import os
import random
import sqlite3
import threading
import time
from collections import defaultdict

import redis

MODO = os.environ.get("MODO", "tobe").lower()
PLANTA = os.environ.get("PLANTA", "P1")
EQUIPOS = int(os.environ.get("EQUIPOS", "50"))
SENSORES = int(os.environ.get("SENSORES", "8"))
TASA_VACIADO = int(os.environ.get("TASA_VACIADO", "800"))
PROB_ACK_PERDIDO = float(os.environ.get("PROB_ACK_PERDIDO", "0.01"))
PROB_CORRUPTO = float(os.environ.get("PROB_CORRUPTO", "0.001"))
PROB_ANOMALIA = float(os.environ.get("PROB_ANOMALIA", "0.0005"))
UMBRAL = float(os.environ.get("UMBRAL_ANOMALIA", "95"))
VENTANA_SEG = int(os.environ.get("VENTANA_SEG", "30"))
LOG_SEG = int(os.environ.get("LOG_SEG", "2"))
DATA = "/data"

if MODO not in ("asis", "tobe"):
    raise SystemExit(f"MODO debe ser 'asis' o 'tobe', no '{MODO}'")

r = redis.Redis(host="redis", socket_timeout=1, socket_connect_timeout=1)
db = sqlite3.connect(f"{DATA}/buffer.db", check_same_thread=False)
db.execute("PRAGMA journal_mode=WAL")
db.execute("CREATE TABLE IF NOT EXISTS pendientes (id TEXT PRIMARY KEY, ts INTEGER, msg TEXT)")
db.execute("CREATE INDEX IF NOT EXISTS pendientes_ts ON pendientes (ts)")
db.commit()

lock = threading.Lock()          # protege la base local y los contadores
rnd = random.Random()
est = {
    "modo": MODO, "equipos": EQUIPOS, "sensores": SENSORES,
    "generadas": 0, "corruptas": 0, "validas": 0,
    "perdidas": 0, "enviadas": 0, "reenviadas": 0, "alertas_locales": 0,
    "en_buffer": 0, "buffer_max": 0,
    "enlace_ok": True, "corte_inicio": None, "corte_fin": None, "vaciado_fin": None,
    "detenido": False,
}
verdad = defaultdict(lambda: [0, 0.0])   # ventana -> [lecturas validas, suma de valores normalizados]


def log(msg):
    print(f"[gateway {MODO}] {msg}", flush=True)


def detener_pedido():
    return os.path.exists(f"{DATA}/detener")


def publicar(mensajes):
    p = r.pipeline(transaction=False)
    for m in mensajes:
        p.xadd("telemetria", {"d": m})
    p.execute()


def registrar_enlace(ok):
    ahora = time.time()
    if not ok and est["enlace_ok"]:
        est.update(enlace_ok=False, corte_inicio=ahora, corte_fin=None, vaciado_fin=None)
        log("enlace CAIDO: no se puede publicar en el broker")
    elif ok and not est["enlace_ok"]:
        est.update(enlace_ok=True, corte_fin=ahora)
        log(f"enlace RESTABLECIDO tras {ahora - est['corte_inicio']:.0f} s")


def crear_lectura(e, s, t):
    """Devuelve (id, mensaje JSON, valor normalizado o None si es corrupta)."""
    valor = 50 + 10 * rnd.random() + (45 if rnd.random() < PROB_ANOMALIA else 0)
    equipo, sensor = f"E{e:03d}", f"S{s}"
    if s % 2 == 0:   # formato v1: campos en espanol, valor en escala 0-100
        m = {"fmt": "v1", "planta": PLANTA, "equipo": equipo, "sensor": sensor,
             "seq": t, "ts": t, "valor": round(valor, 3)}
        normal, campo_seq = m["valor"], "seq"
    else:            # formato v2 (sensor nuevo): campos en ingles, escala 0-1000
        m = {"fmt": "v2", "plant": PLANTA, "asset": equipo, "channel": sensor,
             "n": t, "epoch": t, "raw": round(valor * 10, 2)}
        normal, campo_seq = m["raw"] / 10, "n"
    if rnd.random() < PROB_CORRUPTO:
        m.pop(campo_seq)                 # mensaje que no cumple el contrato
        normal = None
    return f"{PLANTA}-{equipo}-{sensor}-{t}", json.dumps(m), normal


def enviar(filas, reenvio=False):
    """Publica filas (id, ts, msg) y borra del buffer las que el broker confirma."""
    try:
        publicar([m for _, _, m in filas])
    except redis.RedisError:
        registrar_enlace(False)
        return
    registrar_enlace(True)
    # confirmacion perdida simulada: esas filas quedan y se reenvian (duplicados)
    confirmadas = [(i,) for i, _, _ in filas if rnd.random() >= PROB_ACK_PERDIDO]
    with lock:
        db.executemany("DELETE FROM pendientes WHERE id = ?", confirmadas)
        db.commit()
        est["enviadas"] += len(filas)
        if reenvio:
            est["reenviadas"] += len(filas)


def generar():
    t = int(time.time()) + 1
    while not detener_pedido():
        espera = t - time.time()
        if espera > 0:
            time.sleep(espera)
        lote, corruptas = [], 0
        for e in range(EQUIPOS):
            for s in range(SENSORES):
                ident, msg, normal = crear_lectura(e, s, t)
                lote.append((ident, t, msg))
                if normal is None:
                    corruptas += 1
                    continue
                v = verdad[t // VENTANA_SEG]
                v[0] += 1
                v[1] += normal
                if MODO == "tobe" and normal >= UMBRAL:
                    est["alertas_locales"] += 1      # alerta por umbral en el borde
        with lock:
            est["generadas"] += len(lote)
            est["corruptas"] += corruptas
            est["validas"] += len(lote) - corruptas

        if MODO == "asis":
            try:
                publicar([m for _, _, m in lote])
                registrar_enlace(True)
                with lock:
                    est["enviadas"] += len(lote)
            except redis.RedisError:
                registrar_enlace(False)
                with lock:
                    est["perdidas"] += len(lote)     # sin buffer: se pierden
        else:
            with lock:                               # primero se guarda...
                db.executemany("INSERT INTO pendientes VALUES (?, ?, ?)", lote)
                db.commit()
            enviar(lote)                             # ...y despues se envia el dato en vivo
        t += 1
    est["detenido"] = True
    log("generacion detenida por pedido (/data/detener)")


def vaciar():
    """Reenvia el acumulado (lo no confirmado) a una tasa limitada y en desorden."""
    while True:
        inicio = time.monotonic()
        with lock:
            filas = db.execute(
                "SELECT id, ts, msg FROM pendientes WHERE ts < ? ORDER BY ts LIMIT ?",
                (time.time() - 2, TASA_VACIADO)).fetchall()
        if filas:
            rnd.shuffle(filas)                       # los lotes llegan fuera de orden
            enviar(filas, reenvio=True)
        time.sleep(max(0.0, 1 - (time.monotonic() - inicio)))


def guardar_estado():
    with lock:
        est["en_buffer"] = db.execute("SELECT COUNT(*) FROM pendientes").fetchone()[0]
        if est["corte_fin"] and not est["vaciado_fin"]:
            atrasadas = db.execute("SELECT COUNT(*) FROM pendientes WHERE ts <= ?",
                                   (est["corte_fin"],)).fetchone()[0]
            if atrasadas == 0:
                est["vaciado_fin"] = time.time()
                log(f"acumulado vaciado en {est['vaciado_fin'] - est['corte_fin']:.0f} s")
        est["buffer_max"] = max(est["buffer_max"], est["en_buffer"])
        datos = dict(est, verdad={str(k): v for k, v in verdad.items()})
    tmp = f"{DATA}/estado.json.tmp"
    with open(tmp, "w") as f:
        json.dump(datos, f)
    os.replace(tmp, f"{DATA}/estado.json")


if __name__ == "__main__":
    log(f"inicio: {EQUIPOS} equipos x {SENSORES} sensores = {EQUIPOS * SENSORES} lecturas/s")
    if MODO == "tobe":
        threading.Thread(target=vaciar, daemon=True).start()
    threading.Thread(target=generar, daemon=True).start()
    n = 0
    while True:
        guardar_estado()
        if n % LOG_SEG == 0:
            log(f"generadas={est['generadas']} perdidas={est['perdidas']} "
                f"en_buffer={est['en_buffer']} enlace={'OK' if est['enlace_ok'] else 'CAIDO'}")
        n += 1
        time.sleep(1)
