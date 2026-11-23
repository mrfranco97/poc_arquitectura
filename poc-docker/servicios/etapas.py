"""
Pipeline de telemetria de MetalSur 4.0 (Pipes & Filters) y batch de consolidacion.

Una misma imagen ejecuta cualquier etapa; la variable ETAPA elige cual. Cada filtro
lee de un stream de Redis con su propio grupo de consumidores, escribe su salida y
recien despues confirma (XACK): la entrega es al menos una vez. Cada filtro procesa
como maximo CAPACIDAD lecturas por segundo.

  telemetria -> validacion -> validadas -> normalizacion -> normalizadas
             -> deduplicacion -> unicas -> deteccion -> evaluadas -> persistencia
  (errores permanentes de validacion y normalizacion -> stream `dlq`)

  consolidacion: batch periodico sobre el Repository (PostgreSQL) por ventana de
  event time, con marca de agua, versionado y registro de correcciones.
"""
import json
import os
import socket
import time

import psycopg2
import psycopg2.extras
import redis

ETAPA = os.environ["ETAPA"]
CAPACIDAD = int(os.environ.get("CAPACIDAD", "1500"))
EQUIPOS = int(os.environ.get("EQUIPOS", "50"))
SENSORES = int(os.environ.get("SENSORES", "8"))
VENTANA_SEG = int(os.environ.get("VENTANA_SEG", "60"))
WATERMARK_SEG = int(os.environ.get("WATERMARK_SEG", "20"))
QUIETUD_SEG = int(os.environ.get("QUIETUD_SEG", "15"))
UMBRAL = float(os.environ.get("UMBRAL_ANOMALIA", "95"))
LIMITE_ALERTA_SEG = 30                     # objetivo del enunciado: alertas en menos de 30 s
TTL_IDEMPOTENCIA = 7 * 24 * 3600           # claves conservadas 7 dias (> 72 h de reenvio)
CONSUMIDOR = socket.gethostname()

r = redis.Redis(host="redis", decode_responses=True)


def log(msg):
    print(f"[{ETAPA}] {msg}", flush=True)


def conectar_pg():
    while True:
        try:
            pg = psycopg2.connect(host="postgres", user="postgres", password="poc")
            pg.autocommit = True
            return pg
        except psycopg2.OperationalError:
            time.sleep(1)


def a_dlq(pipe, motivo, crudo):
    pipe.xadd("dlq", {"etapa": ETAPA, "motivo": motivo, "d": crudo})
    pipe.hincrby("metricas", "dlq", 1)


# ------------------------------------------------------------------ ejecucion de un filtro
def ejecutar_filtro(entrada, procesar):
    try:
        r.xgroup_create(entrada, ETAPA, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    desde = "0"            # primero lo pendiente de una ejecucion anterior, despues lo nuevo
    procesadas, atraso_max, ultimo_reporte = 0, 0, time.monotonic()
    while True:
        inicio = time.monotonic()
        resp = r.xreadgroup(ETAPA, CONSUMIDOR, {entrada: desde}, count=CAPACIDAD, block=1000)
        lote = resp[0][1] if resp else []
        if desde == "0" and not lote:
            desde = ">"
            continue
        if lote:
            procesar(lote)
            r.xack(entrada, ETAPA, *[mid for mid, _ in lote])   # ACK despues de escribir
            procesadas += len(lote)
        if time.monotonic() - ultimo_reporte >= 5:
            grupo = next(g for g in r.xinfo_groups(entrada) if g["name"] == ETAPA)
            atraso = grupo.get("lag") or 0
            if atraso > atraso_max:
                atraso_max = atraso
                r.hset("metricas", f"atraso_max:{ETAPA}", atraso_max)
            log(f"procesadas={procesadas} atraso={atraso}")
            ultimo_reporte = time.monotonic()
        if lote:
            time.sleep(max(0.0, 1 - (time.monotonic() - inicio)))   # limita a CAPACIDAD/s


# ------------------------------------------------------------------ 1. validacion
CONTRATOS = {
    "v1": {"planta": str, "equipo": str, "sensor": str, "seq": int, "ts": (int, float),
           "valor": (int, float)},
    "v2": {"plant": str, "asset": str, "channel": str, "n": int, "epoch": (int, float),
           "raw": (int, float)},
}
RANGOS_FISICOS = {"v1": ("valor", -50, 200), "v2": ("raw", -500, 2000)}


def revisar(m):
    """Devuelve el motivo de rechazo, o None si el mensaje cumple su contrato."""
    if not isinstance(m, dict) or "fmt" not in m:
        return "sin version de formato"
    contrato = CONTRATOS.get(m["fmt"])
    if contrato is None:
        return None        # formato nuevo: lo resuelve (o rechaza) la normalizacion
    for campo, tipo in contrato.items():
        if campo not in m:
            return f"falta el campo '{campo}'"
        if isinstance(m[campo], bool) or not isinstance(m[campo], tipo):
            return f"tipo invalido en '{campo}'"
    campo, minimo, maximo = RANGOS_FISICOS[m["fmt"]]
    if not minimo <= m[campo] <= maximo:
        return f"'{campo}' fuera de rango fisico"
    return None


def validar(lote):
    p = r.pipeline(transaction=False)
    ok = 0
    for _, campos in lote:
        crudo = campos.get("d", "")
        try:
            motivo = revisar(json.loads(crudo))
        except ValueError:
            motivo = "JSON invalido"
        if motivo:
            a_dlq(p, motivo, crudo)          # error permanente: a la DLQ, sin reintentos
        else:
            p.xadd("validadas", {"d": crudo})
            ok += 1
    p.hincrby("metricas", "validadas", ok)
    p.execute()


# ------------------------------------------------------------------ 2. normalizacion
# Incorporar un formato de sensor = registrar un normalizador y redesplegar solo esta etapa.
NORMALIZADORES = {
    "v1": lambda m: {"planta": m["planta"], "equipo": m["equipo"], "sensor": m["sensor"],
                     "seq": m["seq"], "ts": m["ts"], "valor": m["valor"]},
    "v2": lambda m: {"planta": m["plant"], "equipo": m["asset"], "sensor": m["channel"],
                     "seq": m["n"], "ts": m["epoch"], "valor": m["raw"] / 10},
}


def normalizar(lote):
    p = r.pipeline(transaction=False)
    for _, campos in lote:
        crudo = campos["d"]
        m = json.loads(crudo)
        normalizador = NORMALIZADORES.get(m["fmt"])
        if normalizador is None:
            a_dlq(p, f"formato desconocido '{m['fmt']}'", crudo)
            continue
        c = normalizador(m)
        c["formato"] = m["fmt"]
        c["clave"] = f"{c['planta']}-{c['equipo']}-{c['sensor']}-{c['seq']}"
        p.xadd("normalizadas", {"d": json.dumps(c)})
    p.execute()


# ------------------------------------------------------------------ 3. deduplicacion
# Registro de claves de idempotencia con expiracion. El script es atomico: la clave
# se registra y el mensaje se reenvia en un solo paso, asi un reinicio de la etapa
# entre ambos no puede perder ni duplicar la lectura.
DEDUP_LUA = r.register_script("""
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  redis.call('XADD', KEYS[2], '*', 'd', ARGV[2])
  return 1
end
return 0
""")


def deduplicar(lote):
    p = r.pipeline(transaction=False)
    for _, campos in lote:
        c = json.loads(campos["d"])
        DEDUP_LUA(keys=[f"idem:{c['clave']}", "unicas"], args=[TTL_IDEMPOTENCIA, campos["d"]],
                  client=p)
    nuevas = sum(p.execute())
    if len(lote) - nuevas:
        r.hincrby("metricas", "duplicados_descartados", len(lote) - nuevas)


# ------------------------------------------------------------------ 4. deteccion de anomalias
def detectar(pg):
    cur = pg.cursor()

    def procesar(lote):
        ahora = time.time()
        alarmas = []
        p = r.pipeline(transaction=False)
        for _, campos in lote:
            c = json.loads(campos["d"])
            if c["valor"] >= UMBRAL:
                alarmas.append((c["clave"], c["planta"], c["equipo"], c["sensor"], c["valor"],
                                UMBRAL, c["ts"], ahora, ahora - c["ts"] > LIMITE_ALERTA_SEG))
            p.xadd("evaluadas", {"d": campos["d"]})
        if alarmas:
            # evento AlarmaDetectada: se registra en la base propia del servicio de Alarmas
            psycopg2.extras.execute_values(
                cur, "INSERT INTO alarmas VALUES %s ON CONFLICT (clave) DO NOTHING", alarmas)
        p.execute()

    return procesar


# ------------------------------------------------------------------ 5. persistencia
def persistir(pg):
    cur = pg.cursor()

    def procesar(lote):
        filas = []
        for _, campos in lote:
            c = json.loads(campos["d"])
            filas.append((c["clave"], c["planta"], c["equipo"], c["sensor"], c["seq"], c["ts"],
                          c["valor"], c["formato"], int(c["ts"] // VENTANA_SEG)))
        insertadas = psycopg2.extras.execute_values(
            cur, "INSERT INTO lecturas (clave, planta, equipo, sensor, seq, ts, valor, formato,"
                 " ventana) VALUES %s ON CONFLICT (clave) DO NOTHING RETURNING clave",
            filas, page_size=len(filas), fetch=True)
        p = r.pipeline(transaction=False)
        p.hincrby("metricas", "persistidas", len(insertadas))
        if len(filas) - len(insertadas):
            # segunda barrera: la clave primaria tambien absorbe duplicados
            p.hincrby("metricas", "duplicados_en_persistencia", len(filas) - len(insertadas))
        p.execute()

    return procesar


# ------------------------------------------------------------------ batch de consolidacion
def consolidar(pg):
    """Cada 5 s publica las ventanas cerradas (fin + marca de agua) y recalcula las que
    recibieron datos tardios, como una version nueva y con la correccion registrada.
    Para no publicar una version por cada lote del vaciado, una ventana se recalcula
    cuando deja de recibir datos tardios durante QUIETUD_SEG."""
    cur = pg.cursor()
    esperadas = EQUIPOS * SENSORES * VENTANA_SEG       # una lectura por sensor y segundo
    while True:
        cur.execute("SELECT ventana, COUNT(*), AVG(valor),"
                    " EXTRACT(EPOCH FROM clock_timestamp() - MAX(ingesta)),"
                    " EXTRACT(EPOCH FROM clock_timestamp())"
                    " FROM lecturas GROUP BY ventana")
        actuales = cur.fetchall()
        cur.execute("SELECT ventana, version, lecturas FROM consolidado_vigente")
        vigentes = {v: (ver, n) for v, ver, n in cur.fetchall()}
        for ventana, n, promedio, quietud, ahora in sorted(actuales):
            if float(ahora) < (ventana + 1) * VENTANA_SEG + WATERMARK_SEG:
                continue                                 # ventana todavia abierta
            previo = vigentes.get(ventana)
            if previo and previo[1] == n:
                continue                                 # sin cambios desde la ultima version
            if previo and float(quietud) < QUIETUD_SEG:
                continue                                 # todavia llegan datos tardios
            version = previo[0] + 1 if previo else 1
            completitud = n / esperadas
            cur.execute("INSERT INTO consolidados (ventana, version, lecturas, promedio,"
                        " completitud) VALUES (%s, %s, %s, %s, %s)",
                        (ventana, version, n, promedio, completitud))
            hora = time.strftime("%H:%M", time.gmtime(ventana * VENTANA_SEG))
            if previo:
                cur.execute("INSERT INTO correcciones (ventana, version_anterior, version_nueva,"
                            " causa, registros_agregados) VALUES (%s, %s, %s, %s, %s)",
                            (ventana, previo[0], version, "llegada tardia", n - previo[1]))
                log(f"ventana {hora} recalculada: v{previo[0]} -> v{version} "
                    f"(+{n - previo[1]} lecturas tardias)")
            else:
                estado = "completa" if completitud >= 0.99 else "PARCIAL"
                log(f"ventana {hora} publicada v1 ({n} lecturas, {completitud:.1%}, {estado})")
        time.sleep(5)


if __name__ == "__main__":
    log(f"inicio (capacidad {CAPACIDAD} lecturas/s)")
    if ETAPA == "validacion":
        ejecutar_filtro("telemetria", validar)
    elif ETAPA == "normalizacion":
        ejecutar_filtro("validadas", normalizar)
    elif ETAPA == "deduplicacion":
        ejecutar_filtro("normalizadas", deduplicar)
    elif ETAPA == "deteccion":
        ejecutar_filtro("unicas", detectar(conectar_pg()))
    elif ETAPA == "persistencia":
        ejecutar_filtro("evaluadas", persistir(conectar_pg()))
    elif ETAPA == "consolidacion":
        consolidar(conectar_pg())
    else:
        raise SystemExit(f"ETAPA desconocida: {ETAPA}")
