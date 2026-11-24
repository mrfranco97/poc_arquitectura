"""
Informe de verificacion de la PoC: compara lo que genero el gateway (su "verdad",
leida del volumen del gateway) con lo que quedo en el Repository, y evalua los
escenarios E1, E2 y E4.

  python informe.py            informe del estado actual
  python informe.py --esperar  espera a que el sistema se estabilice (generacion
                               detenida, buffer vacio, sin atraso y consolidados
                               al dia) y luego informa
"""
import argparse
import json
import os
import sys
import time

import psycopg2
import redis

ESTADO = "/gateway/estado.json"
GRUPOS = [("telemetria", "validacion"), ("validadas", "normalizacion"),
          ("normalizadas", "deduplicacion"), ("unicas", "deteccion"),
          ("evaluadas", "persistencia")]

r = redis.Redis(host="redis", decode_responses=True)


def leer_estado():
    with open(ESTADO) as f:
        return json.load(f)


def conectar_pg():
    pg = psycopg2.connect(host="postgres", user="postgres", password="poc")
    pg.autocommit = True
    return pg.cursor()


def atrasos():
    """(etapa, atraso, pendientes de confirmar) por cada filtro."""
    res = []
    for stream, grupo in GRUPOS:
        try:
            g = next((g for g in r.xinfo_groups(stream) if g["name"] == grupo), None)
        except redis.ResponseError:
            g = None
        res.append((grupo, (g or {}).get("lag") or 0, (g or {}).get("pending") or 0))
    return res


def consolidados_pendientes(cur):
    cur.execute("""SELECT COUNT(*) FROM (SELECT ventana, COUNT(*) n FROM lecturas GROUP BY 1) l
                   LEFT JOIN consolidado_vigente c USING (ventana)
                   WHERE c.lecturas IS DISTINCT FROM l.n""")
    return cur.fetchone()[0]


def esperar(cur, limite):
    inicio = time.time()
    while time.time() - inicio < limite:
        est = leer_estado()
        a = atrasos()
        motivos = []
        if not est["detenido"]:
            motivos.append("el gateway sigue generando")
        if est["en_buffer"]:
            motivos.append(f"en_buffer={est['en_buffer']}")
        if any(lag or pend for _, lag, pend in a):
            motivos.append("atraso=" + ",".join(f"{g}:{lag}" for g, lag, _ in a if lag))
        pend = consolidados_pendientes(cur)
        if pend:
            motivos.append(f"ventanas sin consolidar={pend}")
        if not motivos:
            return True
        print(f"esperando estabilizacion: {'; '.join(motivos)}", file=sys.stderr, flush=True)
        time.sleep(3)
    print("se agoto la espera; el informe refleja un estado intermedio", file=sys.stderr)
    return False


def hora(ventana, seg):
    return time.strftime("%H:%M:%S", time.gmtime(ventana * seg))


def fila(etiqueta, valor, nota=""):
    print(f"  {etiqueta:<38}{valor:>12}  {nota}")


def cumple(ok):
    return "CUMPLE" if ok else "NO CUMPLE"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--esperar", action="store_true")
    ap.add_argument("--limite", type=int, default=900, help="segundos maximos de espera")
    args = ap.parse_args()

    cur = conectar_pg()
    if args.esperar:
        esperar(cur, args.limite)

    est = leer_estado()
    m = r.hgetall("metricas")
    met = lambda k: int(m.get(k, 0))
    seg = int(os.environ.get("VENTANA_SEG", "30"))
    cur.execute("SELECT COUNT(*) FROM lecturas")
    persistidas = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE retrospectiva) FROM alarmas")
    alarmas, retrospectivas = cur.fetchone()
    dlq = r.xlen("dlq")
    # un corrupto cuya confirmacion se perdio llega dos veces: se cuentan mensajes distintos
    dlq_distintos = len({c["d"] for _, c in r.xrange("dlq")})
    perdida = est["validas"] - persistidas
    modo = est["modo"].upper()

    print(f"\n=== Informe PoC MetalSur 4.0 - modo {modo} ===\n")
    print("Gateway de borde")
    fila("lecturas generadas", est["generadas"])
    fila("  corruptas (deben ir a la DLQ)", est["corruptas"])
    fila("  validas", est["validas"])
    fila("perdidas en el gateway", est["perdidas"], "(sin buffer local)" if modo == "ASIS" else "")
    fila("reenviadas desde el buffer", est["reenviadas"])
    fila("maximo acumulado en buffer", est["buffer_max"])
    fila("pendientes en buffer", est["en_buffer"])
    fila("alertas por umbral en el borde", est["alertas_locales"])
    if est["corte_inicio"] and est["corte_fin"]:
        fila("duracion del corte observado (s)", round(est["corte_fin"] - est["corte_inicio"]))
    if est["corte_fin"] and est["vaciado_fin"]:
        fila("vaciado del acumulado (s)", round(est["vaciado_fin"] - est["corte_fin"]))

    print("\nPipeline (Pipes & Filters)")
    fila("validadas", met("validadas"))
    fila("enviadas a la DLQ", dlq, f"({dlq_distintos} mensajes distintos)")
    fila("duplicados descartados (deduplicacion)", met("duplicados_descartados"))
    fila("duplicados absorbidos por la PK", met("duplicados_en_persistencia"))
    fila("persistidas en el Repository", persistidas)
    for etapa, lag, pend in atrasos():
        fila(f"atraso {etapa} (actual / maximo)", f"{lag} / {met('atraso_max:' + etapa)}",
             f"pendientes de ACK: {pend}" if pend else "")
    fila("alarmas detectadas", alarmas)
    fila("  retrospectivas (> 30 s del evento)", retrospectivas)

    print("\nIntegridad")
    fila("perdida silenciosa (validas - persist.)", perdida)

    print(f"\nConsolidados por ventana de {seg} s (hora UTC del event time)")
    cur.execute("SELECT ventana, version, lecturas, promedio, completitud FROM consolidado_vigente"
                " ORDER BY ventana")
    vigentes = {v[0]: v[1:] for v in cur.fetchall()}
    verdad = {int(k): v for k, v in est["verdad"].items()}
    errores = 0
    print(f"  {'ventana':<10}{'version':>8}{'lecturas':>10}{'generadas':>11}{'complet.':>10}  ok")
    # se recorren las ventanas generadas: una ventana sin ningun dato tambien es un error
    for ventana in sorted(set(verdad) | set(vigentes)):
        gen_n, gen_suma = verdad.get(ventana, (0, 0.0))
        version, n, prom, comp = vigentes.get(ventana, ("-", 0, None, 0.0))
        ok = n == gen_n and gen_n > 0 and abs(prom - gen_suma / gen_n) < 1e-6
        errores += not ok
        print(f"  {hora(ventana, seg):<10}{version:>8}{n:>10}{gen_n:>11}{comp:>10.1%}  "
              f"{'si' if ok else 'NO'}")

    cur.execute("SELECT ventana, version_anterior, version_nueva, causa, registros_agregados,"
                " registrado FROM correcciones ORDER BY id")
    correcciones = cur.fetchall()
    print(f"\nCorrecciones registradas: {len(correcciones)}")
    for ventana, ant, nue, causa, agregados, registrado in correcciones:
        print(f"  ventana {hora(ventana, seg)}: v{ant} -> v{nue}, {causa}, +{agregados} lecturas,"
              f" registrada {registrado:%H:%M:%S}")

    drenado = est["en_buffer"] == 0 and all(lag == 0 and p == 0 for _, lag, p in atrasos())
    print("\nEscenarios")
    print(f"  E1 continuidad: perdida silenciosa = {perdida} -> {cumple(perdida == 0)}")
    print(f"  E2 absorcion del atraso: sin perdida, buffer vacio y sin atraso en el pipeline -> "
          f"{cumple(drenado and perdida == 0)}")
    e4 = (persistidas == est["validas"] and dlq_distintos == est["corruptas"] and errores == 0
          and met("duplicados_descartados") > 0)
    print(f"  E4 integridad: duplicados descartados={met('duplicados_descartados')}, "
          f"corruptos en DLQ={dlq_distintos}/{est['corruptas']}, ventanas con error={errores}, "
          f"correcciones={len(correcciones)} -> {cumple(e4)}")
    print()


if __name__ == "__main__":
    main()
