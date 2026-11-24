# PoC MetalSur 4.0 — Simulación con Docker

Entorno reducido del TO-BE con contenedores reales. Se ejecuta dos veces con la misma
carga y el mismo corte de red: una con el gateway en modo **AS-IS** (sin almacenamiento
local) y otra en modo **TO-BE** (store-and-forward). Es el mismo código; la única
diferencia entre corridas es la decisión de arquitectura (`MODO=asis|tobe`).

Cubre los escenarios **E1** (continuidad), **E2** (absorción del atraso) y **E4**
(duplicados, desorden y llegadas tardías con reconsolidación versionada), y muestra la
etapa de normalización que soporta **E6** (dos formatos de sensor conviviendo).

## Componentes

| Contenedor | Rol en el TO-BE | Implementación |
|---|---|---|
| `gateway` | Gateway de borde de la planta | Python. Genera 400 lecturas/s (50 equipos × 8 sensores) en formatos v1 y v2, con 0,1 % de mensajes corruptos. En TO-BE guarda primero en SQLite (volumen), envía el dato en vivo con prioridad y reenvía el acumulado a 800 lecturas/s como máximo, en desorden; el 1 % de confirmaciones se pierde y genera duplicados. Emite alertas por umbral locales. |
| `redis` | Broker de mensajería | Redis Streams; un stream por tópico y un grupo de consumidores por filtro |
| `validacion` | Filtro 1 | Contrato del mensaje y rangos físicos; errores permanentes a la DLQ |
| `normalizacion` | Filtro 2 | Un normalizador por formato al modelo canónico; formato desconocido a la DLQ |
| `deduplicacion` | Filtro 3 | Registro de claves de idempotencia con expiración de 7 días (equivale a DynamoDB con TTL) |
| `deteccion` | Filtro 4 / servicio de Alarmas | Umbral sobre el valor; alarmas con más de 30 s de demora se marcan retrospectivas |
| `persistencia` | Filtro 5 | Escribe en el Repository particionado por ventana de event time |
| `consolidacion` | Batch de consolidados | Ventanas de 60 s con marca de agua de 20 s; recalcula como versión nueva al llegar datos tardíos y registra la corrección |
| `postgres` | Repository, base de Alarmas, consolidados | PostgreSQL; la clave de idempotencia es la clave primaria |
| `informe` | Herramienta de verificación | Compara lo generado por el gateway con lo persistido y evalúa E1, E2 y E4 |

Flujo:

```
gateway ─► telemetria ─► validacion ─► validadas ─► normalizacion ─► normalizadas
        ─► deduplicacion ─► unicas ─► deteccion ─► evaluadas ─► persistencia ─► PostgreSQL
                                                     └─► alarmas        consolidacion ◄─┘
validacion / normalizacion ─► dlq
```

Cada filtro confirma el mensaje (XACK) recién después de escribir su salida: la entrega
es al menos una vez y la idempotencia evita los efectos duplicados. Cada filtro procesa
como máximo `CAPACIDAD` lecturas/s (1500), por encima de la carga total con vaciado
(400 + 800 = 1200), de modo que el atraso se absorbe sin pérdida.

El corte se simula desconectando el gateway de la red `poc_nube`, que es la que comparte
con el broker.

## Estructura

```
poc-docker/
├── docker-compose.yml
├── gateway/            Dockerfile, gateway.py
├── servicios/          Dockerfile, etapas.py (filtros y consolidación), informe.py
├── postgres/init.sql   esquema del Repository, alarmas, consolidados y correcciones
└── scripts/corrida.sh  corrida completa automatizada
```

## Ejecución automatizada

Requisitos: Docker con Docker Compose v2. Cada corrida dura unos seis minutos.

```bash
cd poc-docker
scripts/corrida.sh asis
scripts/corrida.sh tobe
```

El script limpia el entorno, lo levanta, espera 60 s de operación normal, corta la red
del gateway 120 s, la restablece, espera 60 s, detiene la generación, espera a que el
sistema se estabilice y deja el informe en `resultados/informe-<modo>.txt`. Los tiempos
se ajustan con `T_NORMAL`, `T_CORTE` y `T_POST`, por ejemplo
`T_CORTE=300 scripts/corrida.sh tobe`.

## Ejecución manual (para la defensa)

En Git Bash (Windows), ejecutar antes `export MSYS_NO_PATHCONV=1` para que rutas como
`/data/detener` no se conviertan en rutas de Windows.

```bash
docker compose build
MODO=tobe docker compose up -d
docker compose logs -f gateway consolidacion          # en otra terminal
docker network disconnect poc_nube gateway            # tras un minuto: corte
docker network connect poc_nube gateway               # dos minutos después: reconexión
docker exec gateway touch /data/detener               # detiene la generación
docker compose run --rm --no-deps informe --esperar   # espera a estabilizar e informa
docker compose down -v                                # antes de la siguiente corrida
```

Qué observar:

- **E1, continuidad.** En AS-IS, `perdidas` del gateway crece 400 por segundo durante el
  corte. En TO-BE se mantiene en cero y `en_buffer` crece 400 por segundo.
- **E2, absorción del atraso.** Al reconectar, el gateway vacía el acumulado a 800
  lecturas/s adicionales; el atraso de los filtros sube y vuelve a cero.
- **E4, integridad.** `duplicados descartados` es mayor que cero por los reenvíos, y aun
  así lo persistido coincide exactamente con las lecturas válidas generadas. Las ventanas
  del corte se publican primero como parciales y luego se recalculan como versión 2, con
  la corrección registrada en la tabla `correcciones`.

## Cómo leer el informe

- `generadas` es la verdad del gateway: cuántas lecturas válidas generó por ventana. La
  columna `ok` indica si el consolidado vigente coincide con ella en cantidad y promedio.
- La completitud de la primera y la última ventana es baja porque la corrida empieza y
  termina a mitad de minuto. No es pérdida: esas ventanas coinciden con lo generado.
- La DLQ puede tener más entradas que mensajes corruptos: un corrupto cuya confirmación
  se perdió se reenvía y llega dos veces. El informe cuenta los mensajes distintos.

## Consultas útiles

Lecturas por minuto (en AS-IS el hueco coincide con el corte):

```bash
docker exec postgres psql -U postgres -c "SELECT to_timestamp(ventana*60) AS minuto, COUNT(*) FROM lecturas GROUP BY ventana ORDER BY ventana"
```

Historia de versiones de los consolidados y correcciones registradas:

```bash
docker exec postgres psql -U postgres -c "SELECT to_timestamp(ventana*60) AS ventana, version, lecturas, round(completitud::numeric, 3) AS completitud, publicado FROM consolidados ORDER BY ventana, version"
docker exec postgres psql -U postgres -c "SELECT * FROM correcciones"
```

Mensajes en la DLQ con su motivo:

```bash
docker exec redis redis-cli XRANGE dlq - + COUNT 5
```

Alarmas retrospectivas (detectadas más de 30 s después del evento):

```bash
docker exec postgres psql -U postgres -c "SELECT equipo, sensor, round(valor::numeric,1) AS valor, round((ts_deteccion - ts_evento)::numeric) AS demora_s FROM alarmas WHERE retrospectiva"
```

## Alcance y limitaciones

- Escala reducida: 400 lecturas/s frente a las 2.880 nominales. `EQUIPOS`,
  `TASA_VACIADO` y `CAPACIDAD` permiten ajustarla; la relación entre las tasas reproduce
  el diseño (el vaciado limitado mantiene la carga por debajo de la capacidad).
- El consolidado se calcula por ventana de 60 s en lugar de por día para que el
  mecanismo de marca de agua, versionado y corrección sea observable en minutos.
- Redis Streams reemplaza a Kinesis y PostgreSQL al Repository en S3/Athena y a
  DynamoDB; los tiempos de latencia no son representativos del entorno productivo en AWS.
