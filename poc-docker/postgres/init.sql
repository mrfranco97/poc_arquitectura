-- Repository analitico: unico escritor, la etapa de persistencia.
-- La clave de idempotencia (planta-equipo-sensor-secuencia) es la clave primaria.
CREATE TABLE lecturas (
    clave    TEXT PRIMARY KEY,
    planta   TEXT NOT NULL,
    equipo   TEXT NOT NULL,
    sensor   TEXT NOT NULL,
    seq      BIGINT NOT NULL,
    ts       DOUBLE PRECISION NOT NULL,          -- event time (instante de captura)
    valor    DOUBLE PRECISION NOT NULL,          -- modelo canonico
    formato  TEXT NOT NULL,
    ventana  BIGINT NOT NULL,                    -- particion por ventana de event time
    ingesta  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX lecturas_ventana ON lecturas (ventana);

-- Base propia del servicio de Alarmas.
CREATE TABLE alarmas (
    clave          TEXT PRIMARY KEY,
    planta         TEXT NOT NULL,
    equipo         TEXT NOT NULL,
    sensor         TEXT NOT NULL,
    valor          DOUBLE PRECISION NOT NULL,
    umbral         DOUBLE PRECISION NOT NULL,
    ts_evento      DOUBLE PRECISION NOT NULL,
    ts_deteccion   DOUBLE PRECISION NOT NULL,
    retrospectiva  BOOLEAN NOT NULL               -- detectada mas de 30 s despues del evento
);

-- Consolidados versionados: cada recalculo agrega una version, la anterior se conserva.
CREATE TABLE consolidados (
    ventana      BIGINT NOT NULL,
    version      INT NOT NULL,
    lecturas     INT NOT NULL,
    promedio     DOUBLE PRECISION,
    completitud  DOUBLE PRECISION NOT NULL,       -- lecturas / lecturas esperadas
    publicado    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (ventana, version)
);

CREATE VIEW consolidado_vigente AS
    SELECT DISTINCT ON (ventana) * FROM consolidados ORDER BY ventana, version DESC;

-- Trazabilidad de correcciones por datos tardios.
CREATE TABLE correcciones (
    id                   SERIAL PRIMARY KEY,
    ventana              BIGINT NOT NULL,
    version_anterior     INT NOT NULL,
    version_nueva        INT NOT NULL,
    causa                TEXT NOT NULL,
    registros_agregados  INT NOT NULL,
    registrado           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
