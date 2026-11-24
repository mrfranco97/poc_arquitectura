#!/usr/bin/env bash
# Ejecuta una corrida completa de la PoC: operacion normal, corte de conectividad
# del gateway, reconexion, estabilizacion e informe.
#
#   scripts/corrida.sh asis|tobe
#
# Tiempos ajustables (segundos): T_NORMAL (20), T_CORTE (60), T_POST (35).
# SIN_BUILD=1 reutiliza las imagenes ya construidas en lugar de reconstruirlas.
# El informe queda en resultados/informe-<modo>.txt
set -euo pipefail

# Git Bash (Windows) convierte argumentos como /data/detener en rutas de Windows;
# son rutas dentro de los contenedores, asi que se desactiva esa conversion.
export MSYS_NO_PATHCONV=1

MODO="${1:-}"
if [[ "$MODO" != "asis" && "$MODO" != "tobe" ]]; then
  echo "uso: $0 asis|tobe" >&2
  exit 1
fi
T_NORMAL="${T_NORMAL:-20}"
T_CORTE="${T_CORTE:-60}"
T_POST="${T_POST:-35}"

cd "$(dirname "$0")/.."
export MODO
mkdir -p resultados

paso() { echo -e "\n[$(date +%H:%M:%S)] $*"; }

paso "Limpiando corridas anteriores"
docker compose down -v --remove-orphans

paso "Levantando el entorno en modo $MODO"
if [[ "${SIN_BUILD:-0}" == "1" ]]; then
  docker compose up -d            # reutiliza las imagenes ya construidas
else
  docker compose up -d --build
fi

paso "Operacion normal durante ${T_NORMAL} s"
sleep "$T_NORMAL"

paso "CORTE: se desconecta el gateway de la red poc_nube durante ${T_CORTE} s"
docker network disconnect poc_nube gateway
sleep "$T_CORTE"

paso "RECONEXION del gateway"
docker network connect poc_nube gateway

paso "Operacion posterior al corte durante ${T_POST} s"
sleep "$T_POST"

paso "Se detiene la generacion; se espera a que el sistema se estabilice"
docker exec gateway touch /data/detener
docker compose run --rm --no-deps informe --esperar | tee "resultados/informe-$MODO.txt"

paso "Listo. El entorno sigue levantado para consultas; para bajarlo: docker compose down -v"
