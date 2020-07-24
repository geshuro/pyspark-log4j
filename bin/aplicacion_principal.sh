#!/bin/sh
. /conf/set_spark_env.sh

NOMBRE_ARCHIVO="aplicacion_principal"
L_LOGDATE=$(date +"%Y%m%d:%H:%M:%S")
L_LOGDATE_ARCH=$(date +"%Y%m%d_%H_%M_%S")
L_LOGDATE_M_ARCH=$(date +"%Y%m%d")
lc_FileLog="aplicacion_principal_"$L_LOGDATE_ARCH
RUTA_PROPERTIES=$RUTA_CONF/"log4j_aplicacion_principal.properties"
mkdir -p -- "$RUTA_LOG/$L_LOGDATE_M_ARCH"

echo " Inicio :$L_LOGDATE " > $RUTA_LOG/$L_LOGDATE_M_ARCH/$lc_FileLog.log
spark-submit \
--master yarn  \
--deploy-mode client \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file://"$RUTA_PROPERTIES \
--driver-memory 5G \
--executor-memory 3G \
--num-executors 5 \
--executor-cores 2 \
$RUTA_BIN/$NOMBRE_ARCHIVO.py >>  $RUTA_LOG/$L_LOGDATE_M_ARCH/$lc_FileLog.log 2>> $RUTA_LOG/$L_LOGDATE_M_ARCH/$lc_FileLog.log
L_LOGDATE=$(date +"%Y%m%d:%H:%M:%S")

echo " Fin :$L_LOGDATE " >>  $RUTA_LOG/$L_LOGDATE_M_ARCH/$lc_FileLog.log
