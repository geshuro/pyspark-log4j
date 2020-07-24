##################################################################################
#   Organizacion     :
#   Programa         : aplicacion_principal.py
#   Creado por       : Isaac Mendoza
#   Fecha Creacion   : xx/xx/2019
#   Proposito        : 
#   Fuentes de datos : 
#   Destino          :
#   Modificaciones   :
#      Correlativo       :
#      Modificado por    :
#      Fecha Modificacion:
#      Descripcion       :
###################################################################################

# SECCION DE IMPORT
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


def start_spark_env():
	SparkContext._ensure_initialized()
	try:
		conf = SparkConf()
		sc = SparkContext(conf=conf) 
		spark = SparkSession.builder.getOrCreate()
		log4jLogger = sc._jvm.org.apache.log4j 
		log = log4jLogger.LogManager.getLogger(__name__) 
		log.info("INICIO: Creacion Sesion Spark")
	except py4j.protocol.Py4JError:       
		spark = SparkSession.builder.getOrCreate()
	except TypeError:
		spark = SparkSession.builder.getOrCreate()
	#Personalizar configuraciones de la sesion spark
	#ejm:
	#spark.conf.set('spark.sql.crossJoin.enabled', 'true')
	log.info("FIN: Creacion Sesion Spark")
	return spark, log

def proceso():
	log.info("INICIO: proceso")
    try:
    #Logica de proceso
    log.debug("Proceso se ejecuto en X minutos")
    log.warn("Proceso supero el umbral de ejecucion X minutos")
    except AnalysisException as ae:
        log.error("Ocurrio el siguiente error: {}".format(ae.message))
        pass
	log.info("FIN: proceso")
	pass

def main():
	global spark
	global log
	spark, log = start_spark_env()
	proceso()
	pass

if __name__ == '__main__':
	main()