from pyspark.sql import SparkSession
import logging
import pyspark
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s ')

def inciar_consumidor():
    logging.info('Iniciando o operário PySpark... Aguarde!')
    
    versao_spark = pyspark.__version__
    pacote_kafka = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{versao_spark}"
    
    # ligando o motor e baixando o conector do kafka
    # o .config vai ate a internet e baixa o .jar do kafka paro spark
    spark = SparkSession.builder \
        .appName('RiotGames_Streaming_Consumer') \
        .config('spark.jars.packages', pacote_kafka) \
        .getOrCreate()
        
    # Deixa os logs do spark menos barulhentos exibindo só erros graves.
    spark.sparkContext.setLogLevel('WARN')
    
    logging.info('Conectando na esteira do Kafka na porta 9092...')
    
    # Lendo o fluxo de dados do Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'lol_partidas') \
        .option('startingOffsets', 'earliest') \
        .load()
     
    # Para traduzir de bytes para string, especialmente a coluna value.
    # Pois o kafka guarda tudo em formato binario (bytes).   
    df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_bruto")
    
    logging.info('Conexão estabelecida! Esperando os dados chegarem... \n')
    
    # Imprimindo os resultados no terminal ao vivo
    query = df_json.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
        
    # Roda o script infinitamente até o CTRL + C ser pressionado.
    query.awaitTermination()

    
if __name__ == "__main__":
    inciar_consumidor()
        