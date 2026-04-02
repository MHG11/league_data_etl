from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import col, from_json, explode
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
        .config('spark.driver.memory', '4g') \
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
        .option('maxOffsetsPerTrigger', 50) \
        .load()
     
    # Para traduzir de bytes para string, especialmente a coluna value.
    # Pois o kafka guarda tudo em formato binario (bytes).   
    df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_bruto")
    
    logging.info('Conexão estabelecida! Esperando os dados chegarem... \n')
    
    esquema_partida = StructType([
    StructField("metadata", StructType([
        StructField("matchId", StringType(), True)]), True),
    
    
    StructField("info", StructType([
        StructField("gameDuration", IntegerType(), True), 
        StructField("participants", ArrayType(
            StructType([
                StructField("championName", StringType(), True),
                StructField("win", BooleanType(), True)
                ]), True))
        ]), True),
     
    ])

    df_estruturado = df_json.withColumn("dados_partidas", from_json(col("json_bruto"), esquema_partida))
    
    df_exploded = df_estruturado.select(
        col("dados_partidas.metadata.matchId"),
        col("dados_partidas.info.gameDuration"),
        explode(col("dados_partidas.info.participants")).alias("jogador")
    )
    
    df_final = df_exploded.select(
        "matchId",
        "gameDuration",
        "jogador.championName",
        "jogador.win"
    )
    
   
    
    # Imprimindo os resultados no terminal ao vivo
    query = df_final.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
        
    # Roda o script infinitamente até o CTRL + C ser pressionado.
    query.awaitTermination()


if __name__ == "__main__":
    inciar_consumidor()
        