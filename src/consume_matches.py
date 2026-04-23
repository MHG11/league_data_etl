import logging
import json
import psycopg2
from kafka import KafkaConsumer
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s ')

DB_CONFIG = {
    "host":"localhost",
    "port":5433,
    "database":"lol_data",
    "user":"postgres",
    "password":"3695"
}

def inciar_consumidor(): 
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    consumer = KafkaConsumer(
        'lol_winrates_enriched',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='grupo_analise_lol',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    
    logging.info('Conexão estabelecida! Esperando os dados chegarem... \n')
    
    for mensagem in consumer:
        payload = mensagem.value
        
        try:
            query = """ 
                INSERT INTO partidas_enriquecidas (match_id, duracao_segundos, time_vencedor, alma_conquistada) 
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (match_id) DO NOTHING;
        """
            cursor.execute(query, (
                payload['match_id'],
                payload['game_duration_seconds'],
                payload['winning_team'],
                payload['dragon_soul']
            ))
            conn.commit()
            logging.info(f' Partida {payload['match_id']} salva no banco.')

        except Exception as e:
            logging.error(f' Erro ao salvar no banco: {e}')
            conn.rollback()

if __name__ == "__main__":
    inciar_consumidor()
        