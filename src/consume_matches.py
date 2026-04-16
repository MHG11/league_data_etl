import logging
import json
from kafka import KafkaConsumer
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s ')

def inciar_consumidor(): 
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
        
        match_id = payload['match_id']
        vencedor = payload['winning_team']
        alma = payload['dragon_soul']
          
        logging.info(f"Recebido: {match_id} | Vencedor: {vencedor} | Alma: {alma}")

if __name__ == "__main__":
    inciar_consumidor()
        