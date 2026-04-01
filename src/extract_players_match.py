import requests
from kafka import KafkaProducer
import json
import os,sys
import time
from pathlib import Path
from dotenv import load_dotenv
import redis
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s ')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

RIOT_API_KEY = os.getenv('API_KEY')

def all_matches_id() -> list:
    path_read = 'data/players.json'
    with open(path_read,'r') as f:
        puuid = json.load(f)
        return puuid
        
def get_match_ids() -> list:
    jogadores = all_matches_id()
    todas_partidas = []
    for puuid in jogadores:
        try:
            url = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count=20&queue=420&api_key={RIOT_API_KEY}'
            response = requests.get(url, timeout=10)
        
            if response.status_code == 200:
                data = response.json()
                todas_partidas.extend(data)
                time.sleep(1.2)
                logging.info(f"Sucesso: {len(data)} partidas coletadas para o jogador {puuid} !")
            else:
                logging.error(f"Erro no PUUID {puuid}. Código: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"A conexão caiu ou a Riot travou no PUUID {puuid}! Pulando...")
            time.sleep(5) # Dá um tempinho extra para a rede estabilizar antes do próximo jogador
            continue

    with open('data/matchs_ids.json', 'w') as f:
            json.dump(todas_partidas, f, indent=4)
        
    logging.info(f"Extração concluída! Total de partidas únicas coletadas: {len(todas_partidas)}")
    return todas_partidas
    
def processar_partidas_unicas():
    #Conectando ao Redis rodando localmente via docker
    banco_redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    with open('data/matchs_ids.json', 'r') as f:
        lista_de_ids = json.load(f)
        
    logging.info(f'Iniciando a verificação de {len(lista_de_ids)} partidas...')
    
    for match_id in lista_de_ids:
        #Verifica de o id da partida já existe no json, caso sim, ignora.
        if banco_redis.exists(match_id):
            logging.info(f'Partida {match_id} já processada anteriormente. Pulando! ⏭️')
            continue
        else:
            url_partida = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={RIOT_API_KEY}'
            try:
                response = requests.get(url_partida, timeout=10)
                if response.status_code == 200:
                    partida_completa = response.json()
                    
                    #Arremessa o JSON das partidas completas para a esteira do kafka
                    producer.send('lol_partidas', partida_completa)
                    
                    banco_redis.set(match_id,"processado")
                    logging.info(f'Partida {match_id} enviada com sucesso para o kafka.')
            except Exception as e:
                logging.error(f'Erro {e} na partida {match_id}')
            time.sleep(1.2)
    
    
#all_matches_id()
#get_match_ids()
processar_partidas_unicas()