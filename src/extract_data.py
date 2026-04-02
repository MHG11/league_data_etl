import requests
import json
from pathlib import Path
import os,sys
from dotenv import load_dotenv
import redis
import time
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s ')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

RIOT_API_KEY = os.getenv('API_KEY')
ROUTING = 'americas'
REGION = 'br1'
url = f'https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 OPR/128.0.0.0",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,ko;q=0.6,pt-PT;q=0.5,fr;q=0.4,it;q=0.3,ru;q=0.2,es;q=0.1",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Riot-Token": RIOT_API_KEY
}

def all_challengers_players(url:str) -> list:
    try:
        response = requests.get(url, headers=HEADERS)
        data = response.json()
        puuids = [entry["puuid"] for entry in data.get("entries", [])]
        
        output_path = 'data/players.json'
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents= True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(puuids, f,ensure_ascii=False, indent=2)
            
        return puuids
    except requests.exceptions.RequestException as e:
        logging.error(f'Erro ao conectar-se a API da riot {e}')
        return
    
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

def get_timeline_match() -> list:
    path_read = 'data/matchs_ids.json'
    with open(path_read, 'r') as f:
        matchId = json.load(f)
    
    matchs_timeline = []
        
    for matchids in matchId:
        try:
            url = f'https://{ROUTING}.api.riotgames.com/lol/match/v5/matches/{matchids}/timeline'
            response = requests.get(url, timeout=10, headers=HEADERS)
        
            if response.status_code == 200:
                data = response.json()
                matchs_timeline.append(data)
                logging.info(f'Time line da match {matchids} coletada com sucesso.')
            else:
                logging.warning(f'Erro {response.status_code} ao buscar {matchId}')
        except Exception as e:
            logging.error('Erro na requisição: ', e)
        
        time.sleep(1.2)
    return matchs_timeline
    
    
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
    
def orchestrate_producer():
    pass

#all_challengers_players(url)
all_matches_id()
get_match_ids()
processar_partidas_unicas()
