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
    

def extrair_alma_do_dragao(timeline_data: dict) -> str:

    dragões_time_azul = 0 
    dragões_time_vermelho = 0 
    
    # Validação de segurança caso o JSON venha malformado ou vazio
    if 'info' not in timeline_data or 'frames' not in timeline_data['info']:
        return None

    #Itera pelos minutos do jogo (frames)
    for frame in timeline_data['info']['frames']:
        
        # Itera pelos eventos de cada minuto
        # O .get('events', []) evita erro caso um frame venha sem a chave 'events'
        for event in frame.get('events', []):
        
            # Filtra apenas a morte de DRAGÕES
            if event.get('type') == 'ELITE_MONSTER_KILL' and event.get('monsterType') == 'DRAGON':
                killer_id = event.get('killerId')
                dragon_subtype = event.get('monsterSubType') # O elemento do dragão

                # Verifica se o evento tem os dados necessários
                if killer_id is None:
                    continue

                # Lógica de contagem por time usando o ID do jogador
                if 1 <= killer_id <= 5:
                    dragões_time_azul += 1
                    # Se chegou no 4º dragão, conquistou a Alma!
                    if dragões_time_azul == 4:
                        return str(dragon_subtype)
                        
                elif 6 <= killer_id <= 10:
                    dragões_time_vermelho += 1
                    # Se chegou no 4º dragão, conquistou a Alma!
                    if dragões_time_vermelho == 4:
                        return str(dragon_subtype)
    return "Sem alma"

def producer():
    data_timeline = []
    output_path = 'data/timeline.json'
    banco_redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    with open('data/matchs_ids.json', 'r') as f:
        lista_de_ids = json.load(f)
        
    logging.info(f'Iniciando o Producer ETL para {len(lista_de_ids)} partidas...')
    
    for match_id in lista_de_ids:
        if banco_redis.exists(match_id):
            logging.info(f'Partida {match_id} já processada anteriormente. Pulando ⏭️')   
            continue
        
        try:
            url_partida = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}'
            url_timeline = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline'
            
            resp_partida = requests.get(url_partida, headers=HEADERS, timeout=10)
            resp_timeline = requests.get(url_timeline, headers=HEADERS, timeout=10)
            
            if resp_partida.status_code == 200 and resp_timeline.status_code == 200:
                match_data = resp_partida.json()
                timeline_data = resp_timeline.json()

                alma_conquistada = extrair_alma_do_dragao(timeline_data)

                time_vencedor = None
                for team in match_data.get('info', {}).get('teams', []):
                    if team.get('win'):
                        time_vencedor = team.get('teamId')

                payload_enriquecido = {
                    "match_id": match_id,
                    "game_duration_seconds": match_data['info']['gameDuration'],
                    "winning_team": time_vencedor,
                    "dragon_soul": alma_conquistada
                }
                
                kafka_producer.send('lol_winrates_enriched', payload_enriquecido)
                
                banco_redis.set(match_id, "processando")
                logging.info(f'Sucesso: Partida {match_id} enriquecida e enviada pro Kafka')
            else: 
                logging.warning(f'Erro nas requisições da partida {match_id}. Match: {resp_partida.status_code} | Timeline: {resp_timeline.status_code}')
        except Exception as e:
            logging.error(f'Erro inesperado no pipeline da partida {match_id}: {e}')
        
        time.sleep(1.2) 

all_challengers_players(url)
get_match_ids()
producer()
