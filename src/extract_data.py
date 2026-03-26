import requests
import json
from pathlib import Path
import os,sys
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

RIOT_API_KEY = os.getenv('API_KEY')
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
        print(f'Erro ao conectar-se a API da riot {e}')
        return
    
    
def all_matches_id() -> list:
    pass