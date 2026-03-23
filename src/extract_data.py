import requests
import json
from pathlib import Path


RIOT_API_KEY = "RGAPI-1d3098f9-917c-4251-82f1-d2a3fe116b94"
REGION = 'br1'
url = f'https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5'
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Riot-Token": RIOT_API_KEY
}

def all_challengers_players(url:str) -> list:
    
    try:
        response = requests.get(url, headers=HEADERS)
        data = response.json()
        
        output_path = 'data/players.json'
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents= True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=4)
            
        return data
    except requests.exceptions.RequestException as e:
        print(f'Erro ao conectar-se a API da riot {e}')
        return
    
    
all_challengers_players(url)