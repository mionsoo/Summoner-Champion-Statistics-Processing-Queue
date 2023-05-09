import requests
import time

def get_json(target_url):
    server_error_count = 0
    while True:
        if server_error_count >= 2:
            raise Exception(f'SERVER ERROR')
        try:
            result = requests.get(url=target_url,
                                  headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                                         '(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'})

            if result.status_code == 200:
                return result.json()

            elif result.status_code == 429:
                waiting = int(result.headers['Retry-After'])
                print(f"Because of API Limit, it will restart in {waiting}s")
                time.sleep(waiting)

            elif result.status_code >= 500:
                print(f"ERROR {result.status_code}. It will restart in 60s")
                time.sleep(60)
                server_error_count += 1

            else:
                raise Exception(f'ERROR {result.status_code}')

        except requests.exceptions.ConnectionError as e:
            print(f"ERROR {e}. It will restart in 60s")
            time.sleep(60)
            server_error_count += 1
            pass


def get_json_time_limit(target_url, time_limit):
    """
    라이엇 API 호출시 설정한 time_limit만큼의 시간이 초과되면 error 발생하는 함수
    :param target_url:
    :param time_limit:
    :return:
    """
    while True:
        result = requests.get(url=target_url,
                              headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                                     '(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'},
                              timeout=time_limit)

        if result.status_code == 200:
            return result.json()

        elif result.status_code == 429:
            waiting = int(result.headers['Retry-After'])
            print(f"Because of API Limit, it will restart in {waiting}s")
            raise Exception(f'ERROR RETRY-AFTER')

        else:
            raise Exception(f'ERROR {result.status_code}')



class RiotV4:
    platform_id2region = {
        'BR1': 'BR',
        'EUN1': 'EUNE',
        'EUW1': 'EUW',
        'JP1': 'JP',
        'KR': 'KR',
        'LA1': 'LAN',
        'LA2': 'LAS',
        'NA1': 'NA',
        'OC1': 'OCE',
        'RU': 'RU',
        'TR1': 'TR',
        'PH2': 'PH',
        'SG2': 'SG',
        'TH2': 'TH',
        'TW2': 'TW',
        'VN2': 'VN'
    }


class RiotV4Tier(RiotV4):

    def __init__(self, api_key, platform_id):
        self.__api_key = api_key
        self.platform_id = platform_id
        self.region = self.platform_id2region[platform_id]

    def get_cgm_url(self, queue_type, tier):
        if tier == 'challenger':
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/' \
                         f'{queue_type}?api_key={self.__api_key}'
        elif tier == 'grandmaster':
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/' \
                         f'{queue_type}?api_key={self.__api_key}'
        else:  # tier == 'master'
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/league/v4/masterleagues/by-queue/' \
                         f'{queue_type}?api_key={self.__api_key}'
        return target_url

    def get_url(self, queue_type, tier, division, page):
        if page:
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/league/v4/entries/' \
                         f'{queue_type}/{tier}/{division}?page={page}&api_key={self.__api_key}'
        else:
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/league/v4/entries/' \
                         f'{queue_type}/{tier}/{division}?page=1&api_key={self.__api_key}'
        return target_url

    def get_by_summoner(self, summoner_id):
        target_url = f'https://{self.platform_id}.api.riotgames.com/lol/league/v4/entries/' \
                            f'by-summoner/{summoner_id}?api_key={self.__api_key}'
        return target_url


class RiotV4Summoner(RiotV4):
    def __init__(self, api_key, platform_id):
        self.__api_key = api_key
        self.platform_id = platform_id
        self.region = self.platform_id2region[platform_id]

    def get_url(self, summoner_name=False, summoner_id=False, puu_id=False):
        if summoner_name:
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/summoner/v4/summoners/by-name/' \
                                 f'{summoner_name}?api_key={self.__api_key}'
        elif summoner_id:
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/summoner/v4/summoners/' \
                                 f'{summoner_id}?api_key={self.__api_key}'
        elif puu_id:
            target_url = f'https://{self.platform_id}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/' \
                                 f'{puu_id}?api_key={self.__api_key}'
        else:
            raise Exception('summonerName, summonerId, puuid 중 하나 입력 필요')

        return target_url


class RiotV5(RiotV4):
    route2regionV5 = {
        'BR': 'americas',
        'EUNE': 'europe',
        'EUW': 'europe',
        'OCE': 'sea',
        'JP': 'asia',
        'KR': 'asia',
        'LAN': 'americas',
        'LAS': 'americas',
        'NA': 'americas',
        'RU': 'europe',
        'TR': 'europe',
        'PH': 'sea',
        'SG': 'sea',
        'TH': 'sea',
        'TW': 'sea',
        'VN': 'sea'
    }


class RiotV5Match(RiotV5):

    def __init__(self, api_key, platform_id, puu_id):
        self.__api_key = api_key
        self.platform_id = platform_id
        self.route = self.platform_id2region[platform_id]
        self.region_v5 = self.route2regionV5[self.route]
        self.puu_id = puu_id

    def get_match_ids_url(self, queue_type, start_time=False, end_time=False, start_idx=0, count=10):
        if queue_type == 'RANKED_SOLO_5x5':
            queue_id = 420
            queue_type = 'ranked'
        elif queue_type == 'NORMAL DRAFT':
            queue_id = 400
            queue_type = 'normal'
        elif queue_type == 'NORMAL':
            queue_id = 430
            queue_type = 'normal'
        elif queue_type == 'RANKED_FLEX_SR':
            queue_id = 440
            queue_type = 'ranked'
        elif queue_type == 'RANKED':
            queue_id = 420440
            queue_type = 'ranked'
        elif queue_type == 'CUSTOM':
            queue_id = 0
            queue_type = 'normal'
        elif queue_type == 'ARAM':
            queue_id = 450
            queue_type = 'normal'
        elif queue_type == 'URF':
            queue_id = 900
            queue_type = 'normal'
        elif queue_type == 'PICK_URF':
            queue_id = 1900
            queue_type = 'normal'
        elif queue_type == 'ULTBOOK':
            queue_id = 1400
            queue_type = 'normal'
        elif queue_type == 'AIGAME':
            queue_id = 850
            queue_type = 'normal'
        elif queue_type == 'AIGAME2':
            queue_id = 840
            queue_type = 'normal'
        elif queue_type == 'CLASH':
            queue_id = 700
        elif queue_type == 'CLASH_ARAM':
            queue_id = 720
        elif queue_type == 'ALL':
            queue_id = 100
        else:
            raise Exception('queue type 정확히 입력')

        if queue_id == 100:
            target_url = f'https://{self.region_v5}.api.riotgames.com/lol/match/v5/matches/by-puuid/' \
                         f'{self.puu_id}/ids?' \
                         f'&api_key={self.__api_key}&start={start_idx}&count={count}'
        elif queue_id == 420440:
            target_url = f'https://{self.region_v5}.api.riotgames.com/lol/match/v5/matches/by-puuid/' \
                         f'{self.puu_id}/ids?type={queue_type}' \
                         f'&api_key={self.__api_key}&start={start_idx}&count={count}'
        elif queue_id in (700, 720):
            target_url = f'https://{self.region_v5}.api.riotgames.com/lol/match/v5/matches/by-puuid/' \
                         f'{self.puu_id}/ids?queue={queue_id}' \
                         f'&api_key={self.__api_key}&start={start_idx}&count={count}'
        else:
            target_url = f'https://{self.region_v5}.api.riotgames.com/lol/match/v5/matches/by-puuid/' \
                         f'{self.puu_id}/ids?queue={queue_id}&type={queue_type}' \
                         f'&api_key={self.__api_key}&start={start_idx}&count={count}'

        if start_time and end_time:
            if type(start_time) == int and type(end_time) == int:
                # self.__target_root += f'&startTime={start_time}&endTime={end_time}&start={start_idx}&count={count}'
                target_url += f'&startTime={start_time}&endTime={end_time}'
            else:
                raise TypeError('start_time과 end_time을 입력할 때는 integer timestamp (ex. 6124551123) 형태로 입력 필요')

        return target_url

    def get_match_url(self, match_id, target_type):
        if target_type == 'match':
            target_url = f'https://{self.region_v5}.api.riotgames.com/lol/match/v5/matches/' \
                                 f'{match_id}?api_key={self.__api_key}'
        else:  # target_type == 'timeline'
            target_url = f'https://{self.region_v5}.api.riotgames.com/lol/match/v5/matches/' \
                                 f'{match_id}/timeline?api_key={self.__api_key}'
        return target_url


class Ddragon(RiotV4):

    def __init__(self, platform_id):
        self.region: str = self.platform_id2region[platform_id]

    def get_versions_url(self):
        target_url = f'https://ddragon.leagueoflegends.com/realms/{self.region.lower()}.json'

        return target_url

    @ staticmethod
    def get_champions_url(version):
        target_url = f'https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/championFull.json'

        return target_url


class RiotV4Spectator(RiotV4):
    def __init__(self, api_key, platform_id):
        self.__api_key = api_key
        self.platform_id = platform_id

    def get_url(self, summoner_id=False):
        target_url = f'https://{self.platform_id}.api.riotgames.com/lol/spectator/v4/active-games/by-summoner/' \
                             f'{summoner_id}?api_key={self.__api_key}'

        return target_url


class RiotV4ChampionMastery(RiotV4):
    def __init__(self, api_key, platform_id):
        self.__api_key = api_key
        self.platform_id = platform_id

    def get_url(self, summoner_id: str, champion_id: int):
        target_url = f'https://{self.platform_id}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/' \
                     f'by-summoner/{summoner_id}/by-champion/{champion_id}?api_key={self.__api_key}'

        return target_url


class RiotV1Challenges(RiotV4):
    def __init__(self, api_key, platform_id):
        self.__api_key = api_key
        self.platform_id = platform_id

    def get_url(self, puu_id: str):
        target_url = f'https://{self.platform_id}.api.riotgames.com/lol/challenges/v1/player-data/' \
                     f'{puu_id}?api_key={self.__api_key}'

        return target_url