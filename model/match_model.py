from pydantic import BaseModel
from datetime import datetime, date

class BatchStatQueueContainer(BaseModel):
    puu_id: str
    match_id: str
    platform_id: str
    season: int
    creation_timestamp: int
    queue_id: int
    position: str
    champion_id: int
    enemy_champion_id: int
    is_win: int
    is_remake: int
    is_runaway: int
    kills: int
    deaths: int
    assists: int
    damage_taken: int
    damage_dealt: int
    cs: int
    gold_diff_15: int
    gold_per_team: float
    damage_per_team: float
    game_duration: int
    gold: int
    kill_point: float
    vision_score: int
    penta_kills: int
    quadra_kills: int
    triple_kills: int
    double_kills: int
    top: int
    jungle: int
    middle: int
    bot: int
    supporter: int
    cs_15: int

class MatchStatsQueueContainer(BaseModel):
    platform_id: str
    puu_id: str
    match_id: str
    status: int
    reg_date: date = date.today()
    reg_datetime: datetime = datetime.now()