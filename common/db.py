from dataclasses import dataclass
from os import path, environ
import boto3
import os
import pymysql

import aiomysql
from dataclasses import asdict
from model.match_model import BatchStatQueueContainer, MatchStatsQueueContainer
from core.Job.stat_job import JobResult
from typing import List
from common.const import Status
import asyncio


aws_access = os.environ.get("AWS_ACCESS", None)
aws_secret = os.environ.get("AWS_SECRET", None)

region = "ap-northeast-2"
base_dir = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))

ssm_client = boto3.client(
        'ssm',
        region_name=region,
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret,
)
ssm_key = ssm_client.get_parameter(Name='b2c_mongo_pass')
_mongo_pass = ssm_key['Parameter']['Value']

parameter = ssm_client.get_parameter(Name='riot_api_key', WithDecryption=False)
db_pass = ssm_client.get_parameter(Name='b2c_db_pass', WithDecryption=False)['Parameter']['Value']
riot_api_key = parameter['Parameter']['Value']


@dataclass
class RDS_INSTANCE_TYPE:
    READ = 'READ'
    WRITE = 'WRITE'
    PLAIN = 'PLAIN'


@dataclass
class Config:
    """
    기본 Configuration
    """
    BASE_DIR: str = base_dir
    DB_POOL_RECYCLE: int = 900
    DB_ECHO: bool = True
    DEBUG: bool = False
    TEST_MODE: bool = False
    MONGO_URL: str = f"mongodb+srv://test:{_mongo_pass}@cluster0.skjtv.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"


@dataclass
class ProdConfig(Config):
    TRUSTED_HOSTS = ["*"]
    ALLOW_SITE = ["*"]
    MYSQL_HOST: str = '172.31.10.236'
    AURORA_HOST: str = 'b2c-deeplol.cluster-clnwhi0hsaib.ap-northeast-2.rds.amazonaws.com'
    AURORA_READ_HOST: str = 'b2c-deeplol-p-cluster.cluster-ro-clnwhi0hsaib.ap-northeast-2.rds.amazonaws.com'
    AURORA_WRITE_HOST: str = 'b2c-deeplol-p-cluster.cluster-clnwhi0hsaib.ap-northeast-2.rds.amazonaws.com'
    AURORA_DB: str = 'deeplol'
    # DOMAIN: str = 'https://renew.deeplol.gg'
    DOMAIN: str = 'http://host.docker.internal'
    MEMCACHED: str = 'deeplol-cache.txxcqd.cfg.apn2.cache.amazonaws.com:11211'
    TABLE_STR: str = ''


def conf():
    """
    환경 불러오기
    :return:
    """
    config = dict(prod=ProdConfig, dev=ProdConfig)
    return config[environ.get("API_ENV", "prod")]()


conf_dict = asdict(conf())


def get_rds_instance_host(instance_type: RDS_INSTANCE_TYPE):
    if instance_type == RDS_INSTANCE_TYPE.READ:
        return conf_dict.get('AURORA_READ_HOST')
    elif instance_type == RDS_INSTANCE_TYPE.WRITE:
        return conf_dict.get('AURORA_WRITE_HOST')
    elif instance_type == RDS_INSTANCE_TYPE.PLAIN:
        return conf_dict.get('AURORA_HOST')
    else:
        raise Exception('Wrong RDS INSTANCE TYPE')


def connect_sql_aurora(instance_type: RDS_INSTANCE_TYPE):
    """
    메인 db 커서
    :return:
    """
    host_url = get_rds_instance_host(instance_type)

    conn = pymysql.connect(
        host=host_url,
        user='dbmasteruser', password=db_pass, db=conf_dict.get('AURORA_DB'), charset='utf8mb4')

    return conn


def sql_execute(query, conn):
    """
        SQL 작업 처리하여 리턴 값 반환
        query : 작업 쿼리
        conn : mysql connect 변수
    """
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result



async def connect_sql_aurora_async(instance_type):
    host_url = get_rds_instance_host(instance_type)
    return await aiomysql.connect(
        host=host_url,
        user='dbmasteruser',
        password=db_pass,
        db=conf_dict.get('AURORA_DB'),
        charset='utf8mb4',
        autocommit=True
    )


def check_types(value):
    if isinstance(value, str):
        return repr(value)
    else:
        return value

def make_insert_champion_stats_queries(query: BatchStatQueueContainer):
    return f'{tuple(query.__dict__.values())}'

def make_insert_summoner_match_query(query: MatchStatsQueueContainer, p_id):
    query.asd(p_id)
    return f"({', '.join([repr(q) if isinstance(q, str) else str(q) if isinstance(q, int) else repr(str(q))  for q in query.__dict__.values()])})"
    # return f'{tuple(query.__dict__.values())}'

def make_insert_duplicate_keys(table_alias):
    return ','.join([f' {i}={table_alias}.{i}' for i in BatchStatQueueContainer.__dict__['__fields__'].keys()])


async def update_current_obj_status(conn, current_objs, t_queries: List[BatchStatQueueContainer | str]):
    sorted_stats_queue_containers = [sort_match_stats_queue_container(query) for query in t_queries]
    p_id = {i.puu_id: i.platform_id for i in current_objs}
    match_containers, error_match_containers = zip(*sorted_stats_queue_containers)
    match_containers = list(filter(lambda item: item is not None and item.puu_id in p_id.keys(), match_containers))
    error_match_containers = list(filter(lambda item: item is not None and item.puu_id in p_id.keys(), error_match_containers))

    match_id_lists_query = ', '.join([make_insert_summoner_match_query(query, p_id) for query in match_containers])
    error_match_id_lists_query = ','.join([make_insert_summoner_match_query(query, p_id) for query in error_match_containers])

    async with conn.cursor() as cursor:
        if error_match_id_lists_query:
            await update_summoner_matches(conn, cursor, error_match_id_lists_query)
        await update_summoner_matches(conn, cursor, match_id_lists_query)


async def update_summoner_matches(conn, cursor, match_id_lists_query):
    await cursor.execute(
        f'INSERT INTO b2c_summoner_match_queue(platform_id, puu_id, match_id, status, reg_date, reg_datetime) '
        f'values {match_id_lists_query} as t '
        f'ON DUPLICATE KEY UPDATE status = t.status'
    )
    await conn.commit()


def sort_match_stats_queue_container(query: BatchStatQueueContainer|str):
    if 'error' in query:
        container = MatchStatsQueueContainer(
            match_id=query.split(', ')[0],
            platform_id=query.split(', ')[1],
            puu_id=query.split(', ')[2],
            status=query.split(', ')[3]
        )
        return [None, container]

    container = MatchStatsQueueContainer(
        match_id=query.match_id,
        platform_id=query.platform_id,
        puu_id=query.puu_id,
        status=Status.Success.code
    )

    return [container, None]


async def execute_match_insert_queries(conn, t_queries: List[BatchStatQueueContainer|str]):
    value_query = ','.join([make_insert_champion_stats_queries(query) for query in t_queries if 'error' not in query])
    if value_query:
        table_alias = 'stats_p'
        duplicate_key_update_query = make_insert_duplicate_keys(table_alias)
        await insert_summoner_champion_stats(conn, table_alias, value_query, duplicate_key_update_query)



async def insert_summoner_champion_stats(conn, table_alias, value_query, duplicate_key_update_query):
    async with conn.cursor() as cursor:
        await cursor.execute(
            query=(
                f'INSERT INTO b2c_summoner_champion_stats_partitioned(puu_id, match_id, platform_id, season, creation_timestamp ,queue_id, position, champion_id,'
                f'enemy_champion_id, is_win,is_remake ,is_runaway ,kills, deaths, assists, damage_taken, damage_dealt, cs, gold_diff_15, gold_per_team,'
                f'damage_per_team, game_duration, gold, kill_point, vision_score, penta_kills, quadra_kills, triple_kills, '
                f'double_kills,top, jungle, middle, bot, supporter,cs_15) '
                f'values {value_query} as {table_alias} '
                f'ON DUPLICATE KEY UPDATE'
                f'{duplicate_key_update_query}'
            )
        )
        await conn.commit()


def make_bulk_insert_query_values_summoner_match_queue(current_obj, match_id) -> str:
    return f'({repr(current_obj.platform_id)}, {repr(current_obj.puu_id)}, {repr(match_id)}, {Status.Working.code})'

def make_summoner_insert_query(current_obj, changed_current_obj_status_code):
    return f'({repr(current_obj.puu_id)}, {repr(current_obj.platform_id)}, {changed_current_obj_status_code}, {repr(str(current_obj.reg_date))}, {repr(str(current_obj.reg_datetime))})'

async def execute_update_queries_summoner(conn, job_results):
    async with conn.cursor() as cursor:
        bulk_item = ', '.join(
            [make_summoner_insert_query(job_result.target_obj, job_result.result_status) for job_result in job_results])

        # queries = make_summoner_insert_query(current_obj, changed_current_obj_status_code)
        await cursor.execute(
            'INSERT INTO b2c_summoner_queue(puu_id, platform_id, status, reg_date, reg_datetime) '
            f'VALUES{bulk_item} as queue '
            f'ON DUPLICATE KEY UPDATE status=queue.status'
        )
        # for query in queries:
        #     await cursor.execute(query)
        await conn.commit()

        return 0

async def execute_update_queries_summoner_wait(conn, job_result: JobResult):
    match_ids=job_result.data
    current_obj=job_result.target_obj
    changed_current_obj_status_code=job_result.result_status



    async with conn.cursor() as cursor:
        if match_ids != -1 and match_ids is not None:
            try:
                await cursor.execute(
                    'SELECT match_id '
                    'FROM b2c_summoner_champion_stats_partitioned '
                    f'WHERE puu_id = {repr(current_obj.puu_id)} '
                    f'and platform_id = {repr(current_obj.platform_id)} '
                    f'and season = {current_obj.season} '
                    f'and queue_id in (420, 440, 450, 900, 1900)'
                )
                db_called_match_ids = await cursor.fetchall()
                db_called_match_ids = set(sum(db_called_match_ids, ()))
                remove_duplicated_match_ids = match_ids.difference(db_called_match_ids)

                bulk_item = ', '.join(
                    [make_bulk_insert_query_values_summoner_match_queue(current_obj, match_id) for match_id in
                     remove_duplicated_match_ids])
            except:
                pass

            if current_obj.status == Status.Waiting.code and bulk_item:
                try:
                    await execute_summoner_match_insert_query(bulk_item, conn, cursor)

                except pymysql.err.OperationalError:
                    await asyncio.sleep(3)
                    await execute_summoner_match_insert_query(bulk_item, conn, cursor)

                except aiomysql.IntegrityError:
                    pass


        queries = make_summoner_insert_query(current_obj, changed_current_obj_status_code)
        await execute_summoner_insert_query(conn, cursor, queries)

        return 0


async def execute_summoner_insert_query(conn, cursor, queries):
    await cursor.execute(
        'INSERT INTO b2c_summoner_queue(puu_id, platform_id, status, reg_date, reg_datetime) '
        f'VALUES{queries} as queue '
        f'ON DUPLICATE KEY UPDATE status=queue.status'
    )

    await conn.commit()


async def execute_summoner_match_insert_query(bulk_item, conn, cursor):
    await cursor.execute(
        'INSERT ignore INTO b2c_summoner_match_queue '
        '(platform_id, puu_id, match_id, status) '
        f'VALUES {bulk_item}'
    )
    await conn.commit()
