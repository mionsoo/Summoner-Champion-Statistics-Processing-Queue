from dataclasses import dataclass
from os import path, environ
import boto3
import os
import pymysql

import aiomysql
from dataclasses import asdict


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


def sql_execute_dict(query, conn):
    """
        SQL 작업 처리하여 리턴 값 반환
        query : 작업 쿼리
        conn : mysql connect 변수
    """
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
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


async def connect_pool_sql_aurora_async(instance_type):
    host_url = get_rds_instance_host(instance_type)
    return await aiomysql.create_pool(
        host=host_url,
        user='dbmasteruser',
        password=db_pass,
        db=conf_dict.get('AURORA_DB'),
        charset='utf8mb4'
    )


async def execute_update_queries_match(conn, queries):
    async with conn.cursor() as cursor:
        await cursor.execute(
            'INSERT INTO b2c_summoner_match_queue(match_id, puu_id, platform_id, status) '
            f'VALUES{", ".join(map(repr, sum(queries, [])))} as queue '
            f'ON DUPLICATE KEY UPDATE status=queue.status'
        )
        # for query in queries:
        #     await cursor.execute(query)
        await conn.commit()


async def execute_update_queries_summoner(conn, queries):
    async with conn.cursor() as cursor:
        await cursor.execute(
            'INSERT INTO b2c_summoner_queue(puu_id, platform_id, status, reg_date, reg_datetime) '
            f'VALUES{", ".join(queries)} as queue '
            f'ON DUPLICATE KEY UPDATE status=queue.status'
        )
        # for query in queries:
        #     await cursor.execute(query)
        await conn.commit()
