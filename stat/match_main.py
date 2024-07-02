import traceback
from itertools import chain

import sys
sys.path.append("/usr/src/app")
from common.utils import get_current_datetime
from common.const import Status, M_EXECUTE_SUMMONER_COUNT
from common.db import connect_sql_aurora_async, RDS_INSTANCE_TYPE, execute_update_queries_match, update_current_obj_status
from core.stat_summoner_match_queue import SummonerMatchQueueOperator
from core.stat_queue_sys import QueueEmptyComment
import asyncio


async def main():
    queue_comment = QueueEmptyComment()
    queue_op = SummonerMatchQueueOperator()
    conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)

    while True:
        try:
            await queue_op.update_new_data(conn)

            if queue_op.is_all_job_done() and queue_comment.is_set_print():
                await queue_comment.print_job_ended()

            elif queue_op.is_all_job_done():
                await queue_op.sleep_queue()

            elif queue_op.is_data_exists():
                queue_comment.set_job_not_done()

                current_objs = await queue_op.get_current_obj(M_EXECUTE_SUMMONER_COUNT)
                if None in current_objs:
                    continue

                tasks = []
                async with conn.cursor() as cursor:
                    for idx, current_obj in enumerate(current_objs):
                        await cursor.execute(
                            'SELECT match_id '
                            'FROM b2c_summoner_match_queue '
                            f'WHERE puu_id = {repr(current_obj.puu_id)} '
                            f'and platform_id = {repr(current_obj.platform_id)} '
                            f'and status = {Status.Working.code} '
                        )
                        result = await cursor.fetchall()
                        match_ids = sum(list(result), ())
                        tasks.append(asyncio.create_task(queue_op.process_job(current_obj, match_ids=match_ids)))

                queries = await asyncio.gather(*tasks)
                t_queries = sum(chain.from_iterable(queries), [])

                match_id_lists, error_match_id_lists = await execute_update_queries_match(conn, t_queries)
                await update_current_obj_status(conn, match_id_lists, error_match_id_lists, current_objs)
                await queue_op.print_counts_remain(conn)
                print('------------------------------\n')


        except Exception:
            print(traceback.format_exc())


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)

