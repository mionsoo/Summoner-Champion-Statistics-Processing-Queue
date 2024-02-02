import time
import traceback

import sys
sys.path.append("/usr/src/app")
from common.utils import get_current_datetime
from common.const import Status
from core.stat_summoner_match_queue import SummonerMatchQueueOperator
from core.stat_queue_sys import QueueComment
import asyncio


async def main():
    queue_comment = QueueComment()
    queue_op = SummonerMatchQueueOperator()

    while True:
        try:
            await queue_op.update_new_data()

            if queue_op.is_all_queue_is_empty() and queue_comment.is_need_to_print_empty():
                print(f'{get_current_datetime()} | Queue is Empty')
                print('------------------------------\n')
                queue_comment.empty_printed()
                time.sleep(60)

            elif queue_op.is_data_exists():
                current_objs = await queue_op.get_current_obj(3)
                if None in current_objs:
                    pass
                else:
                    tasks = [asyncio.create_task(queue_op.process_job(current_obj)) for current_obj in current_objs]
                    await asyncio.gather(*tasks)
                    await queue_op.print_remain()
                    print('------------------------------\n')

                queue_comment.print_empty()

        except Exception:
            print(traceback.format_exc())


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)



