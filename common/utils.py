import asyncio
from datetime import datetime, timedelta

from common.const import Status


def get_current_datetime() -> datetime:
    return datetime.now() + timedelta(hours=9)


async def get_changed_current_obj_status(current_obj, func_return) -> int:
    await asyncio.sleep(0)
    if current_obj.status == Status.Waiting.code and func_return is None:
        changed_status = Status.Success.code
        comment = "is changed Waiting to Success (No matches to insert)"

    elif current_obj.status == Status.Working.code and func_return is None:
        changed_status = Status.Success.code
        comment = "is changed Working to Success"

    elif current_obj.status == Status.Waiting.code:
        changed_status = Status.Working.code
        comment = "is changed Waiting to Working"

    elif current_obj.status == Status.Working.code:
        changed_status = Status.Working.code
        comment = f'is still processing\n"{current_obj}"'
        await asyncio.sleep(5)

    else:
        changed_status = Status.Waiting.code
        comment = "is changed Waiting to Waiting (current status is not 0 or 2)"

    print(f"{get_current_datetime()} |", comment)
    return changed_status


def logging_time(original_fn):
    import time
    from functools import wraps

    @wraps(original_fn)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = original_fn(*args, **kwargs)

        end_time = time.time()
        print(f"{get_current_datetime()} | WorkingTime[{original_fn.__name__}]: {round(end_time - start_time, 4)} sec")
        return result

    return wrapper
