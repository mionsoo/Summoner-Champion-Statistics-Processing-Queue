from common.const import Status
from datetime import datetime, timedelta


def get_current_datetime() -> datetime:
    return datetime.now() + timedelta(hours=9)


def change_current_obj_status(current_obj, func_return) -> int:
    if current_obj.status == Status.Waiting.code and func_return is None:
        changed_status = Status.Success.code
        comment = 'is changed Waiting to Success\n(No matches to insert)'

    elif current_obj.status == Status.Working.code and func_return is None:
        changed_status = Status.Success.code
        comment = 'is changed Working to Success'

    elif current_obj.status == Status.Waiting.code:
        changed_status = Status.Working.code
        comment = 'is changed Waiting to Working'

    elif current_obj.status == Status.Working.code:
        changed_status = Status.Working.code
        comment = 'is still processing'

    else:
        changed_status = Status.Waiting.code
        comment = ('is changed Waiting to Waiting\n'
                   '(current status is not 0 or 2)')

    print(f'{get_current_datetime()} |', *current_obj.__dict__.values(), comment)
    return changed_status
