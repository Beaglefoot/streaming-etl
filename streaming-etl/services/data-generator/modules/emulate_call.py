import asyncio
from datetime import datetime
from random import shuffle

from modules.call import CallPartial, update_call_end_time, upload_call
from modules.call_participant import (
    CallParticipantPartial,
    update_call_participant_leave_time,
    upload_call_participant,
)
from modules.logger import LOGGER
from modules.user import fetch_random_users
from modules.utils import get_ndist_random


async def emulate_call(pool) -> None:
    call = await upload_call(CallPartial(datetime.now(), None), pool)
    users = list(await fetch_random_users(get_ndist_random(4), pool))

    LOGGER.info("Started new call with id: %s", call.call_id)

    # Participants join
    for user in users:
        cp = CallParticipantPartial(call.call_id, user.user_id, datetime.now(), None)
        await upload_call_participant(cp, pool)
        await asyncio.sleep(get_ndist_random(2))

    shuffle(users)

    # Participants leave
    for user in users:
        await update_call_participant_leave_time(
            call.call_id, user.user_id, datetime.now(), pool
        )
        await asyncio.sleep(get_ndist_random(2))

    # Call ends
    await update_call_end_time(call.call_id, datetime.now(), pool)

    LOGGER.info("Finished call with id: %s", call.call_id)
