from asyncpg import NoDataFoundError
from faker import Faker
from modules.logger import LOGGER
from modules.user import fetch_random_users, update_user_email

_fake = Faker()


async def emulate_user_edit(pool) -> None:
    user = next(await fetch_random_users(1, pool), None)

    if user == None:
        raise NoDataFoundError

    user.email = _fake.email()

    await update_user_email(user, pool)

    LOGGER.info("Email changed for user with id: %s", user.user_id)
