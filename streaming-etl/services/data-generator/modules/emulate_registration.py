from modules.logger import LOGGER
from modules.user import generate_user, upload_user


async def emulate_registration(pool) -> None:
    user = await upload_user(generate_user(), pool)

    LOGGER.info("Registered new user with id: %s", user.user_id)
