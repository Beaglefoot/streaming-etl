from dataclasses import dataclass
from typing import Iterable
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection
from modules.logger import LOGGER

_fake = Faker()


@dataclass
class UserPartial:
    first_name: str
    last_name: str
    email: str


@dataclass
class User(UserPartial):
    user_id: int


def generate_user() -> UserPartial:
    return UserPartial(_fake.first_name(), _fake.last_name(), _fake.email())


async def upload_user(user: UserPartial, pool: Pool) -> None:
    sql = """
    INSERT INTO "user" (first_name, last_name, email)
    VALUES ($1, $2, $3) RETURNING user_id;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(sql, user.first_name, user.last_name, user.email)

        if row == None:
            raise NoDataFoundError

        LOGGER.debug("uploaded new user with id: %s", row["user_id"])


async def fetch_random_users(amount: int, pool: Pool) -> Iterable[User]:
    sql = """
    SELECT
        user_id,
        first_name,
        last_name,
        email
    FROM "user" TABLESAMPLE BERNOULLI(1)
    LIMIT $1;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        rows = await db_conn.fetch(sql, amount)

        return (
            User(
                row["first_name"],
                row["last_name"],
                row["email"],
                user_id=row["user_id"],
            )
            for row in rows
        )


async def fetch_user_count(pool: Pool) -> int:
    async with pool.acquire() as db_conn:
        db_conn: Connection

        val = await db_conn.fetchval('SELECT count(*) FROM "user"')

        if val == None:
            raise NoDataFoundError

        return val
