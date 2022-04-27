from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator
from faker import Faker
from asyncpg import NoDataFoundError, Pool, Connection
from modules.logger import LOGGER

_fake = Faker()


@dataclass
class UserPartial:
    username: str
    first_name: str
    last_name: str
    email: str
    registration_time: datetime


@dataclass
class User(UserPartial):
    user_id: int


def generate_user() -> UserPartial:
    date1 = datetime(datetime.now().year, 1, 1) - timedelta(weeks=8)
    date2 = datetime.now() - timedelta(weeks=1)

    return UserPartial(
        _fake.unique.user_name(),
        _fake.first_name(),
        _fake.last_name(),
        _fake.email(),
        _fake.date_time_between(date1, date2),
    )


async def upload_user(user: UserPartial, pool: Pool) -> User:
    sql = """
    INSERT INTO "user" (username, first_name, last_name, email, registration_time)
    VALUES ($1, $2, $3, $4, $5) RETURNING user_id;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        row = await db_conn.fetchrow(
            sql,
            user.username,
            user.first_name,
            user.last_name,
            user.email,
            user.registration_time,
        )

        if row == None:
            raise NoDataFoundError

        LOGGER.debug("uploaded new user with id: %s", row["user_id"])

    return User(
        user.username,
        user.first_name,
        user.last_name,
        user.email,
        user.registration_time,
        row["user_id"],
    )


async def fetch_random_users(amount: int, pool: Pool) -> Iterator[User]:
    sql = """
    SELECT
        user_id,
        username,
        first_name,
        last_name,
        email,
        registration_time
    FROM "user" TABLESAMPLE BERNOULLI(1)
    LIMIT $1;
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        rows = await db_conn.fetch(sql, amount)

        return (
            User(
                row["username"],
                row["first_name"],
                row["last_name"],
                row["email"],
                row["registration_time"],
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


async def update_user_email(user: User, pool: Pool) -> None:
    sql = """
    UPDATE "user"
    SET email = $2
    WHERE user_id = $1
    """

    async with pool.acquire() as db_conn:
        db_conn: Connection

        await db_conn.execute(sql, user.user_id, user.email)

        LOGGER.debug("updated email for user with id: %s", user.user_id)
