import asyncio

from modules.process_call import process_call


async def main() -> None:
    await process_call()


if __name__ == "__main__":
    asyncio.run(main())
