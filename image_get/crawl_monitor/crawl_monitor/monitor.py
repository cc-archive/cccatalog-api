import asyncio
import aredis
import crawl_monitor.settings as settings


async def adjust_rates():
    while True:
        await asyncio.sleep(settings.MONITOR_INTERVAL_SECONDS)


async def log_state():
    while True:
        await asyncio.sleep(settings.LOG_FREQUENCY_SECONDS)


async def monitor():
    await adjust_rates()
    await log_state()


if __name__ == '__main__':
    asyncio.run(monitor(), debug=True)
