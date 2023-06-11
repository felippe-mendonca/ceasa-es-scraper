import asyncio
from aiohttp import ClientSession
from datetime import datetime

from ceasa import CeasaESScraper


N_CONCURRENT_TAKS = 10


async def bound_requests(semaphore, predicate):
    async with semaphore:
        return await predicate


async def main():
    async with ClientSession() as session:
        scraper = CeasaESScraper(session)

        mercado_datas = []
        mercados = await scraper.get_mercados()
        for mercado in mercados:
            datas = await scraper.get_datas(mercado)
            mercado_datas.extend([(mercado, data) for data in datas])

        semaphore = asyncio.Semaphore(N_CONCURRENT_TAKS)

        async def boletim_task(mercado, data):
            t0 = datetime.now()
            b = await scraper.get_boletim(mercado, data)
            took_s = (datetime.now() - t0).total_seconds()
            print(
                f"[{datetime.now()}] mercado={mercado.id} data={data.strftime('%d/%m/%Y')} took={took_s:.2f}s"
            )
            return b

        boletim_tasks = [
            bound_requests(semaphore, boletim_task(mercado, data))
            for mercado, data in mercado_datas
        ]
        boletins = await asyncio.gather(*boletim_tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
