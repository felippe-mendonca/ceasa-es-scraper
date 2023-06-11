import asyncio
from aiohttp import ClientSession
from datetime import datetime

from ceasa import CeasaESScraper
from spreadsheet import CeasaESSpreadsheet


N_CONCURRENT_TAKS = 5

async def bound_requests(semaphore, predicate):
    async with semaphore:
        return await predicate


async def main():
    sheet = CeasaESSpreadsheet()
    sheet_boletins_ids = sheet.get_boletins()

    async with ClientSession() as session:
        scraper = CeasaESScraper(session)

        ceasa_boletins_ids = []
        mercados = await scraper.get_mercados()
        for mercado in mercados:
            datas = await scraper.get_datas(mercado)
            ceasa_boletins_ids.extend([(mercado, data) for data in datas])
        
        boletins_to_fetch_ids = set(ceasa_boletins_ids) - set(sheet_boletins_ids)
        print(f"{len(boletins_to_fetch_ids)} reports to fetch")

        semaphore = asyncio.Semaphore(N_CONCURRENT_TAKS)
        boletim_tasks = [
            bound_requests(semaphore, scraper.get_boletim(mercado, data))
            for mercado, data in boletins_to_fetch_ids
        ]

        boletins = []
        for future_boletim in asyncio.as_completed(boletim_tasks):
            boletim = await future_boletim
            boletins.append(boletim)
            print(f"[{datetime.now()}] mercado={boletim.mercado.id} data={boletim.data.strftime('%d/%m/%Y')}")

        sheet.add_boletins(boletins)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
