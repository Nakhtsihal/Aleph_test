import asyncio
import json

from parser import parse_syslog
from enricher import Enricher

async def main():
    with open("../data/ha_syslogs.json") as file:
        syslogs = json.load(file)
        tasks = []
        enricher = Enricher()
        for log in syslogs:
            parsed_log = parse_syslog(log)
            enriched_log = enricher.enrich_log(parsed_log)
            tasks.append(enriched_log)

        enriched_logs = await asyncio.gather(*tasks)

    print(len(enriched_logs))



asyncio.run(main())