import asyncio
import os

from opensearchpy import AsyncOpenSearch

from osbenchmark.async_connection import AIOHttpConnection

async_client = AsyncOpenSearch(
    hosts=[{"host": "opense-clust-0wEa2jJaumpT-72b822874267d2ec.elb.us-east-1.amazonaws.com", "port": 80}],
    use_ssl=False,
    verify_certs=False,
    connection_class=AIOHttpConnection
)
query = {
    "query": {
        "term": {
            "log.file.path": {
                "value": "/var/log/messages/birdknight"
            }
        }
    }
}

agg_query = {
    "size": 0,
    "aggs": {
        "agent": {
            "cardinality": {
                "field": "agent.name"
            }
        }
    }
}

async def search():
    info = await async_client.search(index="big5", body=agg_query)
    return info


async def main() -> None:
    async with async_client:
        res = await asyncio.gather(search(), search(), search(), search(), search())
    print(res)



if __name__ == '__main__':
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
