from leiserbik import *
from leiserbik.core import __generate_search_url_by_range, _get_page_branches,_get_branch_statuses, _get_user_statuses
from leiserbik.async_http import fetch_all
from pypeln import asyncio_task as aio
from pypeln import thread as th

def query(query: str, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

    logger.debug("Converting dates from string")
    init_date = arrow.get(start_date)
    finish_date = arrow.get(end_date) 

    logger.info("Scrapping 🐦 with:[%s] From 🗓️:[%s] ➡️ To 🗓️:[%s]" % (query, init_date.format('YYYY-MM-DD'), finish_date.format('YYYY-MM-DD')))

    # Create day urls
    urls = __generate_search_url_by_range(query, init_date, finish_date)

    stage_results = fetch_all(urls)
    stage_results = aio.flat_map(_get_page_branches, stage_results, workers=15)
    stage_results = th.flat_map(_get_branch_statuses, stage_results, workers=15)


    results = list(set(stage_results))

    logger.info(f"Getted {len(results)} 💬")

    return results

def user_activity(user:str, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    logger.info(f"Retrieving activiy {user} 💬 ")
    results = query(f"from:{user} OR to:{user} OR on:{user}", start_date, end_date)
    logger.info(f"Retrieved activiy {user} statuses: {len(results)} 💬 ")
    return results

def user_by_id(user:str, max_id: int = 0):
    logger.info(f"Retrieving info {user} 💬 ")
    results = _get_user_statuses(user)
    logger.info(f"Retrieved {user} statuses: {len(results)} 💬 ")
    return results

def user_by_query(user:str, max_id: int = 0):
    logger.info(f"Retrieving activiy {user} 💬 ")
    results = query(f"from:{user}", arrow.get().format(SHORT_DATE_FORMAT), end_date =arrow.get().shift(months=-15))
    logger.info(f"Retrieved activiy {user} statuses: {len(results)} 💬 ")
    return results

