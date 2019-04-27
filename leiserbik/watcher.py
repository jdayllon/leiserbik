import operator

from pypeln import asyncio_task as aio
from pypeln import thread as th

from leiserbik import *
from leiserbik.async_http import fetch_all
from leiserbik.core import __generate_search_url_by_range, _get_page_branches, _get_branch_statuses, _get_user_statuses
from leiserbik.query import TwitterQuery


def query(tq : TwitterQuery ):

    cur_query = tq.query(with_dates=False)
    logger.debug(f"Obtainer Twitter Query Object with query 🔎 {cur_query}")
    return rawquery(cur_query, tq.start_date, tq.end_date)

def iter_query(tq : TwitterQuery ):
    cur_query = tq.query(with_dates=False)
    logger.debug(f"Obtainer Twitter Query Object with query 🔎 {cur_query}")
    return iter_rawquery(cur_query, tq.end_date)

def rawquery(query: str, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

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

def iter_rawquery(query: str, end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

    def list_no_dupes(l):
        return list(set(l))

    def not_in_list(l1, l2):
        return list(set(l2) - set(l1))

    # if we are iterating, start_date is "now"
    start_date: str = arrow.get().format(SHORT_DATE_FORMAT)

    # First call get everything until now
    all_status_until_now = list_no_dupes(rawquery(query, start_date, end_date))
    yield all_status_until_now

    while True:
        cur_date = arrow.get().format(SHORT_DATE_FORMAT)
        cur_statuses = rawquery(query, cur_date, cur_date)
        cur_new_statuses =  not_in_list(all_status_until_now, cur_statuses)
        logger.info(f"Found: {len(cur_statuses)} 💬")
        all_status_until_now += cur_new_statuses

        yield cur_new_statuses

    return


def user_activity(user:str, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    logger.info(f"Retrieving activiy {user} 💬 ")
    results = rawquery(f"from:{user} OR to:{user} OR on:{user}", start_date, end_date)
    logger.info(f"Retrieved activiy {user} statuses: {len(results)} 💬 ")
    return results

def iter_user_activity(user:str, end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    logger.info(f"Retrieving activiy {user} 💬 ")
    return iter_rawquery(f"from:{user} OR to:{user} OR on:{user}", end_date)



def user_by_id(user:str, max_id: int = 0):
    logger.info(f"Retrieving info {user} 💬 ")
    results = _get_user_statuses(user, max_id)
    logger.info(f"Retrieved {user} statuses: {len(results)} 💬 ")
    return results

def user_by_query(user:str, max_id: int = 0):
    logger.info(f"Retrieving activiy {user} 💬 ")
    results = rawquery(f"from:{user}", arrow.get().format(SHORT_DATE_FORMAT), end_date =arrow.get().shift(months=-15))
    logger.info(f"Retrieved activiy {user} statuses: {len(results)} 💬 ")
    return results

def hashtag(hashtag, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

    query_hashtag = hashtag.replace("#","")

    logger.info(f"Querying for {query_hashtag}")

    results = rawquery(query_hashtag, start_date, end_date)
    logger.info(f"Retrieved hashtag {query_hashtag} statuses: {len(results)} 💬 ")

def hashtags(hashtags, type_operator = operator.or_, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

    if type_operator == operator.or_:

        query_hashtag = " OR ".join(hashtags)

    elif type_operator == operator.and_:

        query_hashtag = " AND ".join(hashtags)

    logger.info(f"Querying for '{query_hashtag}'")

    results = rawquery(query_hashtag, start_date, end_date)
    logger.info(f"Retrieved hashtag {query_hashtag} statuses: {len(results)} 💬 ")

def geolocation(lat,lon, radius, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    logger.info(f"Retrieving for [LAT:{lan}, LON:{lon},  Radious:{radius}] 🗺 ")
    results = rawquery(f"geocode:{lan},{lon},{lon}", start_date, end_date)
    logger.info(f"Retrieved for locaton [LAT:{lan}, LON:{lon},  Radious:{radius}] 🗺 statuses : {len(results)} 💬 ")
    return results
