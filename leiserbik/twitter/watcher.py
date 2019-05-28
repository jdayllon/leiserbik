import operator

import arrow
from pypeln import asyncio_task as aio
from pypeln import thread as th

from leiserbik import *
from leiserbik.async_http import fetch_all
from leiserbik.core import __generate_search_url_by_range, _get_page_branches, __get_statuses, _get_user_statuses, \
    list_no_dupes, not_in_list, _get_branch_walk, _session_get_requests, _read_statuses, __update_status_stats, \
    _update_status_stats, _send_kafka
from leiserbik.query import TwitterQuery


def query(tq : TwitterQuery ):

    cur_query = tq.query(with_dates=False)
    logger.debug(f"🔎 Obtainer Twitter Query Object with query {cur_query}")
    return rawquery(cur_query, tq.start_date, tq.end_date)

def iter_query(tq : TwitterQuery ):
    cur_query = tq.query(with_dates=False)
    logger.debug(f"🔎 Obtainer Twitter Query Object with query {cur_query}")
    return iter_rawquery(cur_query, tq.end_date)


def rawquery(query: str, start_date: str = arrow.get().format(SHORT_DATE_FORMAT),
             end_date: str = arrow.get().shift(days=-15).
             format(SHORT_DATE_FORMAT), hydrate: int=0, kafka: bool = False):

    logger.debug("Converting dates from string")
    init_date = arrow.get(start_date)
    finish_date = arrow.get(end_date)

    logger.info("🐦 Scrapping with:[%s] From 🗓️:[%s] ➡️ To 🗓️:[%s]" % (query, init_date.format('YYYY-MM-DD'),
                                                                         finish_date.format('YYYY-MM-DD')))

    # Create day urls
    urls = __generate_search_url_by_range(query, init_date, finish_date)

    stage_results = fetch_all(urls)

    stage_results = aio.flat_map(_get_page_branches, stage_results, workers=MAX_WORKERS)
    stage_results = th.flat_map(_get_branch_walk, stage_results, workers=MAX_WORKERS)
    if hydrate == 0:
        stage_results = th.flat_map(__get_statuses, stage_results, workers=MAX_WORKERS)
    elif hydrate == 1:
        stage_results = th.flat_map(_read_statuses, stage_results, workers=MAX_WORKERS)
        stage_results = th.map(_update_status_stats, stage_results, workers=MAX_WORKERS)
    else:
        raise NotImplementedError

    if kafka:
        stage_results = th.map(_send_kafka, stage_results, workers=MAX_WORKERS)


    # List conversion executes pipeline
    results = list(stage_results)
    results = list_no_dupes(results)

    logger.info(f"💬 Getted {len(results)}")

    return results

def iter_rawquery(query: str, end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT), hydrate:int = 0, kafka: bool = False):

    # if we are iterating, start_date is "now"
    start_date: str = arrow.get().format(SHORT_DATE_FORMAT)

    # First call get everything until now
    all_status_until_now = list_no_dupes(rawquery(query, start_date, end_date, hydrate=hydrate, kafka=kafka))
    yield all_status_until_now

    while True:
        cur_date = arrow.get().format(SHORT_DATE_FORMAT)
        try:
            cur_statuses = rawquery(query, cur_date, cur_date, hydrate=hydrate, kafka=kafka)
            cur_new_statuses =  not_in_list(all_status_until_now, cur_statuses)
            logger.info(f"💬 Found: {len(cur_statuses)}")
            all_status_until_now += cur_new_statuses

            yield cur_new_statuses
        except KeyboardInterrupt:
            raise
        except:
            logger.warning(f"⚠️ Failing running query, will be a next iteration")


def user_activity(user:str, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    logger.info(f"💬 Retrieving activiy {user}")
    results = rawquery(f"from:{user} OR to:{user} OR on:{user}", start_date, end_date)
    logger.info(f"💬 Retrieved activiy {user} statuses: {len(results)}")
    return results


def iter_user_activity(user: str, end_date: str = arrow.get().shift(days=-1).format(SHORT_DATE_FORMAT)):
    logger.info(f"💬 Retrieving activiy {user}")
    return iter_rawquery(f"from:{user} OR to:{user} OR on:{user}", end_date)



def user_by_id(user:str, max_id: int = 0):
    logger.info(f"💬 Retrieving info {user}")
    results = _get_user_statuses(user, max_id)
    logger.info(f"💬 Retrieved {user} statuses: {len(results)}")
    return results

def user_by_query(user:str, max_id: int = 0):
    logger.info(f"💬 Retrieving activiy {user}")
    results = rawquery(f"from:{user}", arrow.get().format(SHORT_DATE_FORMAT), end_date =arrow.get().shift(months=-15))
    logger.info(f"💬 Retrieved activiy {user} statuses: {len(results)}")
    return results

def hashtag(hashtag, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

    query_hashtag = hashtag.replace("#","")

    logger.info(f"Querying for {query_hashtag}")

    results = rawquery(query_hashtag, start_date, end_date)
    logger.info(f"💬 Retrieved hashtag {query_hashtag} statuses: {len(results)}")

def hashtags(hashtags, type_operator = operator.or_, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):

    if type_operator == operator.or_:

        query_hashtag = " OR ".join(hashtags)

    elif type_operator == operator.and_:

        query_hashtag = " AND ".join(hashtags)

    logger.info(f"Querying for '{query_hashtag}'")

    results = rawquery(query_hashtag, start_date, end_date)
    logger.info(f"💬 Retrieved hashtag {query_hashtag} statuses: {len(results)}")

def geolocation(lat,lon, radius, start_date:str=arrow.get().format(SHORT_DATE_FORMAT), end_date:str=arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    logger.info(f"🗺 Retrieving for [LAT:{lat}, LON:{lon},  Radious:{radius}]")
    results = rawquery(f"geocode:{lat},{lon},{lon}", start_date, end_date)
    logger.info(f"🗺💬 Retrieved for locaton [LAT:{lat}, LON:{lon},  Radious:{radius}] statuses : {len(results)}")
    return results
