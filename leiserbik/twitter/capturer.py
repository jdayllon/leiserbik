# -*- coding: utf-8 -*-

import arrow
from pypeln import asyncio_task as aio
from pypeln import thread as th

from leiserbik import *
from leiserbik.async_http import fetch_all
from leiserbik.twitter.core import __generate_search_url_by_range, \
    _get_page_branches, list_no_dupes, not_in_list, \
    _read_statuses, _get_branch_walk
from leiserbik.twitter.query import TwitterQuery


def query(tq: TwitterQuery):
    cur_query = tq.query(with_dates=False)
    logger.debug(f"Obtainer Twitter Query Object with query 🔎 {cur_query}")
    return rawquery(cur_query, tq.start_date, tq.end_date)


def iter_query(tq: TwitterQuery):
    cur_query = tq.query(with_dates=False)
    logger.debug(f"Obtainer Twitter Query Object with query 🔎 {cur_query}")
    return iter_rawquery(cur_query, tq.end_date)


def rawquery(query: str,
             start_date: str = arrow.get().format(SHORT_DATE_FORMAT),
             end_date: str = arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
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
    stage_results = th.flat_map(_read_statuses, stage_results, workers=MAX_WORKERS)

    # results = list_no_dupes(stage_results)
    results = list(stage_results)

    logger.info(f"💬 Captured {len(results)}")

    return results


def iter_rawquery(query: str, end_date: str = arrow.get().shift(days=-15).format(SHORT_DATE_FORMAT)):
    # if we are iterating, start_date is "now"
    start_date: str = arrow.get().format(SHORT_DATE_FORMAT)

    # First call get everything until now
    all_status_until_now = list_no_dupes(rawquery(query, start_date, end_date))
    yield all_status_until_now

    while True:
        cur_date = arrow.get().format(SHORT_DATE_FORMAT)
        cur_statuses = rawquery(query, cur_date, cur_date)
        cur_new_statuses = not_in_list(all_status_until_now, cur_statuses)
        logger.info(f"Found: {len(cur_statuses)} 💬")
        all_status_until_now += cur_new_statuses

        yield cur_new_statuses
