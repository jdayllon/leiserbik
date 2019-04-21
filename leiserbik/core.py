from leiserbik import *
import urllib
from arrow import Arrow
import requests
from requests import Session
import copy
from ratelimit import limits, sleep_and_retry
import time

def __generate_search_url_by_day(query: str, date: Arrow):
    """
    Returns a string with a url to ask twitter for a query in a day
    :param query:str twitter advanced query string
    :param date: date to query
    :return: url for date
    """

    search_url = '%s since:%s until:%s' % (query, date.format('YYYY-MM-DD'), date.shift(days=1).format('YYYY-MM-DD'))
    search_url = 'https://mobile.twitter.com/search?q=' + urllib.parse.quote_plus(search_url)
    logger.debug(f"Generated url: {search_url}")
    return search_url


def __session_get_request(session:Session, url:str):
    session.headers.update({'User-Agent': GENERATED_USER_AGENT})

    if 'HTTPS_PROXY' in globals():
        session.proxies = {"http": '127.0.0.1:5566', "https": '127.0.0.1:5566'}
        return session.get(url)
    else:
        session.proxies = {"http": None, "https": None}
        return __session_get_rated_requests(**locals())


@sleep_and_retry
@limits(calls=50, period=60)
def __session_get_rated_requests(session:Session, url:str):
    logger.trace(f"👮‍Rate limited GET request: {url}")
    return session.get(url)

def __session_post_request(session:Session, url):
    session.headers.update({'User-Agent': GENERATED_USER_AGENT})

    if 'HTTPS_PROXY' in globals():
        session.proxies = {"http": '127.0.0.1:5566', "https": '127.0.0.1:5566'}
        return session.get(url)
    else:
        session.proxies = {"http": None, "https": None}
        return __session_get_rated_requests(**locals())

@sleep_and_retry
@limits(calls=50, period=60)
def __session_post_rated_requests(session:Session, url:str):
    logger.trace(f"👮‍Rate limited POST request: {url}")
    return session.post(url)


def __get_statuses(decoded_content):
    #return [f"https://mobile.twitter.com{path}" for path in REGEX_STATUS_LINK.findall(decoded_content)]
    return REGEX_STATUS_LINK_VALUES.findall(decoded_content)

def __get_next_page(decoded_content, session, REGEX_COMPILED_PATTERN):
    next_pages = [f"https://mobile.twitter.com{path}" for path in REGEX_COMPILED_PATTERN.findall(decoded_content)]
    if len(next_pages) == 1:
        logger.debug(f"Requesting: {next_pages[0]}")
        res = __session_get_request(session, next_pages[0])
        logger.debug(f"Request: {next_pages[0]} |{res.status_code}|")
        if res.status_code == 200:
            return res.content.decode('utf-8')
        elif res.status_code == 429:
            logger.warning(f"Request Rate Limit Exception: {next_pages[0]}")
            time.sleep(30)
            return __get_next_page(decoded_content, session, REGEX_COMPILED_PATTERN)
        else:
            return None
    return None

def __generate_search_url_by_range(query: str, init_date:Arrow, finish_date:str=Arrow):
    urls = []
    cur_date = init_date

    while cur_date >= finish_date:
        cur_url = __generate_search_url_by_day(query, cur_date)
        urls += [cur_url]
        cur_date = cur_date.shift(days=-1)

    return urls

def  _get_page_branches(content):

    def get_query_from_content(decode_content):
        results =REGEX_GET_QUERY.findall(decode_content)
        if len(results) == 1:
            return results[0]
        else:
            return []

    try:
        cur_decoded_content = content.decode('utf-8')
        session  = requests.Session()
    except:
        return []

    data = []
    branches = 1
    query_from_content = get_query_from_content(cur_decoded_content)

    while True:

        data += [(cur_decoded_content , copy.deepcopy(session), branches, query_from_content)]

        #cur_decoded_content  = get_next_branch(cur_decoded_content , session)
        cur_decoded_content = __get_next_page(cur_decoded_content, session, REGEX_UPDATE_LINK)

        if cur_decoded_content is None:
            break
        else:
            branches += 1
            logger.debug(f"New Branch |{query_from_content}|, total branches ⌥: {branches}")

    return data

def _get_user_statuses(user):

    session = requests.Session()
    branch = 0
    query_from_content = user
    res = __session_get_request(session, f"https://mobile.twitter.com/{user}")
    #res = session.get(f"https://mobile.twitter.com/{user}")
    statuses = []

    logger.debug(f"Requests: {res.url} |{res.status_code}|")
    if res.status_code == 200:
        cur_content = res.content.decode('utf-8')
    else:
        return statuses

    # Do while emulation
    while True:
        cur_statuses = __get_statuses(cur_content)

        if len(cur_statuses) == 0:
            logger.debug(f"Statuses 💬 not Found 😅 |{user}|")
            nojs_post_url =REGEX_NOJS_ROUTER.findall(cur_content)[0]
            logger.debug(f"POST Requests detected: {nojs_post_url}")

            cur_content = __session_post_request(session,nojs_post_url)

            if cur_content is None and type(cur_content) is bytes:
                cur_content = cur_content.decode('utf-8')
                logger.info(cur_content)
                cur_statuses_check = __get_statuses(cur_content)

                if len(cur_statuses_check) == 0:
                    return statuses

        else:
            statuses = list(set(cur_statuses + statuses))
            logger.debug(f"Current content statuses found: {len(statuses)} 💬 |{user}|")

            cur_content = __get_next_page(cur_content, session, REGEX_USER_NEXT_LINK)

        if cur_content is None:
            return statuses


def _get_branch_statuses(params):

    decoded_content = params[0]
    session = params[1]
    branch = params[2]
    query_from_content = params[3]

    statuses = []
    cur_content = decoded_content

    # Do while emulation
    while True:
        cur_statuses = __get_statuses(cur_content)

        if len(cur_statuses) == 0:
            logger.debug(f"Statuses 💬 not Found 😅 |{query_from_content} -- Branch: {branch}|")
            return statuses
        else:
            statuses = list(set(cur_statuses + statuses))
            logger.debug(f"Current content statuses found: {len(statuses)} 💬 |{query_from_content} -- Branch: {branch}|")

        cur_content = __get_next_page(cur_content, session, REGEX_NEXT_LINK)

        if cur_content is None:
            return statuses



