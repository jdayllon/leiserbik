import copy
import json
import time
import urllib

import arrow
from arrow import Arrow
from bs4 import BeautifulSoup, Tag
from ratelimit import limits, sleep_and_retry
from requests import Session
from scalpl import Cut

from leiserbik import *
from leiserbik.borg import Kakfa
from leiserbik.twitter.query import TwitterQueryStatus


def not_in_list(l1, l2):
    if l1 is None:
        l1 = []
    if l2 is None:
        l2 = []

    if l1 == [] and l2 != []:
        return list_no_dupes(l2)
    elif l1 != [] and l2 == []:
        return list_no_dupes(l1)
    elif l1 == [] and l2 == []:
        return []
    else:
        # return list(set(l2) - set(l1))
        return union_lists_no_dupes(l2, l1)


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


def _session_get_requests(url: str):
    return __session_get_request(requests.Session(), url)


def __session_get_request(session: Session, url: str):
    # session.headers.update({'User-Agent': GENERATED_USER_AGENT})

    if 'HTTPS_PROXY' in globals():
        session.proxies = {"http": '127.0.0.1:5566', "https": '127.0.0.1:5566'}
        return session.get(url)
    else:
        session.proxies = {"http": None, "https": None}
        return __session_get_rated_requests(**locals())


@sleep_and_retry
@limits(calls=50, period=60)
def __session_get_rated_requests(session: Session, url: str):
    logger.trace(f"👮‍Rate limited GET request: {url}")
    try:
        response = session.get(url)
        return response
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail on GET request - Retry on 30s: {url}")
        time.sleep(10)
        return session.get(url)


def __session_post_request(session: Session, url):
    session.headers.update({'User-Agent': GENERATED_USER_AGENT})

    if 'HTTPS_PROXY' in globals():
        session.proxies = {"http": ROTATE_HTTP_PROXY, "https": ROTATE_HTTPS_PROXY}
        return session.get(url)
    else:
        session.proxies = {"http": None, "https": None}
        return __session_get_rated_requests(**locals())


@sleep_and_retry
@limits(calls=50, period=60)
def __session_post_rated_requests(session: Session, url: str):
    logger.trace(f"👮‍Rate limited POST request: {url}")
    return session.post(url)


def __get_statuses(decoded_content):
    # return [f"https://mobile.twitter.com{path}" for path in REGEX_STATUS_LINK.findall(decoded_content)]

    def _get_base_status(id: int):
        status = Cut()
        status['@data_source'] = 'https://mobile.twitter.com'
        status['id'] = id
        status['id_str'] = str(id)
        status['@updated_at'] = arrow.utcnow().format(LONG_DATETIME_PATTERN) + "Z"

        return status.data

    statuses = []
    statuses_links = list_no_dupes(REGEX_STATUS_LINK_VALUES.findall(decoded_content))
    for x in statuses_links:
        try:
            if type(x) is list:
                for y in x:
                    statuses += [_get_base_status(int(y))]
            else:
                statuses += [_get_base_status(int(x))]
        except KeyboardInterrupt:
            raise
        except:
            logger.warning(f"⚠️ Converting to integer: {x}")

    return statuses


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


def __generate_search_url_by_range(query: str, init_date: Arrow, finish_date: str = Arrow):
    urls = []
    cur_date = init_date

    while cur_date >= finish_date:
        cur_url = __generate_search_url_by_day(query, cur_date)
        urls += [cur_url]
        cur_date = cur_date.shift(days=-1)

    return urls


def _get_page_branches(content):
    def get_query_from_content(decode_content):
        results = REGEX_GET_QUERY.findall(decode_content)
        if len(results) == 1:
            return results[0]
        else:
            return []

    # twqstatus = TwitterQueryStatus()

    try:
        cur_decoded_content = content.decode('utf-8')
        session = requests.Session()
    except KeyboardInterrupt:
        raise
    except:
        return []

    data = []
    branches = 1
    query_from_content = get_query_from_content(cur_decoded_content)

    while True:

        # cur_statuses = __get_statuses(cur_decoded_content)
        # new_statuses = not_in_list(twqstatus.get(query_from_content), cur_statuses)

        data += [(cur_decoded_content, copy.deepcopy(session), branches, query_from_content)]

        # cur_decoded_content  = get_next_branch(cur_decoded_content , session)
        cur_decoded_content = __get_next_page(cur_decoded_content, session, REGEX_UPDATE_LINK)

        if cur_decoded_content is None:
            break
        else:
            branches += 1
            logger.debug(f"New Branch |{query_from_content}|, total branches ⌥: {branches}")

    return data


def _get_user_statuses(user, max_id=0):
    session = requests.Session()
    branch = 0
    query_from_content = user
    if max_id > 0:
        res = __session_get_request(session, f"https://mobile.twitter.com/{user}?max_id={max_id}")
    else:
        res = __session_get_request(session, f"https://mobile.twitter.com/{user}")
    # res = session.get(f"https://mobile.twitter.com/{user}")
    statuses = []

    logger.debug(f"Requests: {res.url} |{res.status_code}|")
    if res.status_code == 200:
        cur_content = res.content.decode('utf-8')
        # logger.info(cur_content)
    else:
        return statuses

    # Do while emulation
    while True:
        cur_statuses = __get_statuses(cur_content)

        if len(cur_statuses) == 0:
            logger.debug(f"Statuses 💬 not Found 😅 |{user}|")
            # nojs_post_url =REGEX_NOJS_ROUTER.findall(cur_content)[0].split('"')[0]
            # logger.debug(f"POST Requests detected: {nojs_post_url}")

            # cur_content = __session_post_request(session,nojs_post_url)

            # if cur_content is None and type(cur_content) is bytes:
            #    cur_content = cur_content.decode('utf-8')
            #    logger.info(cur_content)
            #    cur_statuses_check = __get_statuses(cur_content)

            #    if len(cur_statuses_check) == 0:
            #        return statuses

            return statuses

        else:
            statuses = list(set(cur_statuses + statuses))
            logger.debug(f"Current content statuses found: {len(statuses)} 💬 |{user}|")

            cur_content = __get_next_page(cur_content, session, REGEX_USER_NEXT_LINK)

        if cur_content is None:
            return statuses


def _get_branch_walk(params):
    decoded_content = params[0]
    session = params[1]
    branch = params[2]
    query_from_content = params[3]

    contents = []
    cur_content = decoded_content

    twqstatus = TwitterQueryStatus()

    # Do while emulation
    while True:
        contents += [cur_content]
        cur_statuses = __get_statuses(cur_content)

        try:
            new_statuses = not_in_list(twqstatus.get(query_from_content), cur_statuses)
        # except KeyboardInterrupt:
        #    raise               
        except:
            new_statuses = []
            logger.exception("🚨Error on content parameters, probably partial page download")

        if len(cur_statuses) == 0:
            logger.debug(f"💬 No more statuses found 😅 |{query_from_content} -- Branch: {branch}|")
            return contents
        elif len(new_statuses) == 0:
            logger.debug(f"💬 No new statuses found 😅 |{query_from_content} -- Branch: {branch}|")
            return contents
        else:

            logger.info(f"💬 {len(new_statuses)} new statuses found 👍 |{query_from_content} -- Branch: {branch}|")
            twqstatus.append(query_from_content, cur_statuses)

            cur_content = __get_next_page(cur_content, session, REGEX_NEXT_LINK)

            if cur_content is None:
                return contents


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
            logger.debug(
                f"Current content statuses found: {len(statuses)} 💬 |{query_from_content} -- Branch: {branch}|")

        cur_content = __get_next_page(cur_content, session, REGEX_NEXT_LINK)

        if cur_content is None:
            return statuses


def _get_status(id: int, session: Session = requests.Session()):
    res = __session_get_request(session, f"https://mobile.twitter.com/twitter/status/{id}")
    if res.status_code == 200:
        return _read_statuses(res.content.decode('utf-8'))
    elif res.status_code == 429:
        time.sleep(10)
        return _get_status(id, session)
    else:
        return None


def _read_statuses(content: str):
    statuses_data = []

    soup = BeautifulSoup(content, "html.parser")
    statuses = soup.find_all('table', {"class": "tweet"})

    if len(statuses) == 0:
        statuses = soup.find_all('div', {"class": "js-tweet-text-container"})

    for cur_tweet in statuses:
        cur_statuses_data = __read_status(cur_tweet)
        statuses_data += [cur_statuses_data]

    return statuses_data


def _update_status_stats(status: dict):
    """Get from desktop web version stats about current status (RT and FAVs) 
    
    Arguments:
        status {dict} -- Current status in scalpl object
    
    Returns:
        dict -- Updated stats status
    """

    try:
        # ! TODO Implement a better version in parallalel or async
        cur_retweets, cur_favs = __update_status_stats(status['id'])
        status['retweet_count'] = cur_retweets
        status['favorite_count'] = cur_favs
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting RT and Favs from 🐦: {status['id']}")

    return status


def __update_status_stats(id: int, session: Session = requests.Session()):
    res = __session_get_request(session, f"https://twitter.com/twitter/status/{id}")

    # Getting web standart version
    if res.status_code == 200:

        soup = BeautifulSoup(res.content.decode('utf-8'), "html.parser")

        retweet_ele = soup.find('li', {"class": "js-stat-retweets"})
        fav_ele = soup.find('li', {"class": "js-stat-favorites"})

        if retweet_ele is not None:
            cur_retweets = int(retweet_ele.find('a').find('strong').get_text())
        else:
            cur_retweets = 0

        if fav_ele is not None:
            cur_favorites = fav_ele.find('a').find('strong').get_text()
        else:
            cur_favorites = 0

        logger.info(f"🔁 Retweets for {id} : {cur_retweets}")
        logger.info(f"❤️  Favs for {id} : {cur_favorites}")

        return cur_retweets, cur_favorites

    elif res.status_code == 429:
        time.sleep(10)
        return __update_status_stats(id, session)
    else:
        return None


def __read_status(soup):
    status = Cut()

    if 'tombstone-tweet' in soup['class']:
        # Dead twitter account reference
        return status

    cur_tweet_data = soup.find('div', {"class": "tweet-text"})

    status['@data_source'] = 'https://mobile.twitter.com'
    status['id'] = int(cur_tweet_data['data-id'])
    status['id_str'] = str(status['id'])
    status['@updated_at'] = arrow.utcnow().format(LONG_DATETIME_PATTERN) + "Z"

    status['user'] = {}
    try:
        status['user.screen_name'] = soup.find('div', {"class": "username"}).get_text().replace('\n', '').strip()[
                                     1:]  # Omits @
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting screen_name from 🐦: {status['id']}")
        status['user.screen_name'] = soup['href'].split('/')[1]

    try:
        status['user.name'] = soup.find('strong', {"class": "fullname"}).get_text()
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting fullname from 🐦: {status['id']}")

    try:
        cur_tweet_mentions = soup.find('a', {"class": "twitter-atreply"})
        status['user_mentions'] = []
        if cur_tweet_mentions is not None:

            if type(cur_tweet_mentions) is Tag:
                cur_tweet_mentions = [cur_tweet_mentions]

            for cur_mention in cur_tweet_mentions:
                # Example info
                # {
                #       "id": 3001809246,
                #       "id_str": "3001809246",
                #       "name": "Rafael Moreno Rojas",
                #       "screen_name": "rafamorenorojas"
                #   },
                #
                status['user_mentions'] += [{
                    'id': cur_mention['data-mentioned-user-id'],
                    'id_str': str(cur_mention['data-mentioned-user-id']),
                    'screen_name': cur_mention.get_text()[1:]  # Omit @
                }]
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting user_mentions from 🐦: {status['id']}")

    try:

        cur_tweet_text = cur_tweet_data.find('div', {"class": "dir-ltr"})
        if cur_tweet_text is None:
            cur_tweet_text = cur_tweet_data.get_text().lstrip()
        else:
            cur_tweet_text = cur_tweet_text.get_text().lstrip()

        status['full_text'] = cur_tweet_text
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting full_text from 🐦: {status['id']}")

    try:
        cur_tweet_date = soup.find('td', {"class": "timestamp"}).find('a').get_text()

        if "h" in cur_tweet_date and len(cur_tweet_date) < 4:
            hours = int(re.findall("([0-9]{0,2})\s?h", cur_tweet_date)[0])
            cur_tweet_date = arrow.get().shift(hours=-hours).format(LONG_DATETIME_PATTERN)
        elif "m" in cur_tweet_date and len(cur_tweet_date) < 4:
            minutes = int(re.findall("([0-9]{0,2})\s?m", cur_tweet_date)[0])
            cur_tweet_date = arrow.get().shift(minutes=-minutes).format(LONG_DATETIME_PATTERN)
        elif "s" in cur_tweet_date and len(cur_tweet_date) < 4:
            hours = int(re.findall("([0-9]{0,2})\s?s", cur_tweet_date)[0])
            cur_tweet_date = arrow.get().shift(hours=-hours).format(LONG_DATETIME_PATTERN)
        elif len(cur_tweet_date) < 9:
            # On current year tweets doesn't show a year in text
            cur_tweet_date += arrow.get().format(" YY")
            cur_tweet_date = arrow.get(cur_tweet_date, "MMM D YY").format(LONG_DATETIME_PATTERN)
        else:
            cur_tweet_date = arrow.get(cur_tweet_date, "D MMM YY").format(LONG_DATETIME_PATTERN)

        status['created_at'] = cur_tweet_date.format(LONG_DATETIME_PATTERN) + "Z"
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting created_at from 🐦: {status['id']}")

    try:
        cur_tweet_hashtags = soup.find('a', {"class": "twitter-hashtag"})
        status['hashtags'] = []

        if cur_tweet_hashtags is not None:

            if type(cur_tweet_hashtags) is Tag:
                cur_tweet_hashtags = [cur_tweet_hashtags]

            for cur_hashtag in cur_tweet_hashtags:
                # "hashtags": [
                #    {
                #    "text": "Gastronom\u00eda"
                #    },
                #    {
                #    "text": "Andaluc\u00eda"
                #    }
                # ],
                status['hashtags'] += [{
                    'text': cur_hashtag.get_text()[1:]  # Omits '#'
                }]
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting hashtags from 🐦: {status['id']}")

    try:
        cur_tweet_urls = soup.find('a', {"class": "twitter_external_link"})
        status['urls'] = []

        if cur_tweet_urls is not None:

            if type(cur_tweet_urls) is Tag:
                cur_tweet_urls = [cur_tweet_urls]

            for cur_url in cur_tweet_urls:
                #    "urls": [
                #               {
                #                   "expanded_url": "https://sevilla.abc.es/gurme//reportajes-bares-y-restaurantes-cordoba/cordoba/rafael-moreno-rojas-director-la-catedra-gastronomia-andalucia-objetivo-darnos-conocer-cordoba/",
                #                   "url": "https://t.co/5Qiiv6KR9w"
                #               }
                #             ],
                status['urls'] += [{
                    'url': cur_url['href'],
                    'expanded_url': cur_url['data-expanded-url'] if 'data-expanded-url' in cur_url else None,
                }]
    except KeyboardInterrupt:
        raise
    except:
        logger.warning(f"🚨 Fail getting external urls from 🐦: {status['id']}")

        # return status.data
    # return json.dumps(status.data, indent=4)
    return status.data


def _send_kafka(cur_dict: dict, topic=None):
    logger.info("Going to send kafka")

    kafka = Kakfa()

    cur_json = json.dumps(cur_dict, indent=4)

    try:
        if topic is None:
            logger.debug(f"📧 Sending to Kafka [{kafka.topic}]: {cur_json} - {cur_dict['id_str']}")
            future_requests = kafka.producer.send(kafka.topic, f'{cur_json}'.encode(), key=cur_dict['id_str'].encode())
        else:
            logger.debug(f"📧 Sending to Kafka [{topic}]: {cur_json} - {cur_dict['id_str']}")
            future_requests = kafka.producer.send(topic, f'{cur_json}'.encode(), key=cur_dict['id_str'].encode())

        future_response = future_requests.get(timeout=10)
    except:
        logger.exception(f"🚨 Error sending to Kafka: {cur_dict['id_str']}")

    return cur_dict
