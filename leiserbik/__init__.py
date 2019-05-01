# -*- coding: utf-8 -*-

"""Top-level package for leiserbik."""

__author__ = """Juan David Ayllon Burguillo"""
__email__ = 'jdayllon@gmail.com'
__version__ = '0.1.0'


import os
import re

import requests
from fake_useragent import UserAgent
from loguru import logger

SHORT_DATE_FORMAT = 'YYYY-MM-DD'
LONG_DATETIME_PATTERN = "YYYY-MM-DDTHH:mm:ss"


REGEX_UPDATE_LINK = re.compile(r'(\/search\?q=.*\;prev_cursor=.*)"')
REGEX_NEXT_LINK = re.compile(r'(\/search\?q=.*\;next_cursor=.*)"')
REGEX_STATUS_LINK = re.compile(r'\/.*\/status\/\d*')
REGEX_STATUS_LINK_VALUES = re.compile(r'\/.*\/status\/(\d*)')
REGEX_GET_QUERY = re.compile(r'id=\"q\"\sname=\"q\"\stype=\"text\"\svalue=\"(.*)\"')
REGEX_USER_NEXT_LINK = re.compile(r'<a href=\"(\/.*\?max_id=\d*)\"')
REGEX_NOJS_ROUTER = re.compile(r'\"(https:\/\/mobile\.twitter\.com\/i\/nojs_router\?path=.*)\"')


GENERATED_USER_AGENT = UserAgent().chrome
logger.debug(f"Using UserAngent {GENERATED_USER_AGENT} 🕵‍")

MAX_WORKERS = 3

## Rotating Proxy
if "HTTPS_PROXY" in os.environ and "HTTP_PROXY" in os.environ:
    logger.info("Proxy envs dectected 🚇")
    HTTPS_PROXY = os.environ["HTTPS_PROXY"]
    HTTP_PROXY = os.environ["HTTP_PROXY"]

    proxies = {
        'http': f'http://{HTTP_PROXY}',
        'https': f'http://{HTTPS_PROXY}',
    }

    empty_proxies = {
        "http": None,
        "https": None,
    }

    try:

        res_wo_proxy_content = requests.get("https://api.ipify.org?format=text", proxies=empty_proxies).content.decode('utf-8')
        logger.info(f"🌐 Ip without proxy = {res_wo_proxy_content}")
        res_proxy = requests.get("https://api.ipify.org?format=text", proxies=proxies)


        if res_proxy.status_code == 200:
            res_proxy_content = res_proxy.content.decode('utf-8')
            logger.info(f"🌐🚇 Ip with proxy = {res_proxy_content}")
            if res_proxy_content != res_wo_proxy_content:
                logger.info("Proxy checked 👍")

                if "MAX_WORKERS" in os.environ:
                    MAX_WORKERS = int(os.environ["MAX_WORKERS"])

            else:
                logger.warning("Proxy fail 🚨")
                HTTP_PROXY = None
                HTTPS_PROXY = None
        else:
            logger.warning("Proxy fail 🚨")
            HTTP_PROXY = None
            HTTPS_PROXY = None
    except:
        logger.warning("Proxy fail 🚨")
        HTTP_PROXY = None
        HTTPS_PROXY = None


def list_no_dupes(l):
    return list(set(l))


def union_lists_no_dupes(l1, l2):
    return list(set(l2) + set(l1))
