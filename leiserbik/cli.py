# -*- coding: utf-8 -*-

"""Console script for leiserbik."""
import pprint
import sys

import click
from slugify import slugify

from leiserbik import *
from leiserbik import watcher, capturer
import arrow

#from kafka import KafkaProducer
from leiserbik.borg import Kakfa

cprint = pprint.PrettyPrinter(indent=4).pprint

@click.group()
@click.option('-v', '--verbose', count=True)
@click.option('-h', '--hydrate', count=True)
@click.option('-u', '--update', count=True)
@click.option('-s/-ns', '--stream/--no-stream', default=False)
@click.option('-w/-nw', '--write/--no-write', default=False)
@click.option('-k/-nk', '--kafka/--no-kafka', default=False)
@click.pass_context
def main(ctx, verbose, stream, write, kafka, hydrate, update):

    if verbose == 0:
        logger.info("🕵 ‍Logger level: WARNING")
        logger.remove()
        logger.add(sys.stderr, level="WARNING")
    elif verbose == 1:
        logger.info("🕵 ‍Logger level: INFO")
        logger.remove()
        logger.add(sys.stderr, level="INFO")
    elif verbose == 2:
        logger.info("🕵 ‍Logger level: DEBUG")
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.info("🕵 ‍Logger level: TRACE")
        logger.remove()
        logger.add(sys.stderr, level="TRACE")

    if hydrate == 0:
        logger.info("🌞 Only capture status id")
    elif hydrate == 1:
        logger.info("💧 Scrapping status mobile-web based data")
    elif hydrate == 2:
        logger.info("💧 Scrapping status web based data")
    else:
        logger.info("🌊 Status info from 🐦 API")
        raise NotImplementedError


    ctx.ensure_object(dict)

    ctx.obj['STREAM'] = stream
    ctx.obj['WRITE'] = write
    ctx.obj['KAFKA'] = kafka
    ctx.obj['HYDRATE'] = hydrate

@main.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
@click.option('-s','--screen_name', help='Twitter Screen Name.')
def user(ctx, screen_name):
    """Retrieves info about user. If no option is given string after command is identified with screen name requiered"""

    logger.info("Running User Extraction")

    if screen_name is None and len(ctx.args) == 1:
        screen_name = ctx.args[0]

    res = watcher.user_by_id(screen_name)

    cprint(res)

    return 0

@main.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
@click.option('-s','--screen_name', help='Twitter Screen Name.')
def query_user(ctx, screen_name):
    """Retrieves info about user. If no option is given string after command is identified with screen name requiered"""
    logger.info("Running User Query")

    if screen_name is None and len(ctx.args) == 1:
        screen_name = ctx.args[0]

    res = watcher.user_by_query(screen_name)

    cprint(res)

    return 0

@main.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
def rawquery(ctx, query=None, end_date: str = arrow.get().shift(days=-1).format(SHORT_DATE_FORMAT)):
    """Console script for leiserbik."""

    logger.info("Running Query")

    if ctx.obj['KAFKA']:
        #kafka_producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
        kafka_instance = Kakfa('127.0.0.1:9092')
        kafka_producer = kafka_instance.producer
    else:
        kafka_producer = None

    global WORK_DIR

    if query is None and len(ctx.args) == 1:
        query = ctx.args[0]

    filename = None

    hydrate = ctx.obj['HYDRATE']
    if hydrate == 0:
        kafka_topic = LEISERBIK_TOPIC_STATUS_ID
        kafka_instance.topic = LEISERBIK_TOPIC_STATUS_ID
    elif hydrate == 1:
        kafka_topic = LEISERBIK_TOPIC_STATUS_ID_WEB
        kafka_instance.topic = LEISERBIK_TOPIC_STATUS_ID_WEB
    else:
        kafka_topic = LEISERBIK_TOPIC_STATUS_ID_WEB
        kafka_instance.topic = LEISERBIK_TOPIC_STATUS_ID_WEB
        #raise NotImplementedError

    if ctx.obj['WRITE'] and ctx.obj['STREAM']:
        operation = capturer
        filename = f"{WORK_DIR}{arrow.get().format(LONG_DATETIME_PATTERN)}-STREAM-{slugify(query)}.json"
    elif ctx.obj['WRITE'] and not ctx.obj['STREAM']:
        operation = capturer
        filename = f"{WORK_DIR}{arrow.get().format(LONG_DATETIME_PATTERN)}-STATIC-{slugify(query)}.json"
    else:
        operation = watcher

    if ctx.obj['STREAM']:
        counter = 0

        if filename is not None:
            logger.info(f"💾 Opening {filename} for streaming output")
            with open(filename, 'w') as f:
                for cur_statuses in operation.iter_rawquery(query, end_date=end_date, hydrate=hydrate):
                    logger.info(f"🚚 Iteration: {counter} | Elements {len(cur_statuses)}")
                    for cur_status in cur_statuses:
                        print(cur_status)
                        sys.stdout.flush()
                        f.write(f"{cur_status}\n")
                        f.flush()
                    counter += 1
        else:
            for cur_statuses in operation.iter_rawquery(query, end_date=end_date, hydrate=hydrate):
                logger.info(f"🚚 Iteration: {counter} | Elements {len(cur_statuses)}")

                for cur_status in cur_statuses:
                    if kafka_producer is not None:
                        pass
                        #import ipdb ; ipdb.set_trace()
                        #logger.debug(f"📧 Sending to Kafka [{kafka_topic}]: {cur_status}")
                        #future_requests = kafka_producer.send(kafka_topic, f'{cur_status}'.encode())
                        #future_response = future_requests.get(timeout=10)
                        #logger.debug(f"📧 Sended to Kafka with response {future_response}")
                    else:
                        cprint(cur_status)
                        sys.stdout.flush()

                counter += 1

    else:

        if filename is not None:
            logger.info(f"💾 Opening {filename}")
            with open(filename, 'w') as f:
                cur_statuses = watcher.rawquery(query, hydrate=hydrate)
                with open(filename, 'w') as f:
                    for cur_status in cur_statuses:
                        cprint(cur_status)
                        sys.stdout.flush()
                        f.write(f"{cur_status}\n")
                        f.flush()
            logger.info(f"💾 Closing {filename}")

        else:
            cur_statuses = watcher.rawquery(query, hydrate=hydrate)
            for cur_status in cur_statuses:
                if kafka_producer is not None:
                    pass
                    #logger.debug(f"📧 Sending to Kafka [{kafka_topic}]: {cur_status}")
                    #future_requests = kafka_producer.send(kafka_topic, f'{cur_status}'.encode())
                    #future_response = future_requests.get(timeout=10)
                    #logger.debug(f"📧 Sended to Kafka with response {future_response}")
                else:
                    cprint(cur_status)
                    sys.stdout.flush()


    # Close Kafka

    if kafka_producer is not None:
        kafka_producer.flush()
        kafka_producer.close()

    return 0

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pylint: disable= all; problems with click call
