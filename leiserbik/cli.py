# -*- coding: utf-8 -*-

"""Console script for leiserbik."""
import pprint
import sys

import click

from leiserbik import *
from leiserbik import watcher, capturer

cprint = pprint.PrettyPrinter(indent=4).pprint

@click.group()
@click.option('-d/-ns', '--debug/--no-debug', default=False)
@click.option('-s/-ns', '--stream/--no-stream', default=False)
@click.option('-w/-nw', '--write/--no-write', default=False)
@click.pass_context
def main(ctx, debug, stream, write):

    if not debug:
        logger.remove()
        logger.add(sys.stderr, level="WARNING")

    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug
    ctx.obj['STREAM'] = stream
    ctx.obj['WRITE'] = write


#@click.pass_context
#def cli(ctx, debug):
#    pass

@main.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
@click.option('-s','--screen_name', help='Twitter Screen Name.')
def user(ctx, screen_name):
    """Retrieves info about user. If no option is given string after command is identified with screen name requiered"""

    loguru.info("Running User Extraction")

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
    loguru.info("Running User Query")

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

    if query is None and len(ctx.args) == 1:
        query = ctx.args[0]

    if ctx.obj['WRITE']:
        operation = capturer
    else:
        operation = watcher

    if ctx.obj['STREAM']:
        counter = 0
        for cur_statuses in operation.iter_rawquery(query, end_date=end_date):
            logger.info(f"Iteration: {counter} | Elements {len(cur_statuses)}")
            for cur_status in cur_statuses:
                cprint(cur_status)
            counter += 1
    else:

        cur_statuses = watcher.rawquery(query)
        for cur_status in cur_statuses:
            cprint(cur_status)

    #cprint(dir(ctx.args))
    #cprint(ctx.args)

    return 0

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
