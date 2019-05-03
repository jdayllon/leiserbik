# -*- coding: utf-8 -*-

"""Console script for leiserbik."""
import pprint
import sys

import click
from slugify import slugify

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

    global WORK_DIR

    if query is None and len(ctx.args) == 1:
        query = ctx.args[0]

    filename = None

    if ctx.obj['WRITE'] and ctx.obj['STREAM']:
        operation = capturer
        filename = f"{WORK_DIR}{arrow.get().format(LONG_DATETIME_PATTERN)}-STREAM-{slugify(query)}.lst"
    elif ctx.obj['WRITE'] and not ctx.obj['STREAM']:
        operation = capturer
        filename = f"{WORK_DIR}{arrow.get().format(LONG_DATETIME_PATTERN)}-STATIC-{slugify(query)}.lst"
    else:
        operation = watcher

    if ctx.obj['STREAM']:
        counter = 0

        if filename is not None:
            logger.info(f"💾 Opening {filename} for streaming output")
            with open(filename, 'w') as f:
                for cur_statuses in operation.iter_rawquery(query, end_date=end_date):
                    logger.info(f"🚚 Iteration: {counter} | Elements {len(cur_statuses)}")
                    for cur_status in cur_statuses:
                        print(cur_status)
                        sys.stdout.flush()
                        f.write(f"{cur_status}\n")
                        f.flush()
                    counter += 1
        else:
            for cur_statuses in operation.iter_rawquery(query, end_date=end_date):
                logger.info(f"🚚 Iteration: {counter} | Elements {len(cur_statuses)}")
                for cur_status in cur_statuses:
                    cprint(cur_status)
                    sys.stdout.flush()
                counter += 1

    else:

        if filename is not None:
            logger.info(f"💾 Opening {filename}")
            with open(filename, 'w') as f:
                cur_statuses = watcher.rawquery(query)
                with open(filename, 'w') as f:
                    for cur_status in cur_statuses:
                        cprint(cur_status)
                        sys.stdout.flush()
                        f.write(f"{cur_status}\n")
                        f.flush()
            logger.info(f"💾 Closing {filename}")

        else:
            cur_statuses = watcher.rawquery(query)
            for cur_status in cur_statuses:
                cprint(cur_status)
                sys.stdout.flush()

    #cprint(dir(ctx.args))
    #cprint(ctx.args)

    return 0

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
