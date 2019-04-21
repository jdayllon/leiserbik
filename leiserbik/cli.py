# -*- coding: utf-8 -*-

"""Console script for leiserbik."""
import sys
import click
from leiserbik import *
from leiserbik import watcher
import pprint

cprint = pprint.PrettyPrinter(indent=4).pprint

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def main(ctx, debug):
    if not debug:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug


#@click.pass_context
#def cli(ctx, debug):
#    pass

@main.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
@click.option('-s','--screen_name', help='Twitter Screen Name.')
def user(ctx, screen_name):
    """Retrieves info about user. If no option is given string after command is identified with screen name requiered"""

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

    if screen_name is None and len(ctx.args) == 1:
        screen_name = ctx.args[0]

    res = watcher.user_by_query(screen_name)

    cprint(res)

    return 0

@main.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
def query(ctx):
    """Console script for leiserbik."""

    cprint(dir(ctx.args))
    cprint(ctx.args)

    return 0

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
