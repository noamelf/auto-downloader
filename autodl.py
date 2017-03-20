from os.path import expanduser

import click
import requests

DAEMON_ADDRESS = 'http://127.0.0.1:6736'


def request(*args, **kwargs):
    try:
        resp = requests.post(*args, **kwargs)
        resp.raise_for_status()
    except requests.ConnectionError:
        print('No response from daemon, is it running?')
    except requests.HTTPError:
        print(f"Something went wrong with the request: {resp.status_code} {resp.text}")

@click.group()
def cli():
    pass


@cli.command()
@click.argument('url')
@click.argument('dirpath', default='')
def add(url, dirpath):
    dirpath = dirpath or expanduser("~")
    request(f'{DAEMON_ADDRESS}/add', json={'url': url, 'dirpath': dirpath})


@cli.command()
def clear():
    request(f'{DAEMON_ADDRESS}/clear')


if __name__ == '__main__':
    cli()
