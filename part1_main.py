##
# @file part1_main.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 11/2021
# Downloads data and imports them to DB

import argparse
import sys

from part1.download import download_data
from part1.db import DBC

def main() -> None:
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-H', '--host', dest='host', help='DB server host', default=DBC.DEFAULT_HOST, type=str, required=False)
    argparser.add_argument('-p', '--port', dest='port', help='DB server port number', default=DBC.DEFAULT_PORT, type=int, required=False)
    argparser.add_argument('-t', '--timeout', dest='timeout', help='timeout for server connection', default=None, type=int, required=False)

    args, _ = argparser.parse_known_args(sys.argv)

    download_data(rewrite=True)

    dbc = DBC(args.host, args.port, args.timeout)
    dbc.create_all_collections()

if __name__ == '__main__':
    main()
