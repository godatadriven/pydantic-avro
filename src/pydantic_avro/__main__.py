import argparse
import sys
from typing import List

from pydantic_avro.avro_to_pydantic import convert_file


def main(input_args: List[str]):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="sub_command", required=True)

    parser_cache = subparsers.add_parser("avro_to_pydantic")
    parser_cache.add_argument("--asvc", type=str, dest="avsc", required=True)
    parser_cache.add_argument("--output", type=str, dest="output")

    args = parser.parse_args(input_args)

    if args.sub_command == "avro_to_pydantic":
        convert_file(args.avsc, args.output)


def root_main():
    main(sys.argv[1:])


if __name__ == "__main__":
    root_main()
