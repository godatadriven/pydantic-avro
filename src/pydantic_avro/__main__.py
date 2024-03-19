import argparse
import sys
from typing import List


from pydantic_avro import avro_to_graphql
from pydantic_avro import avro_to_pydantic


def main(input_args: List[str]):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="sub_command", required=True)

    parser_cache = subparsers.add_parser("avro_to_pydantic")
    parser_cache.add_argument("--asvc", type=str, dest="avsc", required=True)
    parser_cache.add_argument("--output", type=str, dest="output")

    parser_cache = subparsers.add_parser("avro_to_graphql")
    parser_cache.add_argument("--asvc", type=str, dest="avsc", required=True)
    parser_cache.add_argument("--output", type=str, dest="output")
    parser_cache.add_argument("--config", type=str, dest="config")

    parser_cache = subparsers.add_parser("avro_folder_to_graphql")
    parser_cache.add_argument("--asvc", type=str, dest="avsc", required=True)
    parser_cache.add_argument("--output", type=str, dest="output")
    parser_cache.add_argument("--config", type=str, dest="config")

    args = parser.parse_args(input_args)

    if args.sub_command == "avro_to_pydantic":
        avro_to_pydantic.convert_file(args.avsc, args.output)

    if args.sub_command == "avro_to_graphql":
        avro_to_graphql.convert_file(args.avsc, args.output, args.config)

    if args.sub_command == "avro_folder_to_graphql":
        avro_to_graphql.convert_files(args.avsc, args.output, args.config)


def root_main():
    main(sys.argv[1:])


if __name__ == "__main__":
    root_main()
