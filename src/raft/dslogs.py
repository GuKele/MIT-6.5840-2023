#!/usr/bin/env python3
import sys
import shutil
from typing import Optional, List, Tuple, Dict

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "TIMR": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEAD": "#d0b343",
    "TERM": "#70c43f",
    "LOG1": "#4878bc",
    "LOG2": "#398280",
    "CMIT": "#98719f",
    "PERS": "#d08341",
    "SNAP": "#FD971F",
    "DROP": "#ff615c",
    "CLNT": "#00813c",
    "TEST": "#fe2c79",
    "INFO": "#ffffff",
    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
    "HEAR": "cyan",
}
# TOPICS = {
#     "TIMR": "bright_black",
#     "VOTE": "bright_cyan",
#     "LEAD": "yellow",
#     "TERM": "green",
#     "LOG1": "blue",
#     "LOG2": "cyan",
#     "CMIT": "magenta",
#     "PERS": "white",
#     "SNAP": "bright_blue",
#     "DROP": "bright_red",
#     "CLNT": "bright_green",
#     "TEST": "bright_magenta",
#     "INFO": "bright_white",
#     "WARN": "bright_yellow",
#     "ERRO": "red",
#     "TRCE": "red",
# }
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize: bool = typer.Option(True, "--no-color"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
    ignore: Optional[str] = typer.Option(None, "--ignore", "-i", callback=list_topics),
    just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
):
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        try:
            time, topic, *msg = line.strip().split(" ")
            # To ignore some topics
            if topic not in topics:
                continue

            msg = " ".join(msg)

            # Debug calls from the test suite aren't associated with
            # any particular peer. Otherwise we can treat second column
            # as peer id
            if topic != "TEST":
                i = int(msg[1])

            # Colorize output by using rich syntax when needed
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            # Single column printing. Always the case for debug stmts in tests
            if n_columns is None or topic == "TEST":
                print(time, msg)
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                print(cols)
        except:
            # Code from tests or panics does not follow format
            # so we print it as is
            if line.startswith("panic"):
                panic = True
            # Output from tests is usually important so add a
            # horizontal line with hashes to make it more obvious
            if not panic:
                print("#" * console.width)
            print(line, end="")


if __name__ == "__main__":
    typer.run(main)
