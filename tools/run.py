#!/usr/bin/env python3

import argparse

from subprocess import run
from typing import Optional, Dict

from datagen import lib, util


def flatten(ls):
    return [i for sl in ls for i in sl]


def run_local(
        jar_file: str,
        params_file: str,
        cores: Optional[int] = None,
        memory: Optional[str] = None,
        parallelism: Optional[int] = None,
        spark_conf: Optional[Dict] = None
):
    if not cores:
        cores = "*"
    if not spark_conf:
        spark_conf = {}
    opt_class = ['--class', lib.main_class]
    opt_master = ['--master', f'local[{cores}]']

    additional_opts = []

    # In local mode execution takes place on the driver
    if memory:
        additional_opts.extend(['--driver-memory', memory])

    final_spark_conf = {
        **({'spark.default.parallelism': str(parallelism)} if parallelism else {}),
        **spark_conf
    }

    arg_opts = [
        *(['--num-threads', str(parallelism)] if parallelism else [])
    ]

    conf = flatten([['-c', f'{k}={v}'] for k, v in final_spark_conf.items()])
    cmd = [
        'spark-submit',
        *conf,
        *opt_master,
        *opt_class,
        *additional_opts,
        jar_file,
        params_file,
        *arg_opts
    ]

    run(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a Datagen job locally')
    parser.add_argument('jar',
                        type=str,
                        help='LDBC Datagen JAR file')
    parser.add_argument('params_file',
                        type=str,
                        help='params file name')
    parser.add_argument('--cores',
                        type=int,
                        help='number of vcpu cores to use'
                        )
    parser.add_argument('--memory',
                        type=str,
                        help='amount of memory to use. E.g 512m, 16g, 1t'
                        )
    parser.add_argument('--conf',
                        action=util.KeyValue,
                        help="Spark conf as a list of key=value pairs")
    parser.add_argument('--parallelism',
                        type=int,
                        help='sets job parallelism. Higher values might reduce chance of OOM.')
    parser.add_argument('-y',
                        action='store_true',
                        help='Assume \'yes\' for prompts')

    args = parser.parse_args()

    run_local(
        args.jar,
        args.params_file,
        cores=args.cores,
        memory=args.memory,
        parallelism=args.parallelism,
        spark_conf=args.conf
    )
