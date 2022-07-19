#!/usr/bin/env python3

import boto3
from os import path
from datetime import datetime
import pprint
import csv
import re
import __main__

from math import ceil
from datagen import lib, util

import argparse

from datagen.util import KeyValue, split_passthrough_args

min_num_workers = 1
max_num_workers = 1000
min_num_threads = 1

defaults = {
    'bucket': 'ldbc-snb-datagen-store',
    'use_spot': True,
    'master_instance_type': 'r6gd.2xlarge',
    'instance_type': 'r6gd.4xlarge',
    'executors_per_sf': 1e-3,
    'partitions_per_sf': 1e-1,
    'az': 'us-east-2c',
    'yes': False,
    'ec2_key': None,
    'emr_release': 'emr-6.6.0'
}

pp = pprint.PrettyPrinter(indent=2)

dir = path.dirname(path.realpath(__file__))
ec2info_file = 'Amazon EC2 Instance Comparison.csv'


with open(path.join(dir, ec2info_file), mode='r') as infile:
    reader = csv.DictReader(infile)
    ec2_instances = [dict(row) for row in reader]


def get_instance_info(instance_type):
    def parse_vcpu(col):
        return int(re.search(r'(\d+) .*', col).group(1))

    def parse_mem(col):
        return int(re.search(r'(\d+).*', col).group(1))

    vcpu = next((parse_vcpu(i['vCPUs']) for i in ec2_instances if i['API Name'] == instance_type), None)
    mem = next((parse_mem(i['Memory']) for i in ec2_instances if i['API Name'] == instance_type), None)

    if vcpu is None or mem is None:
        raise Exception(f'unable to find instance type `{instance_type}`. If not a typo, reexport `{ec2info_file}` from ec2instances.com')

    return {'vcpu': vcpu, 'mem': mem}


def submit_datagen_job(name,
                       sf,
                       format,
                       mode,
                       bucket,
                       jar,
                       use_spot,
                       instance_type,
                       executors,
                       executors_per_sf,
                       partitions,
                       partitions_per_sf,
                       master_instance_type,
                       az,
                       emr_release,
                       yes,
                       ec2_key,
                       conf,
                       copy_filter,
                       copy_all,
                       passthrough_args, **kwargs
                       ):

    is_interactive = (not yes) and hasattr(__main__, '__file__')

    build_dir = '/ldbc_snb_datagen/build'

    if not copy_filter:
        copy_filter = f'.*{build_dir}/graphs/{format}/{mode}/.*'
    else:
        copy_filter = f'.*{build_dir}/{copy_filter}'

    emr = boto3.client('emr')

    ts = datetime.utcnow()
    ts_formatted = ts.strftime('%Y%m%d_%H%M%S')

    jar_url = f's3://{bucket}/jars/{jar}'

    results_url = f's3://{bucket}/results/{name}'
    run_url = f'{results_url}/runs/{ts_formatted}'

    spark_config = {
        'maximizeResourceAllocation': 'true'
    }

    if executors is None:
        executors = max(min_num_workers, min(max_num_workers, ceil(sf * executors_per_sf)))

    if partitions is None:
        partitions = max(min_num_threads, ceil(sf * partitions_per_sf))

    spark_defaults_config = {
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.default.parallelism': str(partitions),
        **(dict(conf) if conf else {})
    }

    market = 'SPOT' if use_spot else 'ON_DEMAND'

    ec2_key_dict = {'Ec2KeyName': ec2_key} if ec2_key is not None else {}

    job_flow_args = {
        'Name': f'{name}_{ts_formatted}',
        'LogUri': f's3://{bucket}/logs/emr',
        'ReleaseLabel': emr_release,
        'Applications': [
            {'Name': 'hadoop'},
            {'Name': 'spark'},
            {'Name': 'ganglia'}
        ],
        'Configurations': [
            {
                'Classification': 'spark',
                'Properties': spark_config
            },
            {
                'Classification': 'spark-defaults',
                'Properties': spark_defaults_config
            }
        ],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': "Driver node",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                },
                {
                    'Name': "Worker nodes",
                    'Market': market,
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': executors,
                }
            ],
            **ec2_key_dict,
            'Placement': {'AvailabilityZone': az},
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'VisibleToAllUsers': True,
        'Steps': [
            {
                'Name': 'Run LDBC SNB Datagen',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--class', lib.main_class, jar_url,
                             '--output-dir', build_dir,
                             '--scale-factor', str(sf),
                             '--num-threads', str(partitions),
                             '--mode', mode,
                             '--format', format,
                             *passthrough_args
                             ]
                }

            },
            {
                'Name': 'Save output',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': 'command-runner.jar',
                    'Args': ['s3-dist-cp',
                             '--src', f'hdfs://{build_dir}',
                             '--dest', f'{run_url}/social_network',
                             *(['--srcPattern', copy_filter] if not copy_all else [])
                             ]
                }
            }]
    }

    if is_interactive:
        job_flow_args_formatted = pp.pformat(job_flow_args)
        if not util.ask_continue(f'Job parameters:\n{job_flow_args_formatted}'):
            return

    emr.run_job_flow(**job_flow_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Submit a Datagen job to EMR')
    parser.add_argument('name',
                        type=str,
                        help='name')
    parser.add_argument('sf', type=int,
                        help='scale factor (used to calculate cluster size)')
    parser.add_argument('format', type=str, help='the required output format')
    parser.add_argument('mode', type=str, help='output mode')
    market_args = parser.add_mutually_exclusive_group()
    market_args.add_argument('--use-spot',
                             default=defaults['use_spot'],
                             action='store_true',
                             help='Use SPOT workers')
    market_args.add_argument('--no-use-spot',
                             default=not defaults['use_spot'],
                             dest='use_spot',
                             action='store_false',
                             help='Do not use SPOT workers')
    parser.add_argument('--az',
                        default=defaults['az'],
                        help=f'Cluster availability zone. Default: {defaults["az"]}')
    parser.add_argument('--bucket',
                        default=defaults['bucket'],
                        help=f'LDBC SNB Datagen storage bucket. Default: {defaults["bucket"]}')
    parser.add_argument('--instance-type',
                        default=defaults['instance_type'],
                        help=f'Override the EC2 instance type used for worker nodes. Default: {defaults["instance_type"]}')
    parser.add_argument('--ec2-key',
                        default=defaults['ec2_key'],
                        help='EC2 key name for SSH connection')
    parser.add_argument('--jar',
                        required=True,
                        help='LDBC SNB Datagen library JAR name')
    parser.add_argument('--emr-release',
                        default=defaults['emr_release'],
                        help='The EMR release to use. E.g. emr-6.6.0')
    parser.add_argument('-y', '--yes',
                        default=defaults['yes'],
                        action='store_true',
                        help='Assume \'yes\' for prompts')
    copy_args = parser.add_mutually_exclusive_group()
    copy_args.add_argument('--copy-filter',
                           type=str,
                           help='A regular expression specifying filtering paths to copy from the build dir to S3. '
                                'By default it is \'graphs/{format}/{mode}/.*\'')
    copy_args.add_argument('--copy-all',
                           action='store_true',
                           help='Copy the complete build dir to S3')
    parser.add_argument("--conf",
                            metavar="KEY=VALUE",
                            nargs='+',
                            action=KeyValue,
                            help="SparkConf as key=value pairs")
    executor_args=parser.add_mutually_exclusive_group()
    executor_args.add_argument("--executors",
                               type=int,
                               help=f"Total number of Spark executors."
                               )
    executor_args.add_argument("--executors-per-sf",
                               type=float,
                               default=defaults['executors_per_sf'],
                               help=f"Number of Spark executors per scale factor. Default: {defaults['executors_per_sf']}"
                               )
    partitioning_args = parser.add_mutually_exclusive_group()
    partitioning_args.add_argument("--partitions",
                                   type=int,
                                   help=f"Total number of Spark partitions to use when generating the dataset."
                                   )
    partitioning_args.add_argument("--partitions-per-sf",
                                   type=float,
                                   default=defaults['partitions_per_sf'],
                                   help=f"Number of Spark partitions per scale factor to use when generating the dataset. Default: {defaults['partitions_per_sf']}"
                                   )

    parser.add_argument('--', nargs='*', help='Arguments passed to LDBC SNB Datagen', dest="arg")

    self_args, passthrough_args = split_passthrough_args()

    args = parser.parse_args(self_args)

    submit_datagen_job(passthrough_args=passthrough_args,
                       master_instance_type=defaults['master_instance_type'],
                       **args.__dict__)
