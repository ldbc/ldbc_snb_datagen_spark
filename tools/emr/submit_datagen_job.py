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

defaults = {
    'bucket': 'ldbc-snb-datagen-store',
    'use_spot': False,
    'master_instance_type': 'm5d.2xlarge',
    'instance_type': 'i3.4xlarge',
    'sf_ratio': 100.0, # ratio of SFs and machines. a ratio of 50.0 for SF100 yields 2 machines
    #'sf_ratio': 240.0, # for limited vCPU counts, this ratio is still sufficient to generate the data
    'platform_version': lib.platform_version,
    'version': lib.version,
    'az': 'us-west-2c',
    'yes': False,
    'ec2_key': None,
    'emr_release': 'emr-5.31.0'
}

pp = pprint.PrettyPrinter(indent=2)

dir = path.dirname(path.realpath(__file__))
ec2info_file = 'Amazon EC2 Instance Comparison.csv'


with open(path.join(dir, ec2info_file), mode='r') as infile:
    reader = csv.DictReader(infile)
    ec2_instances = [dict(row) for row in reader]


def calculate_cluster_config(scale_factor, sf_ratio, vcpu):
    num_workers = max(min_num_workers, min(max_num_workers, ceil(scale_factor / sf_ratio)))
    parallelism_factor = max(2.0, sf_ratio / vcpu / 3)
    num_threads = ceil(num_workers * vcpu * parallelism_factor)
    return {
        'num_workers': num_workers,
        'num_threads': num_threads
    }


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
                       use_spot,
                       instance_type,
                       sf_ratio,
                       master_instance_type,
                       az,
                       emr_release,
                       platform_version,
                       version,
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

    exec_info = get_instance_info(instance_type)

    cluster_config = calculate_cluster_config(sf, sf_ratio, exec_info['vcpu'])

    emr = boto3.client('emr')

    ts = datetime.utcnow()
    ts_formatted = ts.strftime('%Y%m%d_%H%M%S')

    jar_url = f's3://{bucket}/jars/ldbc_snb_datagen_{platform_version}-{version}-jar-with-dependencies.jar'

    results_url = f's3://{bucket}/results/{name}'
    run_url = f'{results_url}/runs/{ts_formatted}'

    spark_config = {
        'maximizeResourceAllocation': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
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
                    'InstanceCount': cluster_config['num_workers'],
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
                             '--num-threads', str(cluster_config['num_threads']),
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
    parser.add_argument('--use-spot',
                        default=defaults['use_spot'],
                        action='store_true',
                        help='Use SPOT workers')
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
    parser.add_argument('--platform-version',
                        default=defaults['platform_version'],
                        help='The spark platform the JAR is compiled for formatted like {scala.compat.version}_spark{spark.comapt.version}, e.g. 2.11_spark2.4, 2.12_spark3.1')
    parser.add_argument('--version',
                        default=defaults['version'],
                        help='LDBC SNB Datagen library version')
    parser.add_argument('--emr-release',
                        default=defaults['emr_release'],
                        help='The EMR release to use. E.g emr-5.31.0, emr-6.1.0')
    parser.add_argument('-y', '--yes',
                        default=defaults['yes'],
                        action='store_true',
                        help='Assume \'yes\' for prompts')
    copy_args = parser.add_mutually_exclusive_group()
    copy_args.add_argument('--copy-filter',
                           type=str,
                           help='A regular expression specifying filtering paths to copy from the build dir to S3. '
                                'By default it is \'{format}/{mode}/.*\'')
    copy_args.add_argument('--copy-all',
                           action='store_true',
                           help='Copy the complete build dir to S3')
    parser.add_argument("--conf",
                            metavar="KEY=VALUE",
                            nargs='+',
                            type=KeyValue,
                            help="SparkConf as key=value pairs")

    parser.add_argument('--', nargs='*', help='Arguments passed to LDBC SNB Datagen', dest="arg")

    self_args, passthrough_args = split_passthrough_args()

    args = parser.parse_args(self_args)

    submit_datagen_job(passthrough_args=passthrough_args,
                       sf_ratio=defaults['sf_ratio'],
                       master_instance_type=defaults['master_instance_type'],
                       **args.__dict__)
