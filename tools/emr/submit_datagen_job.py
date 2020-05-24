#!/usr/bin/env python3

import boto3
from os import path
from datetime import datetime
import pprint
import __main__

import argparse

main_class = 'ldbc.snb.datagen.spark.LdbcDatagen'

version = '0.4.0-SNAPSHOT'

min_num_workers = 1
max_num_workers = 100

defaults = {
    'bucket': 'ldbc-snb-datagen-store',
    'use_spot': False,
    'instance_type': 'i3.xlarge',
    'az': 'us-west-2a',
    'is_interactive': False
}

pp = pprint.PrettyPrinter(indent=2)


def ask_continue(message):
    print(message)
    resp = None
    inp = input("Continue? [Y/N]:").lower()
    while resp is None:
        if inp == 'y' or inp == 'yes':
            resp = True
        elif inp == 'n' or inp == 'no':
            resp = False
        else:
            inp = input("Please answer yes or no:").lower()
    return resp


def calculate_cluster_config(scale_factor):
    num_workers = max(min_num_workers, min(max_num_workers, scale_factor // 20))
    return {
        'num_workers': num_workers,
    }


def submit_datagen_job(params_file, sf,
                       bucket=defaults['bucket'],
                       use_spot=defaults['use_spot'],
                       instance_type=defaults['instance_type'],
                       az=defaults['az'],
                       is_interactive=defaults['is_interactive']
                       ):
    emr = boto3.client('emr')

    name = path.splitext(params_file)[0]
    ts = datetime.utcnow()
    ts_formatted = ts.strftime('%Y%m%d_%H%M%S')

    jar_url = f's3://{bucket}/jars/ldbc_snb_datagen-{version}-jar-with-dependencies.jar'
    params_url = f's3://{bucket}/params/{name}.ini'

    results_url = f's3://{bucket}/results/{name}'
    run_url = f'{results_url}/runs/{ts_formatted}'

    cluster_config = calculate_cluster_config(sf)

    spark_config = {
        'maximizeResourceAllocation': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
    }

    hdfs_prefix = '/ldbc_snb_datagen'

    build_dir = f'{hdfs_prefix}/build'
    sn_dir = f'{hdfs_prefix}/social_network'

    market = 'SPOT' if use_spot else 'ON_DEMAND'

    job_flow_args = {
        'Name': f'{name}_{ts_formatted}',
        'LogUri': f's3://{bucket}/logs/emr',
        'ReleaseLabel': 'emr-5.30.0',
        'Applications': [
            {'Name': 'hadoop'},
            {'Name': 'spark'}
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
                    'InstanceType': instance_type,
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
                    'Args': ['spark-submit', '--class', main_class, jar_url, params_url,
                             '--sn-dir', sn_dir, '--build-dir', build_dir]
                }

            },
            {
                'Name': 'Save output',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': 'command-runner.jar',
                    'Args': ['s3-dist-cp',
                             '--src', f'hdfs://{sn_dir}',
                             '--dest', f'{run_url}/social_network'
                             ]
                }
            }]
    }

    if is_interactive:
        job_flow_args_formatted = pp.pformat(job_flow_args)
        if not ask_continue(f'Job parameters:\n{job_flow_args_formatted}'):
            return


    emr.run_job_flow(**job_flow_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Submit a Datagen job to EMR')
    parser.add_argument('params_url', type=str,
                        help='params file name')
    parser.add_argument('sf', type=int,
                        help='scale factor (used to calculate cluster size)')
    parser.add_argument('--use-spot', action='store_true',
                        help='Use SPOT workers')
    parser.add_argument('--az', default=defaults['az'], help='Cluster availability zone')
    parser.add_argument('--bucket', default=defaults['bucket'],
                        help='LDBC SNB Datagen storage bucket')
    parser.add_argument('-y', action='store_true')

    args = parser.parse_args()

    is_interactive = hasattr(__main__, '__file__')

    submit_datagen_job(args.params_url, args.sf,
                       bucket=args.bucket, use_spot=args.use_spot, az=args.az,
                       is_interactive=is_interactive and not args.y
                       )
