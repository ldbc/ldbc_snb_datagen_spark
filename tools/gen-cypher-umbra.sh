#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

# generate BI test data sets
for SF in 0.003 0.1 0.3 1 3; do
    rm -rf out-sf${SF}/
    tools/run.py \
        --cores $(nproc) \
        --memory ${LDBC_SNB_DATAGEN_MAX_MEM} \
        ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
        -- \
        --format csv \
        --scale-factor ${SF} \
        --mode bi \
        --output-dir out-sf${SF} \
        --generate-factors
    
    rm -rf out-${SF}/graphs/parquet    
    tools/run.py \
        --cores $(nproc) \
        --memory ${LDBC_SNB_DATAGEN_MAX_MEM} \
        ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
        -- \
        --format csv \
        --scale-factor ${SF} \
        --explode-edges \
        --mode bi \
        --output-dir out-sf${SF}/ \
        --format-options header=false,quoteAll=true,compression=gzip
done
