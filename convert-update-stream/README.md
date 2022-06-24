# Converter

Script to convert from BI to Interactive update streams.
The conversion includes merging daily batches into a single file, sorting by `creationDate` and pre-joining attributes (e.g. `tagIds`).

## Prerequisites

Install DuckDB:

```bash
pip3 install --user duckdb==0.3.4
```

## Usage

Generate the data set:

```bash
export SF=0.1
```
```bash
tools/run.py \
    --cores 4 \
    --memory 8G \
    ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
    -- \
    --format csv \
    --scale-factor ${SF} \
    --explode-edges \
    --mode bi \
    --output-dir out-sf${SF}/
```

Run the converter in this directory:

```bash
mkdir out-sf${SF}-test
python3 convert.py --input_dir ../out-sf${SF} --output_dir out-sf${SF}-test
```
