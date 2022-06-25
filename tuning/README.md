# Datagen Tuning

## 1. Run Datagen

Navigate to `LDBC_SNB_DATAGEN_HOME` and follow Datagen instructions to generate data. 

+ TODO: currently produces no update streams/refresh batches.

## 2. Run Analysis

`analysis.R` takes the generated update operation `csv` files as input. 
Analysis:
+ total operation count per operation type across the update period. 
+ plot of operation count, per day, per operation type across the update period; stored in `figs`. 

```bash
# navigate to tuning directory
cd tuning/

# run analysis
./run-analysis.sh
```

## 3. Tuning Report

+ TODO: list configurable parameters
+ TODO: maybe add rmarkdown to easily visualise changes in plots
