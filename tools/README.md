# Distribution Analysis

This directory includes the scripts, data and plots for the various distributions used in Datagen.

```
# produces the CDF used when assigning comments a creation date
./comment_cdf.sh

# produces the CDF used when assiging posts in flashmob events a creation date
./flashmob_posts_cdf.sh

# produces a histogram of the knows degree distribution for SF1 and prints the average degree for each scale factor
./fb_degree_analysis.sh

# produces the analysis of the Facebook friend removal data
./fb_dump_analysis.sh

# delete operation counts
./delete_operation_counts.sh
```

Note, raw data of Facebook user's friend removal is not included. However, plots `friends_vs_removed`, `hist_num_friends` and `friends_vs_removed.pdf` can be found in `/graphics/`.
