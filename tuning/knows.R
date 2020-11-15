# The target % of explicit know edge delete events across the 
# simulation period is 5%.

## libraries 
suppressMessages(library(tidyverse))
suppressMessages(library(lubridate))

## data
DIR = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/out/social_network/dynamic/"
PKP = paste0(DIR,"person_knows_person_0_0_trimmed.csv")
knows = suppressMessages(read_delim(PKP,"|")) # sf1
knows = knows %>% rename(similarity=weight)

## eda
mean(knows$similarity) # 0.9222521

## plots
sim_hist = ggplot(knows, aes(x=similarity)) + 
  geom_histogram() + 
  theme_bw() 
sim_hist


