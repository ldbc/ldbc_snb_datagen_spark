library(tidyverse)
library(plyr)
library(drc)
library(nCal)

df <- read.csv('data.csv')

m2 = drm.fit(df$np~df$sf, dat = df)
predict(m2, newdata = data.frame(df$sf))
plot(m2, log="xy")

predict(m2, newdata = data.frame(sf = 300))
predict(m2, newdata = data.frame(sf = 1000))
predict(m2, newdata = data.frame(sf = 3000))   #  8 991 224 
predict(m2, newdata = data.frame(sf = 10000))  # 20 251 346
