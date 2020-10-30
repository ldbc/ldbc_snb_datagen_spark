# libraries
suppressMessages(library(tidyverse))
options(scipen=999)

## data ##
ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/src/main/resources/dictionaries/"
buckets = paste0(ldbc,"facebookBucket100.dat")
buckets = read_delim(buckets,delim = " ",col_names=c("min","max","id"))

# handcooked cdf
a = 0.3
r = 0.629
n = 10
p1 = rep(0,10) 
p1[1] = a
for (i in 1:10) {
  p1[i+1] = p1[i] * r
}
p2= rep(0,90)
p2[1] = 0.002
r=0.99
for (i in 1:90) {
  p2[i+1] = p2[i] * r
}
p3 = rep((1 - (sum(p1) + sum(p2)))/900,900)
cdf = c(p1,p2,p3)

write.table(cdf, file = paste0(ldbc,"personDelete.txt"), row.names = FALSE, col.names = FALSE)

plot(cdf )

## functions ##
# Calculates the probability a person is deleted given their max friends.
pd_cdf = function(max_friends,cdf) {
  return(cdf[max_friends + 1])
}
# Calculates the avg number of friends for a given scale factor.
tavg = function(s) {
  avg = s^(0.512 - (0.028 * log10(s)))
  return(round(avg))
}
# Rescales the bucket boundaries for a given network size.
rebuild_buckets = function(buckets,network_size) {
  target = tavg(network_size) # mean friends per person
  fbavg = 190 # fb mean friends
  rebuilt = buckets %>% transmute(min = (min * target)/fbavg, max = (max * target)/fbavg)
  return(rebuilt)
}
# Generates a max knows degree for a person.
next_degree = function(rebuilt_buckets) {
  max_friends = 1000 # max friends per person 
  r_bucket = sample(1:100,1,replace = T) # pick bucket
  bucket = rebuilt_buckets[r_bucket,]
  bound = round(bucket$max) - round(bucket$min)
  if (bound == 0) {
    degree = round(bucket$min)
  } else {
    degree = sample(round(bucket$min):round(bucket$max),1,replace = T) 
  }
  degree = min(degree,max_friends)
  return(degree)
}

## analysis ##
sf1 = 11000
tavg(sf1)
rebuilt = rebuild_buckets(buckets,sf1)
samp = rep(0,1000)
for (i in 1:length(samp)) {
  samp[i] = next_degree(rebuilt)
}
mean(samp)

max_friends = rep(0,sf1)
delete_prob = rep(0,sf1)
deleted = rep(T,sf1)
for (i in 1:sf1) {
  max_friends[i] = next_degree(rebuilt)
  delete_prob[i] = pd_cdf(max_friends[i],cdf)
  if (delete_prob[i] > runif(1)) {
    deleted[i] = T
  } else {
    deleted[i] = F
  }
}
sum(deleted)/length(deleted)
max_friends[which(deleted == T)]







