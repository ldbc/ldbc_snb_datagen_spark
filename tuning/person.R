# libraries
suppressMessages(library(tidyverse))
options(scipen=999)

## data ##
ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/src/main/resources/dictionaries/"
buckets = paste0(ldbc,"facebookBucket100.dat")
buckets = read_delim(buckets,delim = " ",col_names=c("min","max","id"))

# handcooked iwiw pmf
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

(1 - (sum(p1) + sum(p2)))/900

p3 = rep((1 - (sum(p1) + sum(p2)))/900,900)
pmf = c(p1,p2,p3)
plot(pmf) # TODO: ggplot


ggplot(data = data.frame(x = 1:1002,y = pmf),
       aes(x = x, y = y)) +
  geom_line() +
  scale_y_continuous(name="explicit deletion probability",
                     limits = c(0.0,0.31)) +
   scale_x_log10(name="knows degree") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5),
        text = element_text(size = 12))
ldbc = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/tuning/figs/"
ggsave(paste0(ldbc,"person.pdf"), width = 5, height = 5,device = "pdf")

write.table(pmf, file = paste0(ldbc,"personDelete.txt"), row.names = FALSE, col.names = FALSE)



## functions ##
# Calculates the probability a person is deleted given their max friends.
pd_cdf = function(max_friends,pmf) {
  return(pmf[max_friends + 1])
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
  delete_prob[i] = pd_cdf(max_friends[i],pmf)
  if (delete_prob[i] > runif(1)) {
    deleted[i] = T
  } else {
    deleted[i] = F
  }
}
sum(deleted)/length(deleted)
max_friends[which(deleted == T)]







