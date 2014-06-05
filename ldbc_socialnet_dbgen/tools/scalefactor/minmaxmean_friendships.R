library(data.table)
library(igraph)
#library(RSvgDevice)
library(plotrix)
require(bit64)
#require(int64)	

message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

message("Set column names")
setNames(df, c("Person1", "Person2"))

library(plyr)
d2 <- ddply(df, "Person1", summarize, P2Count = length(Person2))
d3 <- as.numeric(d2$P2Count)
message("\\hline  \\#friends/user  &", min(d3) ,  " &  ", max(d3),  " & ", round(mean(d3)) , " & ", round(median(d3)), " \\\\")

#d2 <- df[,length(Person2),by=Person1]
#message("\\hline  \\#friends/user  &", min(d2$V1) ,  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")


#devSVG(file = "numFriendsDensity.svg")
#pdf("numFriendsDensity.pdf")
#plot(density(d2$V1), main="Density #friends per user") 
#dev.off()
#devSVG(file = "numFriendsCumm.svg")	

message("Plot cummulative distribution of #friends/users")
pdf("numFriendsCumm.pdf")
plot(ecdf(d3),main="Cummulative distribution #friends per user", xlab="Number of friends", ylab="Percentage number of users")	
dev.off()
