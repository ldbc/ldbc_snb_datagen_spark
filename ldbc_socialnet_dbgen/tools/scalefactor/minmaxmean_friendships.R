library(data.table)
library(igraph)
#library(RSvgDevice)
library(plotrix)

message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

message("Set column names")
#names(df)=c("Post", "Person")
setNames(df, c("Person1", "Person2"))

d2 <- df[,length(Person2),by=Person1]
#message("STATISTICS: Friendships/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))

message("\\hline  \\#friends/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")


#devSVG(file = "numFriendsDensity.svg")
#pdf("numFriendsDensity.pdf")
#plot(density(d2$V1), main="Density #friends per user") 
#dev.off()
#devSVG(file = "numFriendsCumm.svg")	

message("Plot cummulative distribution of #friends/users")
pdf("numFriendsCumm.pdf")
plot(ecdf(d2$V1),main="Cummulative distribution #friends per user", xlab="Number of friends", ylab="Percentage number of users")	
dev.off()
