
args <- commandArgs(trailingOnly=TRUE);
print(args[1]);
data <- as.matrix(read.table(paste(args[1],sep=''),header=T))
n = sum(!is.na(data))
data.ecdf = ecdf(data)
data.mean = mean(data)
data.max = max(data)
data.min = min(data)

postscript(args[2])
plot(data.ecdf, xlab = args[3], log="x",xlim=c(1,data.max), ylab = "cumulative probability", main = '')
text((data.max - data.min)/10,0.9,paste("Average degree: ",data.mean,sep=" "))
dev.off()
