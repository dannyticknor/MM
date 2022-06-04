library(data.table)
#####import the raw data here(kw sheet from A/B Test report)
data <- read.csv("Downloads/Home Warranty LTK Test_Home Warranty LTK Test_keywords_120520 (15).csv")
data <- as.data.table(data)
data <- data[Keyword.state=="enabled" & Ad.group.state=="enabled",]

####You don't really need those three if you know column index of the metrics you are interested in
which(colnames(data)=="Cost")
which(colnames(data)=="Quality.score")
which(colnames(data)=="Conv..rate")
###############

#######subset the dataset based on the campaign names(base vs. experiment)
base <- data[Campaign == "[362](XB){MY}|T|!S!Home Warranty LTK",]
exp <- data[Campaign == "[362](XB){MY}|T|!S!Home Warranty LTK_MMMay20",]
base <- base[,c(3,5:7,15,18)]
exp <- exp[,c(3,5:7,15,18)]

colnames(base) <- c("Keyword","base_imp","base_click","base_conv","base_cost","base_CVR")
colnames(exp) <- c("Keyword","exp_imp","exp_click","exp_conv","exp_cost","exp_CVR")
new <- merge(base,exp,all.x = TRUE)

###########calculate the difference for each metrics
new <- new[,diff_imp:=exp_imp - base_imp][,diff_click:=exp_click - base_click][,diff_conv:=exp_conv - base_conv][,diff_cost:=base_cost-exp_cost][,diff_CVR:=exp_CVR - base_CVR]
new <- new[,diff_qs:=as.numeric(levels(exp_qs))[exp_qs] - as.numeric(levels(base_qs))[base_qs]]

#############export the resut
write.csv(new,file="LTK_KwComparison_CVR_060420.csv")


