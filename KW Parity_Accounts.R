library(data.table)
library(compare)

#######this script is to compare keyword count between Google and Bing
####import data here
avisGoogle <- as.data.table(All_Managed_coherency_90_Days_kw_overview_300420_300720_1_)
avisBing <- as.data.table(all_search_coherency_kw_overview_300420_300720_5_)
which(colnames(avisGoogle)=="Ad group state")
avisGoogle <- avisGoogle[,c(2:3,35:37)]
which(colnames(avisBing)=="Ad group state")
avisBing <- avisBing[,c(2:3,32:34)]

#############
require(dplyr)
difference1 <- anti_join(avisGoogle,avisBing,by = c("Ad group","Keyword"))
difference2 <- anti_join(avisBing,avisGoogle,by = c("Ad group","Keyword"))
write.csv(difference1, file = "Avis_Brand_kwInGGLNotBNG_v2.csv")
#############


