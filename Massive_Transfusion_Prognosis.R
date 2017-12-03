# Load configuration settings

library(RPostgreSQL)

dbdriver <- 'PostgreSQL'
host  <- '127.0.0.1'
port  <- '5432'
user  <- 'user1'
password <- 'KkuZtKP9Vc'
dbname <- 'mimic'
schema <- 'mimiciii'

# Connect to the database using the configuration settings
con <- dbConnect(dbDriver(dbdriver), dbname = dbname, host = host, port = port, 
                 user = user, password = password)

# Set the default schema
dbExecute(con, paste("SET search_path TO ", schema, sep=" "))

#In order how much time there has been between the admission time and the transfusion time, we need to get the admission time  ('intime') from 'icustays'.
sql_query_admtime <- "SELECT i.icustay_id, i.intime
FROM icustays i;"
admtimes <- dbGetQuery(con, sql_query_admtime)

# There are three codes in 'inputevents_mv' related to transfusion (225168, 227070, 226368). We get the rows with this information.
sql_query_pat_item <- "SELECT*
FROM inputevents_mv i where itemid='225168' or itemid='227070' or itemid='226368';"
data <- dbGetQuery(con, sql_query_pat_item)

# We left join merge the obtained data with the admission time.
datamerged <- merge(x = data, y = admtimes, by = "icustay_id", all.x = TRUE)

# We collect the day when each transfusion was performed.
datamerged$newtime<-datamerged$starttime-datamerged$intime
datamerged$newtime<-datamerged$newtime/86400
datamerged$newtime<-floor(datamerged$newtime)

# We collect only the transfusion data from the two first days and we perofrm some tidying.
data2firstdays<-datamerged[datamerged$newtime<2,]
data2firstdays$newtime<-as.integer(data2firstdays$newtime)
data2firstdays<-data2firstdays[data2firstdays$newtime!=-1,]

# We collect the data of  transfusions during the first two days for patient.
trans_per_day_data<-table(data.frame(data2firstdays$icustay_id,data2firstdays$newtime))
trans_per_day_data<-as.data.frame.matrix(trans_per_day_data)
trans_per_day_data$icustay_id<-rownames(trans_per_day_data)
rownames(trans_per_day_data)<-NULL

# We choose from 'datamerged' the variables we are interested in and join the data with the data of transfusions per day.
newdata <- merge(x = trans_per_day_data, y = datamerged[,c(1,3,4)], by = "icustay_id", all.x = TRUE)
newdata <- newdata[!duplicated(newdata),]
colnames(newdata)[2]<-'transf0'
colnames(newdata)[3]<-'transf1'

# We prepare vectors of icustays and hadm_ids that will be useful for SQL queries
IDlist <- newdata$icustay_id
IDlist_hadm <- newdata$hadm_id

# We choose from 'inputevents_mv' data of patients with transfusions during the first two days.
a<-paste("SELECT*
         FROM inputevents_mv i WHERE icustay_id IN (", paste(IDlist, collapse = ", "), ")")
data_inputeventsall <- dbGetQuery(con, a)

# We choose from 'inputevents_mv' data of patients with transfusions during the first two days.
b<-paste("SELECT*
         FROM admissions i WHERE hadm_id IN (", paste(IDlist_hadm, collapse = ", "), ")")
data_discharges <- dbGetQuery(con, b)

# We prepare a factor variable with the outcome (death or not) of the patient from the ICU.
#TODO: set restrictionsi n death time.
data_discharges$outcome<- factor(ifelse(!is.na(data_discharges$deathtime), "YES", "NO"))

# We choose only hadm, diagnostic and outcome from 'data_discharges'.
data_discharges_clean<-data_discharges[,c(3,17,20)]

# Left join merge of inputevents_mv with admission times.
datamerged_big <- merge(x = data_inputeventsall, y = admtimes, by = "icustay_id", all.x = TRUE)

# We estimate when the data in 'datamerged_big' was collcted from the admission time.
datamerged_big$newtime<-datamerged_big$starttime-datamerged_big$intime
datamerged_big$newtime<-datamerged_big$newtime/86400
datamerged_big$newtime<-floor(datamerged_big$newtime)

# We select only the data from the first two days since admission
data2firstdays_big<-datamerged_big[datamerged_big$newtime<2,]
data2firstdays_big$newtime<-as.integer(data2firstdays_big$newtime)
data2firstdays_big<-data2firstdays_big[data2firstdays_big$newtime!=-1,]

# Left join merge with data of outcome
datamerged_superbig <- merge(x = data2firstdays_big, y = data_discharges_clean, by = "hadm_id", all.x = TRUE)


#We collect data of dopamine administrated during each one of the first two days.
aa=as.data.frame(table(data.frame(datamerged_superbig$hadm_id,datamerged_superbig$itemid)[datamerged_superbig$newtime==0,]))
bb=which(aa[,2]==221662&aa[,3]>0)
newdata$dopa0=0
newdata$dopa0[newdata$hadm_id %in% aa[bb,1]]=1
aa=as.data.frame(table(data.frame(datamerged_superbig$hadm_id,datamerged_superbig$itemid)[datamerged_superbig$newtime==1,]))
bb=which(as.data.frame(aa)[,2]==221662&aa[,3]>0)
newdata$dopa1=0
newdata$dopa1[newdata$hadm_id %in% aa[bb,1]]=1


#We collect data of vasopressors administrated during each one of the first two days.
aa=as.data.frame(table(data.frame(datamerged_superbig$hadm_id,datamerged_superbig$itemid)[datamerged_superbig$newtime==0,]))
bb=which(aa[,2]==222315&aa[,3]>0)
newdata$vasop0=0
newdata$vasop0[newdata$hadm_id %in% aa[bb,1]]=1
aa=as.data.frame(table(data.frame(datamerged_superbig$hadm_id,datamerged_superbig$itemid)[datamerged_superbig$newtime==1,]))
bb=which(as.data.frame(aa)[,2]==222315&aa[,3]>0)
newdata$vasop1=0
newdata$vasop1[newdata$hadm_id %in% aa[bb,1]]=1


#We collect data of norepinephrine administrated during each one of the first two days.
aa=as.data.frame(table(data.frame(datamerged_superbig$hadm_id,datamerged_superbig$itemid)[datamerged_superbig$newtime==0,]))
bb=which(as.data.frame(aa)[,2]==221906&aa[,3]>0)
newdata$norep0=0
newdata$norep0[newdata$hadm_id %in% aa[bb,1]]=1
aa=as.data.frame(table(data.frame(datamerged_superbig$hadm_id,datamerged_superbig$itemid)[datamerged_superbig$newtime==1,]))
bb=which(as.data.frame(aa)[,2]==221906&aa[,3]>0)
newdata$norep1=0
newdata$norep1[newdata$hadm_id %in% aa[bb,1]]=1



# order by 'hadm_id'
newdata<-newdata[order(newdata$hadm_id),]

# we prepare a vector of not duplicated  'icustay_id's in 'datamerged_superbig'.
a<-duplicated(datamerged_superbig$icustay_id)

# We incorporate outcome an diagnosis information for each ham_id.
newdata$outcome<-datamerged_superbig$outcome[a==FALSE]
newdata$diag<-datamerged_superbig$diagnosis[a==FALSE]

# We prepare a variable which informs us if a patient has received five or more transfusions in at least one of the two first days.
newdata$group<-0
as=apply(newdata[,2:3], 1, max)
newdata$group[as>=5]<-1

#We prepare a summary of the information relating the outcome and the presence of massive transfusion during the two first days. As expected, there is a significantly higher presence of deaths in people who have received massive transfusion.
summary(table(data.frame(newdata$outcome,newdata$group)))
mortality_table<-table(data.frame(newdata$outcome,newdata$group))
deaths_massive <- mortality_table[4]*100/(mortality_table[4]+mortality_table[3])
deaths_non_massive <- mortality_table[2]*100/(mortality_table[1]+mortality_table[2])

# Next steps:
# - To select only deaths within the 28 first days since admission.
# - To extract variables that may be correlated with the higher presence of death as outcome after massive transfusion. These variables may be related to a worse condition of the patient or may even have a certain influence on the outcome of the patient.
# - These variables should be extracted from other tables and the moment since admission for each variable value should be calculated. Then only the data from the two first days since admission would be chosen and the procedure shown with dopamine, norepinephrine, and vasopressors would be replicated.
# - Some variables we deem interesting to add to this analysis would be trauma, haemorrhage, acidosis, hypothermia.
# - Depending on the variable, the analysis would be qualitative (chi-square) or quantitative (t-test). If multiple tests, then p-value adjustments would be replicated. Exploratory analysis of data distributions would be recommended to avoid the appearance of false positives or negatives caused by e.g. non-normal distributions
# - An interesting approach would be a machine learning implementation with multiple variables, that could help improve the classification of the outcome.