

# Event identifiers #

# In this code we filter the RepRisk news data.
# 1. Loading RepRisk companies and adding CIK
# 2. Merging with RepRisk news data
# 3. Filter on high severity news (severity == 3)
# 4. Requiring minimum 2 years gap between incidents.

# Set WD
setwd("~/Library/CloudStorage/OneDrive-NorgesHandelshøyskole/Major in Business Analytics/MasterThesisData")

# -------- Libraries ----------
library(dplyr)
library(tidyverse)
library(tidyr)
library(stargazer)
library(ggplot2)

# Connecting to reprisk
library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='chrresch') # CHANGE TO YOUR USERNAME

#res <- dbSendQuery(wrds, "select *
#                   from crspa.crsp_a_ccm")
#data <- dbFetch(res, n=-1)
#dbClearResult(res)


# 1. Loading RepRisk companies and adding CIK ----------------------

# 1a. querying Reprisk company id --> 19024 companies
res <- dbSendQuery(wrds, "select *
                   from reprisk.std_company_identifiers")
reprisk_company_id <- dbFetch(res, n=-1)
dbClearResult(res)

# 1b. Getting CRSP/Compustat
res <- dbSendQuery(wrds, "select datadate, cusip, cik, conm, naicsh, sich, ib, at, tic
                   from comp.funda")
compustat <- dbFetch(res, n=-1) #%>% select(cusip, cik, conm, naics, year1, year2)
dbClearResult(res)
tail(compustat)

# 1c. Adding COMPUSTAT data to Reprisk companies
reprisk_separated <- reprisk_company_id %>%
  separate_rows(all_isins) %>%
  mutate(
    cusip = substr(all_isins, 3, 11)
  )

reprisk_comp <- left_join(reprisk_separated, 
                        compustat, 
                        by = "cusip") %>%
  distinct(reprisk_id, .keep_all = TRUE) %>%
  #drop_na(url, ib, at) %>%
  select(reprisk_id, name, url, sectors, cusip, datadate, cik, conm, naicsh, sich, ib, at, tic)



# 2. Wrangling the data ----------------------------------

# 2a. querying Reprisk news data
res <- dbSendQuery(wrds, "select *
                   from reprisk.std_news_data")
reprisk_news <- dbFetch(res, n=-1)
dbClearResult(res)

length(unique(reprisk_news$reprisk_id)) # 18 203 unique firms
length(reprisk_news$news_date) # 348 190 news

# Adding company id
df <- reprisk_news %>%
  left_join(reprisk_company_id, by = c("reprisk_id"))

# 2  ----------------------- only consider severity 3 news
df_sev <- df %>%
  filter(severity == 3)

length(unique(df_sev$reprisk_id)) # 1748 unique firms
length(df_sev$news_date) # 5 798 news

# 3 ------------------------ url available

df_sev_url <- df_sev %>%
  drop_na(url)

length(unique(df_sev_url$reprisk_id)) # 1685 unique firms
length(df_sev_url$news_date) # 5 674 news

# 4 ------------------------ Aggregate to firm-month level
library(lubridate)
df_sev_month <- df_sev_url %>%
  mutate(
    yearmonth = floor_date(news_date,"month")
  ) %>% 
  distinct(reprisk_id, yearmonth, .keep_all = T) 

length(unique(df_sev_month$reprisk_id)) # 1685 unique firms
length(df_sev_month$yearmonth) # 4 661 news

# 5 -------------------------- take out serial offenders
n_events <- df_sev%>%
  group_by(reprisk_id) %>%
  summarise(
    n = n()
  )

offenders <- n_events %>%
  filter(n <= 20) %>%
  pull(reprisk_id)

df_sev_offend <- df_sev_month %>%
  filter(reprisk_id %in% offenders)

length(unique(df_sev_offend$reprisk_id)) # 1652 unique firms
length(df_sev_offend$yearmonth) # 3 837 news




# 4. Requiring minimum 12 months gap between incidents. -------------------

# Filter out observations where news is occurring less than 
# 1 years after the current news for each firm


df_sev_offend_time <- df_sev_offend %>%
  group_by(reprisk_id) %>%
  arrange(yearmonth) %>%
  filter(c(TRUE, diff(yearmonth) >= 365)) %>%
  ungroup()

length(unique(df_sev_offend_time$reprisk_id)) # 1652 unique firms
length(df_sev_offend_time$yearmonth) # 2 690 news

#save(df_sev_offend_time, file = "limited_news.RData")


# 6. ---------------------------- Matching with compustat data

# 1b. Getting CRSP/Compustat
res <- dbSendQuery(wrds, "select datadate, cusip, cik, conm, naicsh, sich, ib, at, tic
                   from comp.funda")
compustat <- dbFetch(res, n=-1) #%>% select(cusip, cik, conm, naics, year1, year2)
dbClearResult(res)
tail(compustat)

compustat <- compustat %>% mutate(year = year(datadate)) %>%
  distinct(cusip, datadate, .keep_all = T)

df_separated <- df_sev_offend_time %>%
  mutate(
    year = year(yearmonth)
  ) %>%
  separate_rows(all_isins) %>%
  mutate(
    cusip = substr(all_isins, 3, 11)
  ) %>%
  select(reprisk_id, cusip, url)

news <- df_sev_offend_time %>%
  select(yearmonth, reprisk_id) %>%
  mutate(
    event = 1
  )

reprisk_comp <- left_join(compustat, 
                          df_separated, 
                          by = c("cusip")) %>%
  drop_na(ib, at, reprisk_id) %>%
  mutate(
    year = year(datadate)
  ) %>%
  distinct(cusip, reprisk_id, year, .keep_all = T) %>%
  select(year, reprisk_id, cusip, conm, naicsh, ib, at, tic, url) 

# filter on news

df_sev_offend_time_comp <- df_sev_offend_time %>%
  filter(reprisk_id %in% reprisk_comp$reprisk_id)

length(unique(df_sev_offend_time_comp$reprisk_id)) # 515 unique firms
length(df_sev_offend_time_comp$yearmonth) # 990 news



# Merging it all together
load("/Users/chresch/Downloads/processed_url.RData")

news <- df_sev_offend_time_comp %>%
  select(yearmonth, reprisk_id, related_issues) %>%
  mutate(
    event = 1
  )


save(df_sev_offend_time_comp, file = "start_firms.RData")










# padding the data
library(padr)
df_temp <- df_processed2 %>%
  group_by(reprisk_id) %>%
  pad() %>%
  ungroup() %>%
  left_join(news,
            by = c("reprisk_id", "yearmonth")) %>%
  mutate(
    event = coalesce(event, 0),
    year = year(yearmonth)
  )  %>%
  left_join(reprisk_comp,
            by = c("reprisk_id", "year"))

df_temp_2 <- df_temp %>%
  group_by(reprisk_id) %>%
  fill(colnames(df_temp), .direction = "down") %>%
  ungroup()


df_temp_2 %>% summarise(across(everything(), ~ sum(is.na(.))))

df_regresjon <- df_temp_2 %>%
  drop_na(ib, at, naicsh)
  
df_regresjon %>% summarise(across(everything(), ~ sum(is.na(.))))

length(unique(df_regresjon$reprisk_id)) # 469 unique firms
sum(df_regresjon$event) # 870 news

save(df_regresjon, file = "regression_start_data.RData")

df_url <- df_sev_offend_time_comp %>%
  select(url, name, reprisk_id) %>%
  distinct(.keep_all = T)

df_url <- df_url %>%
  left_join(
    df_processed2,
    by = c("reprisk_id")
  )

df_url %>% summarise(across(everything(), ~ sum(is.na(.))))

df_remain <- df_url[!complete.cases(df_url), ] %>%
  rename(name = name.x)


df_regresjon %>%
  filter(reprisk_id == "665") %>%
  ggplot() +
  geom_line(aes(yearmonth, sum_E), color = "blue") +
  geom_line(aes(yearmonth, sum_S), color = "red") +
  geom_line(aes(yearmonth, sum_G), color = "green")+
  geom_line(aes(yearmonth, sum_ESG), color = "black")





# Making a plot of the different event types
load("/Users/chresch/Downloads/regression_start_data (1).RData")

df_type <- df_sev_offend_time %>%
  filter(reprisk_id %in% df_regresjon$reprisk_id) %>%
  separate_longer_delim(related_issues, delim = ";") %>%
  select(reprisk_id, related_issues) %>%
  group_by(related_issues) %>%
  summarise(
    n = n()
  ) %>%
  arrange(-n)


ggplot(data=df_type) +
  geom_bar(fill = "lightblue", color = "black", aes(reorder(related_issues, n),n),
           position="dodge",
           stat="identity") +
  coord_flip() +
  labs(x = "ESG events related issues", y = "Number of firm months") +
  theme_bw() +
  theme(strip.background = element_blank(),
        legend.position = "none",
        text = element_text(size = 12,  family = "Times New Roman"))










