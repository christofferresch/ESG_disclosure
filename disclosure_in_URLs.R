
########################################
### ESG in URLS: a Novel ESG Measure ###
########################################

setwd()

# load libraries
library(tidyverse)
library(readr)
library(rvest)
library(lubridate)
library(purrr)
library(stringr)
library(tm)
library(rrapply)
library(data.table)
library(dplyr)
library(tidytext)
library(ggplot2)
library(dynlm)
library(stargazer)
library(gdata)
library(slam)
library(parallel)
library(textir)
library(udpipe)
library(readxl)
library(janitor)
library(tidytext)

ESG_words <- read_excel("ESG_wordlist_2.xlsx",
                        sheet = "ESG-Wordlist")
ESG_words$Topic_fix <- ifelse(ESG_words$Topic == "Environmental (Additon LMO 2022)", "Environmental", ESG_words$Topic)

ESG_file <- ESG_words %>%
  group_by(Topic_fix) %>%
  summarise(
    Words = paste0(Word, collapse = ",")
  ) %>%
  filter(!Topic == "Environmental (Additon LMO 2022)")




library(httr)
library(jsonlite)
library(parallel)
library(qdapRegex)
library(tokenizers)


# Define the URL to scrape

load("start_firms.RData")

df_url <- df_sev_offend_time_comp %>%
  distinct(url, .keep_all = T) %>%
  select(url, name, reprisk_id)

base_url <- "https://web.archive.org/cdx/search/cdx"

dict <- paste(ESG_words$Word, collapse = "|")

esg_dict <- ESG_words %>%
  mutate(
    length = nchar(Word)
  ) %>%
  filter(length >= 4)

dict <- paste(esg_dict$Word, collapse = "|")

dict_e <- esg_dict %>%
  filter(Topic_fix == "Environmental") 
dict_e <- paste(dict_e$Word, collapse = "|")

dict_s <- esg_dict %>%
  filter(Topic_fix == "Social") 
dict_s <- paste(dict_s$Word, collapse = "|")

dict_g <- esg_dict %>%
  filter(Topic_fix == "Governance") 
dict_g <- paste(dict_g$Word, collapse = "|")

# url <- "https://www.volkswagenag.com"

df_processed2 <- data.frame()
# Loop through each URL and scrape the data
for (i in 1:length(df_url$url)) {
  # Define the parameters for the request
  params <- list(
    url = paste0(df_url$url[232]),
    matchType = "domain",
    fl = "urlkey,timestamp,original,mimetype",
    collapse = "timestamp:8",
    filter = "mimetype:text/html",
    from = 2007,
    to = 2020,
    limit = 1000000000000000,
    output = "json"
  )
  
  # Send the GET request and parse the response as JSON
  response <- GET(url = base_url, query = params)
  
  data <- rjson::fromJSON(content(response, as = "text"))
  
  df_temp <- as.data.frame(do.call(rbind, data)) %>% row_to_names(1)
  
  df_temp2 <- df_temp %>%
    mutate(
      url = df_url$url[232],
      name = df_url$name[232],
      reprisk_id = df_url$reprisk_id[1],
      date = as.Date(str_sub(timestamp, 0, 8), "%Y%m%d"),
      yearmonth = floor_date(date,"month")) %>%
    mutate(
      nøkkel = paste0(urlkey, "|", yearmonth, "|", original)
    ) %>%
    distinct(nøkkel, .keep_all = T) %>%
    select(url, name, reprisk_id, date, yearmonth, urlkey) %>%
    mutate(
      text = gsub(".*)","",urlkey)
    ) %>%
    select(-urlkey) %>%
    mutate(
      name2 = tolower(name),
      token = paste(tokenize_regex(name2, pattern = "[ ();://]"), collapse = "|"),
      text2 = str_remove_all(text, token)
    )
  
  #,
  #token2 = str_remove(text, token)
  
  timing <- Sys.time()
  df_temp_2 <- df_temp2 %>%
    mutate(
      tok_E = str_detect(text, dict_e),
      tok_S = str_detect(text, dict_s),
      tok_G = str_detect(text, dict_g),
      tok2_E = str_detect(token2, dict_e),
      tok2_S = str_detect(token2, dict_s),
      tok2_G = str_detect(token2, dict_g)
    )
  Sys.time() - timing
  
  df_temp_3 <- df_temp_2 %>%
    mutate(
      dummy_E = ifelse(tok_E == "TRUE", 1, 0),
      dummy_S = ifelse(tok_S == "TRUE", 1, 0),
      dummy_G = ifelse(tok_G == "TRUE", 1, 0),
      dummy_ESG = ifelse(tok_E + tok_S + tok_G >= 1, 1, 0),
      dummy2_E = ifelse(tok2_E == "TRUE", 1, 0),
      dummy2_S = ifelse(tok2_S == "TRUE", 1, 0),
      dummy2_G = ifelse(tok2_G == "TRUE", 1, 0),
      dummy2_ESG = ifelse(tok2_E + tok2_S + tok2_G >= 1, 1, 0)
    )
  
  df_temp_4 <- df_temp_3 %>%
    group_by(reprisk_id, name, yearmonth) %>%
    summarise(
      sum_E = as.numeric(length(dummy_E[dummy_E == 1])),
      sum_S = as.numeric(length(dummy_S[dummy_S == 1])),
      sum_G = as.numeric(length(dummy_G[dummy_G == 1])),
      sum_ESG = as.numeric(length(dummy_ESG[dummy_ESG == 1])),
      sum_ALL = as.numeric(n()),
      sum_0 = as.numeric(sum_ALL - sum_ESG),
      sum2_E = as.numeric(length(dummy2_E[dummy2_E == 1])),
      sum2_S = as.numeric(length(dummy2_S[dummy2_S == 1])),
      sum2_G = as.numeric(length(dummy2_G[dummy2_G == 1])),
      sum2_ESG = as.numeric(length(dummy2_ESG[dummy2_ESG == 1])),
      sum2_ALL = as.numeric(n()),
      sum2_0 = as.numeric(sum2_ALL - sum2_ESG)
    ) %>%
    ungroup() 
  
  colSums(Filter(is.numeric, df_temp_4))
  
  df_processed2 <- rbind(df_processed2, df_temp_4)
}

#12-32

df_wrangle <- df_temp_3 %>%
  select(-date, -tok_E, -tok_S, -tok_G) %>%
  arrange(yearmonth) %>%
  slice(30:50) %>%
  mutate(
    tok_E = str_extract_all(text, dict_e),
    tok_S = str_extract_all(text, dict_s),
    tok_G = str_extract_all(text, dict_g)
  ) 

df_table <- df_wrangle %>% 
  select(-name, -reprisk_id, -yearmonth, -tok_E, -tok_S, -tok_G) %>%
  rename(E = dummy_E,
         S = dummy_S,
         G = dummy_G,
         ESG = dummy_ESG)

kbl(df_table, format = "latex")


df_temp_4 %>%
  slice(75:95) %>%
  kbl(format = "latex")




df_processed2 <- df_processed2 %>%
  distinct()

save(df_processed, df_processed2, file = "processed_url.RData")
load("processed_url.RData")

t <- df_processed2 %>%
  filter(reprisk_id == "86553") %>%
  ggplot() +
  geom_line(aes(yearmonth, sum_E), color = "blue") +
  geom_line(aes(yearmonth, sum_S), color = "red") +
  geom_line(aes(yearmonth, sum_G), color = "green")+
  geom_line(aes(yearmonth, sum_ESG), color = "black")

df_temp_4 %>%
  ggplot() + 
  geom_line(aes(yearmonth, sum_ESG/sum_ALL))

length(unique(df_processed2$reprisk_id))

