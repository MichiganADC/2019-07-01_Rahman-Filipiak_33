# 2019-07-01_Rahman-Filipiak_33.R


# Libraries ----

# Database
library(DBI)
# Munging
library(dplyr)
library(readr)
library(stringr)


# Config / Helpers ----

source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")


# ******************
# Connect to DB ----

# con <- dbConnect(odbc::odbc(),
#                  "madcbrain PostgreSQL madc_integ", # ~/.odbc.ini
#                  UID = rstudioapi::askForPassword("Username:"),
#                  PWD = rstudioapi::askForPassword("Password:"))
# con <- dbConnect(odbc::odbc(),
#                  "madcbrain pgsql madc_integ", # ~/.odbc.ini
#                  UID = rstudioapi::askForPassword("Username:"),
#                  PWD = rstudioapi::askForPassword("Password:"))
con <- dbConnect(RPostgres::Postgres(),
                 service = "madcbrain pgsql madc_integ", # ~/.pg_service.conf
                 user     = rstudioapi::askForPassword("PostgreSQL Username:"),
                 password = rstudioapi::askForPassword("PostgreSQL Password:"))
# dbListTables(con)


# **********************
# Forms Header / A1 ----

# Fields from UDS 3 Forms Header & A1 (`header_a1`)
# dbListFields(con, "header_a1")

# ptid, form_date
# Age        => dob (calc Age)
# Race       => race
# Gender     => sex
# Handedness => handed
# Education  => educ

df_hd_a1 <-
  dbGetQuery(con, read_file("sql/query_header_a1.sql")) %>% as_tibble()


# ************
# Form D1 ----

# Fields from UDS 3 Form D1 (`d1`)
# dbListFields(con, "d1")

df_d1_ift <-
  dbGetQuery(con, read_file("sql/query_d1.sql")) %>% as_tibble() %>% 
  purrr::map_df(as.character) %>% 
  type_convert()
df_d1 <-
  df_d1_ift %>% 
  coalesce_ift_cols() # helpers.R
df_d1_madc_dx <-
  df_d1 %>% 
  derive_consensus_dx() %>% # helpers.R
  select(ptid, form_date, madc_dx, everything())

rm(df_d1_ift); rm(df_d1)


# *****************
# iPad Toolbox ----

res_iptb <- dbSendQuery(con, read_file("sql/query_iptb.sql")) 
df_iptb  <- dbFetch(res_iptb) %>% as_tibble()
dbClearResult(res_iptb); rm(res_iptb)


# *************
# Form C1L ----

df_c1l <-
  dbGetQuery(con, read_file("sql/query_c1l.sql")) %>% as_tibble()


# ************
# Form C2 ----

# All UDS 3 Neuropsych measures
# NOTE: dbGetQuery doesn't work when there are 100+ returned fields
#       Have to use `dbSendQuery` + `dbFetch` + `dbClearResult`

res_c2_ift <- dbSendQuery(con, read_file("sql/query_c2.sql"))
df_c2_ift  <- dbFetch(res_c2_ift) %>% as_tibble()
dbClearResult(res_c2_ift); rm(res_c2_ift)

df_c2_ift_coerce <- df_c2_ift %>% 
  purrr::map_df(as.character) %>% 
  type_convert()
df_c2 <-
  df_c2_ift_coerce %>% 
  coalesce_ift_cols()

rm(df_c2_ift); rm(df_c2_ift_coerce)


# *************************
# Non-NACC Instruments ----

# HVLT + COWA CFL + Emory WCST + JOLO + WTAR
res_hvlt_cowa_wcst_jolo_wtar <-
  dbSendQuery(con, 
              read_file("sql/query_hvlt_cowa_wcst_jolo_wtar.sql"))
df_hvlt_cowa_wcst_jolo_wtar <-
  dbFetch(res_hvlt_cowa_wcst_jolo_wtar) %>% as_tibble() %>% 
  # Deselect redundant `ptid..X` and `form_date..X` fields
  select_at(.vars = vars(-matches("^ptid..\\d+$|^form_date..\\d+$")))
dbClearResult(res_hvlt_cowa_wcst_jolo_wtar)
rm(res_hvlt_cowa_wcst_jolo_wtar)


# ***********************
# Disconnect from DB ----

dbDisconnect(con)


# ***********************
# Join All UMMAP DFs ----

df_all <-
  df_hd_a1 %>% 
  left_join(df_c1l,        by = c("ptid", "form_date")) %>% 
  left_join(df_c2,         by = c("ptid", "form_date")) %>% 
  left_join(df_d1_madc_dx, by = c("ptid", "form_date")) %>% 
  left_join(df_iptb,       by = c("ptid", "form_date")) %>% 
  left_join(df_hvlt_cowa_wcst_jolo_wtar,
            by = c("ptid", "form_date")) %>% 
  mutate(educ = as.integer(educ)) %>% 
  select(ptid, form_date, madc_dx, everything())


# ***************************
# MiNDSet for Demog Data ----

fields_ms <-
  c(
    "subject_id"
    , "exam_date"
    , "sex_value"
    , "race_value"
    , "handedness"
    , "ed_level"
  ) %>% paste(collapse = ",")

json_ms <-
  get_rc_data_api(uri = REDCAP_API_URI, 
                  token = REDCAP_API_TOKEN_MINDSET,
                  fields = fields_ms,
                  filterLogic = paste0("(",
                                       "[exam_date] >= '2017-03-15'",
                                       " AND ",
                                       "[subject_id] >= 'UM00000001'",
                                       " AND ",
                                       "[subject_id] <= 'UM00009999'",
                                       ")"))
df_ms <-
  json_ms %>% 
  jsonlite::fromJSON() %>% 
  as_tibble() %>% 
  select(-redcap_event_name) %>% 
  select(subject_id, 
         exam_date, 
         race_ms = race_value, 
         sex_ms = sex_value,
         educ_ms = ed_level,
         everything())

df_ms_mut <-
  df_ms %>%
  mutate(exam_date = as.Date(exam_date)) %>% 
  mutate(sex_ms = as.integer(sex_ms)) %>% 
  mutate(educ_ms = as.integer(educ_ms)) %>% 
  mutate(race_ms = case_when(
    race_ms == 1 ~ 1L,
    race_ms == 2 ~ 1L,
    race_ms == 3 ~ 5L,
    race_ms == 5 ~ 50L,
    race_ms == 6 ~ 99L,
    TRUE ~ NA_integer_
  )) %>% 
  mutate(handed_ms = case_when(
    handedness___1 == 1L ~ 2L,
    handedness___2 == 1L ~ 1L,
    handedness___3 == 1L ~ 3L,
    TRUE ~ NA_integer_
  ))

df_all_ms <-
  df_all %>%
  left_join(df_ms_mut, 
            by = c("ptid" = "subject_id", "form_date" = "exam_date")) %>% 
  mutate(race = coalesce(race, race_ms),
         sex = coalesce(sex, sex_ms),
         educ = coalesce(educ, educ_ms),
         handed = coalesce(handed, handed_ms)) %>% 
  select(-ends_with("_ms")) %>% 
  select(-starts_with("handedness___")) %>% 
  propagate_value(ptid, form_date, dob) %>% 
  propagate_value(ptid, form_date, sex) %>%
  propagate_value(ptid, form_date, race) %>% 
  propagate_value(ptid, form_date, educ) %>% 
  propagate_value(ptid, form_date, handed) %>% 
  calculate_age(dob, form_date) %>% select(-dob, -age_years, -age_units) %>% 
  select(ptid, form_date, madc_dx, age_exact, everything())


# *****************
# Write to CSV ----

write_csv(df_all_ms, "2019-07-01_Rahman_Filipiak_33.csv", na = "")

