# imports ----

library(tidyverse)
library(glue)

# pubdata ----

pubdata_path <- function(path) {
  pubdata_dir <- Sys.getenv("PUBDATA_DIR")
  if (pubdata_dir == "") stop("PUBDATA_DIR environmental variable is not set.")
  file.path(pubdata_dir, path)
}

agcensus <- function(year = NULL) {
  if (is_null(year)) {
    return(
      pubdata_path("agcensus/agcensus.parquet") %>%
        open_dataset(partitioning = "year") %>%
        rename_with(str_to_lower)
    )
  }
  glue("agcensus/agcensus.parquet/{year}/part.pq") %>%
    pubdata_path() %>%
    open_dataset() %>%
    rename_with(str_to_lower)
}

qcew <- function(year = NULL) {
  if (is_null(year)) {
    # default dataset schema is derived from first(?) partition,
    # and disclosure_code incorrectly reads as null type
    # instead, use schema from 2022 where it is correctly detected
    x <- pubdata_path("qcew/qcew.parquet/2022/part.pq") %>%
      arrow::open_dataset()
    sch <- x$schema$fields %>%
      append(arrow::field("year", arrow::int16())) %>%
      arrow::schema()
    return(
      pubdata_path("qcew/qcew.parquet") %>%
        arrow::open_dataset(schema = sch, partitioning = "year")
    )
  }
  glue("qcew/qcew.parquet/{year}/part.pq") %>%
    pubdata_path() %>%
    open_dataset()
}

# farms ----

data <- new.env()

## sales ----

data$farm <- function() {
  
  renames <- tribble(
    ~name,            ~short_desc,
    "sale_tot",       "COMMODITY TOTALS - SALES, MEASURED IN $", 
    "sale_mean",      "COMMODITY TOTALS - SALES, MEASURED IN $ / OPERATION", 
    "n_sale_crop",    "CROP TOTALS - OPERATIONS WITH SALES",
    "sale_crop",      "CROP TOTALS - SALES, MEASURED IN $",
    "n_sale_anim",    "ANIMAL TOTALS, INCL PRODUCTS - OPERATIONS WITH SALES",
    "sale_anim",      "ANIMAL TOTALS, INCL PRODUCTS - SALES, MEASURED IN $",
    "asset_tot",      "AG LAND, INCL BUILDINGS - ASSET VALUE, MEASURED IN $",
    "exp_labhire",    "LABOR, HIRED - EXPENSE, MEASURED IN $",
    "exp_labcont",    "LABOR, CONTRACT - EXPENSE, MEASURED IN $",
    "exp_agser",      "AG SERVICES, CUSTOMWORK - EXPENSE, MEASURED IN $"
  )
  
  agcensus() %>%
    filter(agg_level_desc == "COUNTY", domain_desc == "TOTAL", short_desc %in% renames$short_desc) %>%
    mutate(stcty = paste0(state_fips_code, county_code), .keep = "unused") %>%
    select(year, stcty, short_desc, value) %>%
    collect() %>%
    left_join(renames, "short_desc") %>%
    relocate(year, stcty, name, value, short_desc) %>%
    arrange(year, stcty)
}

# short_desc lookup
if (FALSE) {
  agcensus() %>%
    filter(agg_level_desc == "COUNTY", domain_desc == "TOTAL") %>%
    distinct(short_desc) %>%
    collect() %>%
    filter(str_detect(short_desc, "CUSTOMW"))
}

# agser ----

data$agser <- function() {
  x1 <- qcew() %>%
    filter(
      year %in% seq(2002, 2022, 5),
      agglvl_code == "75",
      industry_code == "115",
      own_code == "5"
    ) %>%
    select(year, area_fips, disclosure_code, annual_avg_estabs, annual_avg_emplvl, total_annual_wages) %>%
    collect()
  x1 %>%
    rename(stcty = area_fips, disc = disclosure_code, est = annual_avg_estabs, emp = annual_avg_emplvl, pay = total_annual_wages)
}
# 
# df = qcew.get_df(r.params['years'], ['year', 'area_fips'], [('agglvl_code', '==', '70')])
# 
# d = qcew.get_df(r.params['years'], 
#                 ['year', 'area_fips', 'disclosure_code', 'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages'],
#                 [('agglvl_code', '==', '75'), ('industry_code', '==', '115'), ('own_code', '==', '5')])
# d = d.rename(columns={'disclosure_code': 'disc', 'annual_avg_estabs': 'est', 'annual_avg_emplvl': 'emp', 'total_annual_wages': 'pay'})
# d['disc'] = d['disc'].replace('N', 'suppressed').fillna('available')
# df = df.merge(d, 'left', ['year', 'area_fips'])
# df['disc'] = df['disc'].fillna('zero')
# df[['est', 'emp', 'pay']] = df[['est', 'emp', 'pay']].fillna(0)
# df['pay'] = nom2real(df['year'], df['pay'])
# 
# d = qcew.get_df(r.params['years'], 
#                 ['year', 'area_fips', 'disclosure_code', 'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages'],
#                 [('agglvl_code', '==', '76'), ('industry_code', '==', '1151'), ('own_code', '==', '5')])
# d = d.rename(columns={'disclosure_code': 'disc1151', 'annual_avg_estabs': 'est1151', 'annual_avg_emplvl': 'emp1151', 'total_annual_wages': 'pay1151'})
# d['disc1151'] = d['disc1151'].replace('N', 'suppressed').fillna('available')
# df = df.merge(d, 'left', ['year', 'area_fips'])
# df['disc1151'] = df['disc1151'].fillna('zero')
# df[['est1151', 'emp1151', 'pay1151']] = df[['est1151', 'emp1151', 'pay1151']].fillna(0)
# df['pay1151'] = nom2real(df['year'], df['pay1151'])
# 
# df = df.rename(columns={'area_fips': 'stcty'})
