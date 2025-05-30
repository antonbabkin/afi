
# libraries and sources ----

library(tidyverse)
library(arrow)
library(logger)
library(glue)
library(readxl)




# data ----

ipath <- list(
  pubdata = Sys.getenv("PUBDATA_DIR"),
  rdc_dyn = file.path(Sys.getenv("RDC_RESULTS_DIR"), "20191101/results_disc.xlsx"),
  rdc_res_2024 = file.path(Sys.getenv("RDC_RESULTS_DIR"), "20240802.xlsx"),
  price_index = file.path(Sys.getenv("RUREC_DIR"), "data/pubdata/bea_nipa/price_index.pq")
)

opath <- list(

)

# pubdata ----

pubdata_path <- function(path) {
  if (ipath$pubdata == "") stop("PUBDATA_DIR environmental variable is not set.")
  file.path(ipath$pubdata, path)
}


df_price_idx <- ipath$price_index %>%
  read_parquet() %>%
  arrange(year) %>%
  mutate(price_idx = gdp_price_index / last(gdp_price_index)) %>%
  select(year, price_idx)

deflate_dollars <- function(year, x) {
  data.frame(year, x) %>%
    left_join(df_price_idx, "year") %>%
    mutate(y = x / price_idx) %>%
    pull(y)
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


data_farm <- function(geo = c("county", "state", "national")) {
  geo <- match.arg(geo)
  
  renames <- tribble(
    ~name,            ~short_desc,
    "sale_tot",        "COMMODITY TOTALS - SALES, MEASURED IN $", 
    "sale_mean",       "COMMODITY TOTALS - SALES, MEASURED IN $ / OPERATION", 
    "n_sale_crop",     "CROP TOTALS - OPERATIONS WITH SALES",
    "n_land_crop",     "AG LAND, CROPLAND, HARVESTED - NUMBER OF OPERATIONS",
    "n_land_corn",     "CORN, GRAIN - OPERATIONS WITH AREA HARVESTED",
    "n_land_soy",      "SOYBEANS - OPERATIONS WITH AREA HARVESTED",
    "n_land_wheat",    "WHEAT - OPERATIONS WITH AREA HARVESTED",
    "sale_crop",       "CROP TOTALS - SALES, MEASURED IN $",
    # "land_crop_all",       "AG LAND, CROPLAND - ACRES",
    "land_crop",       "AG LAND, CROPLAND, HARVESTED - ACRES",
    "land_corn",       "CORN, GRAIN - ACRES HARVESTED",
    "land_soy",        "SOYBEANS - ACRES HARVESTED",
    "land_wheat",      "WHEAT - ACRES HARVESTED",
    "prod_corn",       "CORN, GRAIN - PRODUCTION, MEASURED IN BU",
    "prod_soy",        "SOYBEANS - PRODUCTION, MEASURED IN BU",
    "prod_wheat",      "WHEAT - PRODUCTION, MEASURED IN BU",
    "n_sale_anim",     "ANIMAL TOTALS, INCL PRODUCTS - OPERATIONS WITH SALES",
    "sale_anim",       "ANIMAL TOTALS, INCL PRODUCTS - SALES, MEASURED IN $",
    "asset_tot",       "AG LAND, INCL BUILDINGS - ASSET VALUE, MEASURED IN $",
    "asset_mach",      "MACHINERY TOTALS - ASSET VALUE, MEASURED IN $",
    "exp_tot",         "EXPENSE TOTALS, OPERATING - EXPENSE, MEASURED IN $",
    "exp_fert",        "FERTILIZER TOTALS, INCL LIME & SOIL CONDITIONERS - EXPENSE, MEASURED IN $",
    "exp_chem",        "CHEMICAL TOTALS - EXPENSE, MEASURED IN $",
    "exp_seed",        "SEEDS & PLANTS TOTALS - EXPENSE, MEASURED IN $",
    "exp_anim",        "ANIMAL TOTALS - EXPENSE, MEASURED IN $", # Livestock and poultry purchased or leased
    "exp_feed",        "FEED - EXPENSE, MEASURED IN $",
    "exp_fuel",        "FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $", # Gasoline, fuels, and oils purchased
    "exp_rent",        "RENT, CASH, LAND & BUILDINGS - EXPENSE, MEASURED IN $",
    "exp_repair",      "SUPPLIES & REPAIRS, (EXCL LUBRICANTS) - EXPENSE, MEASURED IN $",
    "exp_labhire",     "LABOR, HIRED - EXPENSE, MEASURED IN $",
    "exp_labcont",     "LABOR, CONTRACT - EXPENSE, MEASURED IN $",
    "exp_agser_cust",  "AG SERVICES, CUSTOMWORK - EXPENSE, MEASURED IN $",
    "exp_agser_util",  "AG SERVICES, UTILITIES - EXPENSE, MEASURED IN $",
    "exp_agser_rent",  "AG SERVICES, MACHINERY RENTAL - EXPENSE, MEASURED IN $",
    "exp_agser_anim",  "AG SERVICES, CUSTOM SERVICES FOR LIVESTOCK, INCL MEDICAL SUPPLIES & VETERINARY - EXPENSE, MEASURED IN $",
    "exp_agser_other", "AG SERVICES, OTHER - EXPENSE, MEASURED IN $",
    "exp_int",         "INTEREST - EXPENSE, MEASURED IN $",
    "exp_tax",         "TAXES, PROPERTY, REAL ESTATE & NON-REAL ESTATE, (EXCL PAID BY LANDLORD) - EXPENSE, MEASURED IN $",
  )
  
  agcensus() %>%
    filter(agg_level_desc == toupper(geo), domain_desc == "TOTAL", short_desc %in% renames$short_desc) %>%
    mutate(stcty = paste0(state_fips_code, county_code), .keep = "unused") %>%
    select(year, stcty, short_desc, value) %>%
    collect() %>%
    left_join(renames, "short_desc") %>%
    relocate(year, stcty, name, value, short_desc) %>%
    mutate(value = if_else(str_detect(name, "^(sale_|exp_)"), deflate_dollars(year, value), value)) %>%
    arrange(year, stcty)
  
}

# short_desc lookup
if (FALSE) {
  agcensus() %>%
    filter(agg_level_desc == "COUNTY", domain_desc == "TOTAL") %>%
    distinct(short_desc) %>%
    collect() %>%
    filter(str_detect(short_desc, "MACHINE")) %>%
    pull(short_desc)
  
}


# BEA I-O ----
data_io <- function() {
  df <- list()
  # https://www.bea.gov/industry/historical-benchmark-input-output-tables
  # https://apps.bea.gov/industry/zip/2002detail.zip
  d <- read_fwf(
    "projects/aaea-conf-2025-agser/2002detail/REV_NAICSUseDetail 4-24-08.txt",
    col_positions = fwf_cols(com_code = c(1,6), com_name = c(11, 99), ind_code = c(101, 106), ind_name = c(111, 197), pur_val = c(301, 310)),
    col_types = cols(com_code = "c", com_name = "c", ind_code = "c", ind_name = "c", pur_val = "n"),
    skip = 1,
    n_max = 56759
  )
  
  # total use by industry
  d <- bind_rows(
    d, 
    d %>%
      filter(!(com_code %in% c("V00100", "V00200", "V00300"))) %>%
      summarize(pur_val = sum(pur_val), .by = c(ind_code, ind_name)) %>%
      mutate(com_code = "T005", com_name = "Total Intermediate")
  )
  
  # aggregate codes for consistency with later years
  d <- d %>%
    mutate(
      com_code = case_match(
        com_code,
        c("111335", "1113A0") ~ "111300",
        c("111910", "111920", "1119A0", "1119B0") ~ "111900",
        .default = com_code
      ),
      com_name = case_match(
        com_code,
        "111300" ~ "Fruit and tree nut farming",
        "111900" ~ "Other crop farming",
        .default = com_name
      ),
      ind_code = case_match(
        ind_code,
        c("111335", "1113A0") ~ "111300",
        c("111910", "111920", "1119A0", "1119B0") ~ "111900",
        .default = ind_code
      ),
      ind_name = case_match(
        ind_code,
        "111300" ~ "Fruit and tree nut farming",
        "111900" ~ "Other crop farming",
        .default = ind_name
      )
    ) %>%
    summarize(pur_val = sum(pur_val), .by = c(com_code, com_name, ind_code, ind_name))
  
  df[["2002"]] <- d
  
  df[["2007"]] <- pubdata::get("bea_io", "2023_mu_use-bef-pur_det_2007") %>%
    filter(!is.na(value)) %>%
    select(com_code = row_code, com_name = row_name, ind_code = col_code, ind_name = col_name, pur_val = value)
  df[["2012"]] <- pubdata::get("bea_io", "2023_mu_use-bef-pur_det_2012") %>%
    filter(!is.na(value)) %>%
    select(com_code = row_code, com_name = row_name, ind_code = col_code, ind_name = col_name, pur_val = value)
  df[["2017"]] <- pubdata::get("bea_io", "2023_mu_use-bef-pur_det_2017") %>%
    filter(!is.na(value)) %>%
    select(com_code = row_code, com_name = row_name, ind_code = col_code, ind_name = col_name, pur_val = value)
  
  df <- bind_rows(df, .id = "year") %>%
    mutate(year = as.numeric(year)) %>%
    mutate(pur_val = 1e6 * deflate_dollars(year, pur_val))
  df

}


# QCEW agser ----

data_agser <- function() {
  x1 <- qcew() %>%
    filter(
      year %in% seq(2002, 2022, 5),
      agglvl_code == "76",
      industry_code == "1151",
      own_code == "5"
    ) %>%
    select(year, area_fips, disclosure_code, annual_avg_estabs, annual_avg_emplvl, total_annual_wages) %>%
    collect()
  x1 %>%
    rename(stcty = area_fips, disc = disclosure_code, est = annual_avg_estabs, emp = annual_avg_emplvl, pay = total_annual_wages)
}



# RDC results ----

get_rdc_res_2024 <- function() {
  numeric_cols <- c("year", "nobs", "mean", "sd", "25%", "50%", "75%", "r.squared", "estimate", "std.error", "covariance")
  excel_sheets(ipath$rdc_res_2024) %>%
    set_names() %>%
    map(function(sheet) {
      x <- ipath$rdc_res_2024 %>%
        read_excel(sheet = sheet, skip = 4) %>%
        separate_wider_regex(sample_label, c("smp_", commodity = ".*", "_", year = ".*"), cols_remove = FALSE) %>%
        mutate(across(
          any_of(numeric_cols), 
          \(x) as.double(na_if(x, "X"))))
      class(x) <- c("lmdf", class(x))
      x
    })
}

## modelsummary support ----

glance.lmdf <- function(x, ...) {
  x %>% 
    filter(term == "model_stats") %>% 
    select(any_of("label"), nobs, r.squared)
}

tidy.lmdf <- function(x, ...) {
  conf_p <- 0.05
  z_crit <- qnorm(1 - conf_p / 2)
  x %>%
    filter(term != "model_stats", term != "coef_covar") %>%
    select(any_of("label"), term, est = estimate, std.error, sign, significance) %>%
    mutate(
      estimate = paste0(
        if_else(is.na(est), paste0("[", sign, "]"), as.character(est)), 
        replace_na(significance, "")),
      conf.low = est - z_crit * std.error,
      conf.high = est + z_crit * std.error
    )
}
