
# libraries and sources ----

library(tidyverse)
library(arrow)
library(logger)
library(glue)
library(readxl)




# data ----

ipath <- list(
  pubdata = Sys.getenv("PUBDATA_DIR"),
  rdc_results = file.path(Sys.getenv("RDC_RESULTS_DIR"), "20240802.xlsx")
)

opath <- list(

)

# pubdata ----

pubdata_path <- function(path) {
  if (ipath$pubdata == "") stop("PUBDATA_DIR environmental variable is not set.")
  file.path(ipath$pubdata, path)
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


data_farm <- function() {
  
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
    "land_crop",       "AG LAND, CROPLAND - ACRES",
    "land_corn",       "CORN, GRAIN - ACRES HARVESTED",
    "land_soy",        "SOYBEANS - ACRES HARVESTED",
    "land_wheat",      "WHEAT - ACRES HARVESTED",
    "n_sale_anim",     "ANIMAL TOTALS, INCL PRODUCTS - OPERATIONS WITH SALES",
    "sale_anim",       "ANIMAL TOTALS, INCL PRODUCTS - SALES, MEASURED IN $",
    "asset_tot",       "AG LAND, INCL BUILDINGS - ASSET VALUE, MEASURED IN $",
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
    filter(str_detect(short_desc, "WHEAT.*ACR")) %>%
    pull(short_desc)
  
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

get_rdc_results <- function() {
  numeric_cols <- c("year", "nobs", "mean", "sd", "25%", "50%", "75%", "r.squared", "estimate", "std.error", "covariance")
  excel_sheets(ipath$rdc_results) %>%
    set_names() %>%
    map(function(sheet) {
      x <- ipath$rdc_results %>%
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
