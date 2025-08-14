
# libraries and sources ----

library(tidyverse)
library(arrow)
library(logger)
library(glue)
library(readxl)




# data ----

ppath <- \(...) file.path("projects/aaea-conf-2025-agser", ...)

ipath <- list(
  pubdata = Sys.getenv("PUBDATA_DIR"),
  rdc_dyn = file.path(Sys.getenv("RDC_RESULTS_DIR"), "20191101/results_disc.xlsx"),
  rdc_res_2024 = file.path(Sys.getenv("RDC_RESULTS_DIR"), "20240802.xlsx"),
  price_index = file.path(Sys.getenv("RUREC_DIR"), "data/pubdata/bea_nipa/price_index.pq"),
  # https://www.ers.usda.gov/data-products/agricultural-productivity-in-the-united-states
  # https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/47679/table01.xlsx?v=52122
  tfp = ppath("data/tfp/table01.xlsx")
)

opath <- list(
  paper = ppath("paper.rds")
)

# pubdata ----

pubdata_path <- function(path) {
  if (ipath$pubdata == "") stop("PUBDATA_DIR environmental variable is not set.")
  file.path(ipath$pubdata, path)
}

price_idx_base_year <- 2023
df_price_idx <- ipath$price_index %>%
  read_parquet() %>%
  arrange(year) %>%
  mutate(
    base_idx = first(if_else(year == price_idx_base_year, gdp_price_index, NA), na_rm = TRUE),
    price_idx = gdp_price_index / base_idx) %>%
  select(year, price_idx)

deflate_dollars <- function(year, x, base_year = price_idx_base_year) {
  if (base_year == price_idx_base_year) {
    d <- df_price_idx
  } else {
    d <- df_price_idx %>%
      mutate(
        base_idx = first(if_else(year == base_year, price_idx, NA), na_rm = TRUE),
        price_idx = price_idx / base_idx
      )
  }
  data.frame(year, x) %>%
    left_join(d, "year") %>%
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
  pubdata::path("qcew", "naics_ann_2020") %>%
    dirname() %>%
    open_dataset()
}

# Census of agriculture ----

data_tfp <- function() {
  ipath$tfp %>%
    read_excel(skip = 2, n_max = 74) %>%
    rename(
      year = Year,
      output = "Total agricultural output",
      input = "Farm inputs: Total", 
      tfp = "Total factor productivity (TFP)",
      capital = "Capital inputs: Total",
      labor = "Labor inputs: Total",
      int = "Intermediate inputs: Total",
      int_feedseed = "Intermediate inputs: Feed and seed",
      int_energy = "Intermediate inputs: Energy",
      int_fert = "Intermediate inputs: Fertilizer and lime",
      int_pest = "Intermediate inputs: Pesticides",
      int_serv = "Intermediate inputs: Purchased services",
      int_other = "Intermediate inputs: Other intermediate inputs"
    )
}

data_farm <- function(geo = c("county", "state", "national")) {
  geo <- match.arg(geo)
  
  renames <- tribble(
    ~name,            ~short_desc,
    "sale_tot",        "COMMODITY TOTALS - SALES, MEASURED IN $", 
    "sale_mean",       "COMMODITY TOTALS - SALES, MEASURED IN $ / OPERATION",
    "sale_corn",       "CORN - SALES, MEASURED IN $",
    "sale_soy",        "SOYBEANS - SALES, MEASURED IN $",
    "sale_wheat",      "WHEAT - SALES, MEASURED IN $",
    "sale_veg",        "VEGETABLE TOTALS, INCL SEEDS & TRANSPLANTS, IN THE OPEN - SALES, MEASURED IN $",
    "sale_fruit",      "FRUIT & TREE NUT TOTALS - SALES, MEASURED IN $",
    "sale_hort",       "HORTICULTURE TOTALS, (EXCL CUT TREES & VEGETABLE SEEDS & TRANSPLANTS) - SALES, MEASURED IN $",
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
    "land_veg",        "VEGETABLE TOTALS, IN THE OPEN - ACRES HARVESTED",
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
    select(year, stcty, short_desc, value, value_f) %>%
    collect() %>%
    left_join(renames, "short_desc") %>%
    relocate(year, stcty, name, value, value_f, short_desc) %>%
    mutate(value = if_else(str_detect(name, "^(sale_|exp_|asset_)"), deflate_dollars(year, value), value)) %>%
    arrange(year, stcty)
  
}

# short_desc lookup
if (FALSE) {
  agcensus() %>%
    filter(agg_level_desc == "COUNTY", domain_desc == "TOTAL") %>%
    distinct(short_desc) %>%
    collect() %>%
    filter(str_detect(short_desc, "FRUIT.*ACRE")) %>%
    pull(short_desc)
  
}


# BEA ----

data_io <- function() {
  df <- list()
  # https://www.bea.gov/industry/historical-benchmark-input-output-tables
  # https://apps.bea.gov/industry/zip/2002detail.zip
  d <- read_fwf(
    ppath("data/2002detail/REV_NAICSUseDetail 4-24-08.txt"),
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


data_fa <- function() {
  pubdata::get("bea_fa", "det_nonres_stk-cc") %>%
    filter(year %in% 2002:2022, asset_code %in% c("EO30", "EO21"), ind_code %in% c("110C", "113F")) %>%
    mutate(value = 1e6 * deflate_dollars(year, value)) %>%
    mutate(asset = case_match(asset_code, "EO30" ~ "other ag machine", "EO21" ~ "tractors")) %>%
    mutate(industry = case_match(ind_code, "110C" ~ "farms", "113F" ~ "forest, fish and serv")) %>%
    select(year, industry, asset, value) %>%
    arrange(industry, asset, year) %>%
    mutate(value_norm = value / first(value), .by = c(industry, asset))
}

data_fa2 <- function() {
  pubdata::get("bea_fa", "det_nonres_stk-cc") %>%
    filter(asset_code %in% c("EO30", "EO21"), ind_code %in% c("110C", "113F")) %>%
    mutate(value = 1e6 * deflate_dollars(year, value)) %>%
    mutate(industry = case_match(ind_code, "110C" ~ "Farms", "113F" ~ "Forestry, fishing, and related activities")) %>%
    select(year, industry, asset_code, asset = asset_type, ind_code, industry, value) %>%
    arrange(ind_code, asset_code, year)
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

rdc_res_2019 <- function() {
  d1 <- ipath$rdc_dyn %>%
    read_excel(sheet = "count_by_year") %>%
    mutate(across(everything(), as.integer)) %>%
    select(year = yr, birth = sumu_birth, cont = sumu_cont, death = sumu_predeath, est = s_ptot)
  
  # disclosure was in real 2016 (?) dollars, use deflator to convert to year consistent with other datasets
  price_deflator <- df_price_idx %>%
    filter(year == 2016) %>%
    pull(price_idx)
  d2 <- ipath$rdc_dyn %>%
    read_excel(
      sheet = "emp_pay_wage_agg_by_year", 
      skip = 3,
      col_names = c("year", 
                    "emp1_mean", "emp1_std", "emp1_sum", 
                    "empa_mean", "empa_std", "empa_sum",
                    "p941_mean", "p941_std", "p941_sum",
                    "p943_mean", "p943_std", "p943_sum",
                    "wage_mean", "wage_wmean"),
      .name_repair = "unique_quiet") %>%
    mutate(across(
      c(starts_with("p941"), starts_with("p943"), starts_with("wage")),
      \(x) 1000 * x / price_deflator))
  inner_join(d1, d2, "year")
}

rdc_res_2024 <- function() {
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
