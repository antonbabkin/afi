
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


# RDC results ----

get_rdc_results <- function() {
  numeric_cols <- c("year", "nobs", "mean", "sd", "25%", "50%", "75%", "r.squared", "estimate", "std.error", "covariance")
  excel_sheets(ipath$rdc_results) %>%
    set_names() %>%
    map(function(sheet) {
      ipath$rdc_results %>%
        read_excel(sheet = sheet, skip = 4) %>%
        separate_wider_regex(sample_label, c("smp_", commodity = ".*", "_", year = ".*"), cols_remove = FALSE) %>%
        mutate(across(
          any_of(numeric_cols), 
          \(x) as.double(na_if(x, "X"))))
    })
}


