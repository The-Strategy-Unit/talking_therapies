#' -----------------------------------------------------------------------------
#' GET MATCHING VARIABLES
#'
#' Load the matching variables parquet file from the DataLake
#'
#' NB, this script should only be run on UDAL as it requires access to the
#' Strategy Unit's Lake Mart
#' -----------------------------------------------------------------------------

# using the `utility_functions.R` function
source(here::here('scripts', 'utility_functions.R'))

# load the matching variables for referrals
df_matching_referrals <-
  get_su_lakemart_parquet_file(
    str_file_pattern = "matching_referrals.parquet/part"
  )

# get a lookkup list of ODS codes to providers
df_ods_codes <-
  df_matching_referrals |>
  dplyr::select(ODS_Code, Name) |>
  dplyr::distinct()
