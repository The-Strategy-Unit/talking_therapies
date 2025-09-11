#' ----------------------------------------------------------------------------
#' GET MATCHING VARIABLES
#'
#' Load the matching variables parquet file from the DataLake
#'
#' NB, this script should only be run on UDAL as it requires access to the
#' Strategy Unit's Lake Mart
#' ----------------------------------------------------------------------------

# using the `utility_functions.R` function
source(here::here('scripts', 'utility_functions.R'))

# referral-based variables ----------------------------------------------------
# load the matching variables for referrals
df_matching_referrals <-
  get_su_lakemart_parquet_file(
    str_file_pattern = "matching_referrals.parquet/part"
  )

# save the aggregated summary as an RDS file
saveRDS(
  object = df_matching_referrals,
  file = here::here("data", ".secret", "df_matching_referrals.Rds")
)

# contacts-based variables ----------------------------------------------------
# load the matching variables for contacts
df_matching_contacts <-
  get_su_lakemart_parquet_file(
    str_file_pattern = "matching_contacts.parquet/part"
  )

# save the aggregated summary as an RDS file
saveRDS(
  object = df_matching_contacts,
  file = here::here("data", ".secret", "df_matching_contacts.Rds")
)

# other processing ------------------------------------------------------------
# get a lookkup list of ODS codes to providers
df_ods_codes <-
  df_matching_referrals |>
  dplyr::select(ODS_Code, Name) |>
  dplyr::distinct()
