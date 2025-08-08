#' -----------------------------------------------------------------------------
#' GET MATCHING VARIABLES
#'
#' Load the matching variables parquet file from the DataLake
#'
#' NB, this script should only be run on UDAL as it requires access to the
#' Strategy Unit's Lake Mart
#' -----------------------------------------------------------------------------

# # set path to lake mart
container <- Sys.getenv("container")
lake <- Sys.getenv("lake")
folder <- Sys.getenv("folder")
eval_path <- glue::glue("abfss://{container}@{lake}{folder}")

# SAS formatting
# NB, spaces, underscores and slashes are replaced by dash(-)
# container_sas <- container |> stringr::str_replace_all(pattern = " |_|/", replacement = "-")
# lake_sas <- lake |> stringr::str_replace_all(pattern = " |_|/", replacement = "-")
folder_sas <- folder |> stringr::str_replace_all(pattern = " |_|/", replacement = "-")
eval_path_azurestor <- glue::glue("alp-{container}{folder_sas}r")

# read matching variables
matching_variables <- arrow::read_parquet(file = glue::glue("{eval_path}matching_variables.parquet"))

# packages
# pak::pak("AzureStor")
# pak::pak("AzureKeyVault")
# pak::pak("arrow")

# set up access settings -------------------------------------------------------
# get shared access signature auth for data
vault <- AzureKeyVault::key_vault(Sys.getenv("azure_key_vault"))
# nb, follow any authentication directions in the console.

# read from lake ---------------------------------------------------------------

# get the SAS for the relevant data, r = read access, w = write access
az_sas <- vault$secrets$get(glue::glue("alp-{container}-StrategyUnit-r"))

# set connection string
az_endpoint <- AzureStor::storage_endpoint(glue::glue("https://{lake}"), sas = az_sas$value)

# set the storage container
az_container <- AzureStor::storage_container(az_endpoint, container)

# list files in the project folder
az_files <- AzureStor::list_adls_files(az_container, folder, recursive = TRUE)

# identify parquet files in the matching_variables folder
matching_variables_file <- 
  az_files |> 
  tibble::as_tibble() |> 
  dplyr::filter(stringr::str_detect(string = name, pattern = "matching_variables.parquet/part")) |> 
  dplyr::pull(name)

# read all parquet files to a single df
df_matching <-
  purrr::map_dfr(
    .x = matching_variables_file,
    .f = \(.x) AzureStor::storage_download(container = az_container, src = .x, dest = NULL) |> arrow::read_parquet()
  )
