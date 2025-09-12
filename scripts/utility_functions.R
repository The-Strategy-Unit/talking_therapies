#' ----------------------------------------------------------------------------
#' UTILITY FUNCTIONS
#'
#' Generalist functions designed to make the analysis a little easier.
#' ----------------------------------------------------------------------------

#' Download a File from the Teams Project Channel
#'
#' Downloads a file from the MS Team channel for this project. NB, this script
#' uses details held in the .Renviron file to identify the team and channel.
#'
#' @param str_path String - a path to the file in the Teams channel SharePoint file system, e.g. "2. Project delivery/Project plan/TT project plan.xlsx"
#' @param str_dest String - a path to where the file should be copied to
#'
#' @returns String - a path to the copied file
download_file_from_channel <- function(str_path, str_dest) {
  # set details for the team and channel
  str_team = Sys.getenv("team")
  str_channel = Sys.getenv("channel")

  # get a reference to the team (NB, requires MS Authentication)
  team <- Microsoft365R::get_team(str_team)

  # get a reference to the channel
  chan <- team$get_channel(str_channel)

  ## download a copy of the file to tempfile
  chan$download_file(
    src = str_path,
    dest = str_dest,
    overwrite = TRUE
  )

  # return the temp path location
  return(str_dest)
}

#' Get Intervention Project IAPT codes
#'
#' There is an Excel file identifying the intervention projects along with
#' their ODS codes. This list is useful in picking out intervention activity
#' from a list of all IAPT-reported data.
#'
#' @returns List - a character list of ODS codes for intervention sites
get_intervention_project_iapt_codes <- function(download_fresh = FALSE) {
  # set the location for the copy of the intervention projects list
  str_dest <- here::here(
    'data',
    'project',
    'tt_intervention_projects_copy.xlsx'
  )

  # download a fresh copy from MS Teams
  if (download_fresh) {
    # get the path to the downloaded file
    proj_plan <- download_file_from_channel(
      str_path = "2. Project delivery/Project plan/TT projects.xlsx",
      str_dest = str_dest
    )
  } else {
    proj_plan <- str_dest
  }

  # read Excel file to df
  df <-
    readxl::read_excel(
      path = proj_plan,
      sheet = "intervention_projects"
    ) |>
    # pull the list of unique ODS project codes as a list
    dplyr::pull(iapt_code) |>
    unique()
}

#project_codes <- get_intervention_project_iapt_codes(download_fresh = TRUE)

#' Get Strategy Unit LakeMart Parquet File
#'
#' Reads and consolidates the individual 'part' files from within a .parquet
#' file on the Strategy Unit LakeMart area of UDAL and returns this data as a
#' single tibble.
#'
#' @param str_file_pattern String - matching part of a parquet file on the SU LakeMart, e.g. 'matching_referrals.parquet/part'
#'
#' @returns Tibble data held in the parquet files
get_su_lakemart_parquet_file <- function(str_file_pattern = "") {
  # ensure a file pattern was supplied
  if (nchar(str_file_pattern) == 0) {
    cli::cli_abort(
      "{.var str_file_pattern} must be a string matching part of a parquet file, e.g. {.val matching_referrals.parquet/part}"
    )
  }

  # set path to lake mart
  container <- Sys.getenv("container")
  lake <- Sys.getenv("lake")
  folder <- Sys.getenv("folder")
  eval_path <- glue::glue("abfss://{container}@{lake}{folder}")

  # set up access settings -------------------------------------------------------
  # get shared access signature auth for data
  vault <- AzureKeyVault::key_vault(Sys.getenv("azure_key_vault"))
  # nb, follow any authentication directions in the console.

  # read from lake ---------------------------------------------------------------

  # get the SAS for the relevant data, r = read access, w = write access
  az_sas <- vault$secrets$get(glue::glue("alp-{container}-StrategyUnit-r"))

  # set connection string
  az_endpoint <- AzureStor::storage_endpoint(
    glue::glue("https://{lake}"),
    sas = az_sas$value
  )

  # set the storage container
  az_container <- AzureStor::storage_container(az_endpoint, container)

  # list files in the project folder
  az_files <- AzureStor::list_adls_files(az_container, folder, recursive = TRUE)

  # identify parquet files in the matching_variables folder
  requested_file <-
    az_files |>
    tibble::as_tibble() |>
    dplyr::filter(stringr::str_detect(
      string = name,
      pattern = str_file_pattern
    )) |>
    dplyr::pull(name)

  # read all parquet files to a single df
  df_return <-
    purrr::map_dfr(
      .x = requested_file,
      .f = \(.x) {
        AzureStor::storage_download(
          container = az_container,
          src = .x,
          dest = NULL
        ) |>
          arrow::read_parquet()
      }
    )

  # return the df
  return(df_return)
}

#' Read an open Excel file
#'
#' Reads an open Excel file to a data frame avoiding issues
#' caused by Excel file locking.
#'
#' @param path File path to the excel file
#' @param sheet String name of the sheet to read (default = "Sheet1")
#'
#' @returns Tibble of data
read_an_open_excel <- function(path, sheet = "Sheet1") {
  # create a temp file reference
  path_xl <- withr::local_tempfile()

  # copy the excel file to the temp location
  fs::file_copy(
    path = path,
    new_path = path_xl,
    overwrite = TRUE
  )

  # read the file
  df <- readxl::read_excel(path = path_xl, sheet = sheet)

  # return the df
  return(df)
}
