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
  str_dest <- here::here('data', 'project', 'tt_intervention_projects_copy.xlsx')

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
    dplyr::pull(iapt_project_code) |> 
    unique()
}

project_codes <- get_intervention_project_iapt_codes(download_fresh = FALSE)
