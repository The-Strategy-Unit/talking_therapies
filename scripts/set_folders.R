#' -----------------------------------------------------------------------------
#' SETUP FOLDER STRUCTURE
#' 
#' This ensures the required folder structure is in place for the analysis
#' to run.
#' -----------------------------------------------------------------------------

# set a function to create a folder if it doesn't already exist
ensure_folder_exists <- function(folder_path) {
  if (!dir.exists(file.path(folder_path))) {
    dir.create(file.path(folder_path))
  }
}

# list required folder paths
required_folders <- c(
  here::here('scripts'),
  here::here('outputs'),
  here::here('data'),
  here::here('data', 'project'),
  here::here('data', 'reference')
)

# iterate over the folders and create where needed
purrr::walk(
  .x = required_folders,
  .f = \(.x) ensure_folder_exists(folder_path = .x)
)
