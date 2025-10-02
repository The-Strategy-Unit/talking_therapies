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

# project_codes <- get_intervention_project_iapt_codes(download_fresh = TRUE)

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

#' Calculate significance stars
#'
#' @description
#' Calculates signficance stars for a given vector of p-values
#'
#' @param .x Vector of p-values
#'
#' @returns Character vector with stars indicating statistical significance
significance_stars <- function(.x) {
  return_vector <- dplyr::case_when(
    .x <= 0.001 ~ "***",
    .x <= 0.01 ~ "**",
    .x <= 0.05 ~ "*",
    .x <= 0.1 ~ ".",
    .default = ""
  )

  # return(return_vector)
  return(unlist(return_vector))
}

#' Add significance stars to a tibble
#'
#' @description
#' Adds a column to a tibble from the {broom} package to indicate the statistical significance of the p-values.
#'
#' @param df Tibble of data produced by the `tidy()` function from the {broom} package.
#'
#' @returns Tibble of data with a new column called 'significance' added
add_significance_stars <- function(df) {
  df_return <-
    df |>
    dplyr::mutate(significance = p.value |> significance_stars())

  return(df_return)
}

summarise_matching_variable_significance <- function(df, .yearmons, .labels) {
  df <-
    df |>
    labelled::set_variable_labels(.labels = .labels)

  # formula for outcome 1
  formula1 <-
    o1_rate ~
      o1_denom_discharges_count +
        m2_rate +
        m3_rate +
        m4_rate +
        m5_rate +
        m6_rate +
        m7_rate +
        m8_rate +
        m9_rate +
        m10_rate +
        m11_rate +
        m12_rate +
        m13_rate +
        m14_rate +
        m15_rate +
        m16_rate +
        m17_rate

  # formula for outcome 2
  formula2 <-
    o2_rate ~
      o1_denom_discharges_count +
        m2_rate +
        m3_rate +
        m4_rate +
        m5_rate +
        m6_rate +
        m7_rate +
        m8_rate +
        m9_rate +
        m10_rate +
        m11_rate +
        m12_rate +
        m13_rate +
        m14_rate +
        m15_rate +
        m16_rate +
        m17_rate

  # summarise models
  summary_df <-
    purrr::map_dfr(
      .x = .yearmons,
      .f = \(.x) {
        # get the yearmonth (lower and spaces removed)
        .ym <- .x |>
          stringr::str_to_lower() |>
          stringr::str_remove_all(" ") |>
          as.character()

        # outcome 1
        model1 <- lm(
          formula = formula1,
          data = df |> dplyr::filter(calc_month == zoo::as.yearmon(.x))
        ) |>
          broom::tidy() |>
          dplyr::filter(term != "(Intercept)") |>
          dplyr::mutate(outcome = "o1", yearmon = .ym) |>
          add_significance_stars()

        # outcome 2
        model2 <- lm(
          formula = formula2,
          data = df |> dplyr::filter(calc_month == zoo::as.yearmon(.x))
        ) |>
          broom::tidy() |>
          dplyr::filter(term != "(Intercept)") |>
          dplyr::mutate(outcome = "o2", yearmon = .ym) |>
          add_significance_stars()

        # simplify and summarise
        df_return <-
          dplyr::bind_rows(model1, model2) |>
          dplyr::select(term, outcome, yearmon, p.value, significance)
      }
    )

  # process the summary df
  summary_df <-
    summary_df |>
    # pivot the data
    dplyr::select(-p.value) |>
    tidyr::pivot_wider(
      values_from = significance,
      names_from = c(yearmon, outcome)
    ) |>
    # add in variable labels
    dplyr::left_join(
      y = tibble::tibble(
        term = names(.labels),
        description = unlist(.labels)
      ),
      by = dplyr::join_by("term" == "term")
    ) |>
    dplyr::relocate(description, .after = term) |>
    # add an 'overall' indicator for significance
    dplyr::rowwise() |>
    dplyr::mutate(
      overall = paste0(dplyr::c_across(3:dplyr::last_col()), collapse = "")
    ) |>
    dplyr::ungroup()

  return(summary_df)
}

summarise_matching_variable_significance_preintervention <- function(
  df,
  .pre_intervention_period,
  .labels
) {
  df <-
    df |>
    labelled::set_variable_labels(.labels = .labels)

  # formula for outcome 1
  formula1 <-
    o1_rate ~
      o1_denom_discharges_count +
        m2_rate +
        m3_rate +
        m4_rate +
        m5_rate +
        m6_rate +
        m7_rate +
        m8_rate +
        m9_rate +
        m10_rate +
        m11_rate +
        m12_rate +
        m13_rate +
        m14_rate +
        m15_rate +
        m16_rate +
        m17_rate

  # formula for outcome 2
  formula2 <-
    o2_rate ~
      o1_denom_discharges_count +
        m2_rate +
        m3_rate +
        m4_rate +
        m5_rate +
        m6_rate +
        m7_rate +
        m8_rate +
        m9_rate +
        m10_rate +
        m11_rate +
        m12_rate +
        m13_rate +
        m14_rate +
        m15_rate +
        m16_rate +
        m17_rate

  # get a summary df
  average_df <-
    df |>
    # filter for records in the pre-intervention period
    dplyr::filter(
      dplyr::between(
        x = calc_month,
        left = .pre_intervention_period[1],
        right = .pre_intervention_period[2]
      )
    ) |>
    # select fields we are interested in
    dplyr::select(
      c(
        ods_code,
        name,
        dplyr::contains("rate"),
        dplyr::contains("o1_denom")
      )
    ) |>
    # summarise to average values
    dplyr::summarise(
      dplyr::across(
        .cols = dplyr::everything(),
        .fns = ~ mean(., na.rm = TRUE)
      ),
      .by = c(ods_code, name)
    )

  # summarise significance for each of the models
  summary_df <-
    purrr::map_dfr(
      .x = list(formula1, formula2),
      .f = \(.x) {
        # outcome 1
        model1 <- lm(
          formula = .x,
          data = df
        ) |>
          broom::tidy() |>
          dplyr::filter(term != "(Intercept)") |>
          add_significance_stars() |>
          dplyr::mutate(
            outcome = all.vars(.x)[1],
            p.value = scales::pvalue_format()(p.value)
          )
      }
    )

  return(summary_df)
}

summarise_matching_variable_significance_preintervention_panel <- function(
  df,
  .pre_intervention_period,
  .labels
) {
  # formula for outcome 1
  formula1 <-
    o1_rate ~
      o1_denom_discharges_count +
        m2_rate +
        m3_rate +
        m4_rate +
        m5_rate +
        m6_rate +
        m7_rate +
        m8_rate +
        m9_rate +
        m10_rate +
        m11_rate +
        m12_rate +
        m13_rate +
        m14_rate +
        m15_rate +
        m16_rate +
        m17_rate

  # formula for outcome 2
  formula2 <-
    o2_rate ~
      o1_denom_discharges_count +
        m2_rate +
        m3_rate +
        m4_rate +
        m5_rate +
        m6_rate +
        m7_rate +
        m8_rate +
        m9_rate +
        m10_rate +
        m11_rate +
        m12_rate +
        m13_rate +
        m14_rate +
        m15_rate +
        m16_rate +
        m17_rate

  # set up the panel data
  panel_df <-
    df |>
    # filter for records in the pre-intervention period
    dplyr::filter(
      dplyr::between(
        x = calc_month,
        left = .pre_intervention_period[1],
        right = .pre_intervention_period[2]
      )
    ) |>
    # convert to panel data with ods_code as 'individual' and calc_month as 'time' indexes
    plm::pdata.frame(
      index = c("ods_code", "calc_month")
    )

  # summarise significance for each of the models
  summary_df <-
    purrr::map_dfr(
      .x = list(formula1, formula2),
      .f = \(.x) {
        plm::plm(
          formula = .x,
          data = panel_df
        ) |>
          broom::tidy() |>
          dplyr::filter(term != "(Intercept)") |>
          add_significance_stars() |>
          dplyr::mutate(
            outcome = all.vars(.x)[1],
            # p.value = scales::pvalue_format()(p.value)
          )
      }
    )

  # process the summary df
  summary_df <-
    summary_df |>
    # left-join the variable names
    dplyr::left_join(
      y = tibble::tibble(
        term = names(.labels),
        description = unlist(.labels)
      ),
      by = dplyr::join_by("term" == "term")
    ) |>
    # select columns of interest
    dplyr::select(term, description, p.value, significance, outcome) |>
    # pivot wider to put both p.value and significance values on the same row
    tidyr::pivot_wider(
      values_from = c(p.value, significance),
      names_from = outcome,
      names_glue = "{outcome}_{.value}"
    ) |>
    # identify which variables are significant for both outcomes
    dplyr::mutate(
      both_significant = (o1_rate_p.value <= 0.05) & (o2_rate_p.value <= 0.05)
    )

  # process the summary df
  # summary_df <-
  #   summary_df |>
  #   # pivot the data
  #   dplyr::select(-p.value) |>
  #   tidyr::pivot_wider(
  #     values_from = significance,
  #     names_from = c(yearmon, outcome)
  #   ) |>
  #   # add in variable labels
  #   dplyr::left_join(
  #     y = tibble::tibble(
  #       term = names(.labels),
  #       description = unlist(.labels)
  #     ),
  #     by = dplyr::join_by("term" == "term")
  #   ) |>
  #   dplyr::relocate(description, .after = term) |>
  #   # add an 'overall' indicator for significance
  #   dplyr::rowwise() |>
  #   dplyr::mutate(
  #     overall = paste0(dplyr::c_across(3:dplyr::last_col()), collapse = "")
  #   ) |>
  #   dplyr::ungroup()

  return(summary_df)
}

#' Plot outcomes over time
#'
#' @description
#' Create a {ggplot2} plot showing the specified outcome over time
#'
#' @param df Tibble of data from 'df_matching'
#' @param var_outcome String giving the name of the outcome variable to plot
#' @param trendline Boolean (default = FALSE) controls whether to display a trendline for 'var_outcome'
#' @param interventions Boolean (default = FALSE) controls whether to display reference lines and labels for when each intervention was introduced
#' @param title String to use for the plot title
#' @param subtitle String to use for the plot subtitle
#'
#' @returns
#'
#' @export
#' @examples
plot_outcomes_over_time <- function(
  df,
  # org_code,
  df_interventions,
  var_outcome,
  trendline = FALSE,
  interventions = FALSE,
  title = "",
  subtitle = ""
) {
  # get the organisational code from the df
  org_code <-
    df |>
    dplyr::select(ods_code) |>
    dplyr::distinct() |>
    dplyr::slice_head(n = 1) |>
    dplyr::pull()

  # set the title to the org code if title is blank and org_code has a value
  if (title == "" & !is.na(org_code)) {
    title = org_code
  }

  # get the outcome variable as a symbol (for use in ggplot)
  var_outcome_s <- as.symbol(var_outcome)

  # prepare the interventions df
  df_interventions <-
    df_interventions |>
    # limit to the organisation being plotted
    dplyr::filter(iapt_code == org_code) |>
    dplyr::arrange(calc_month) |>
    dplyr::mutate(
      y_val = dplyr::row_number() * 0.05,
      intervention_title = glue::glue("{intervention_title}")
    )

  # start the plot
  p <-
    df |>
    ggplot2::ggplot(
      mapping = ggplot2::aes(
        x = calc_month,
        y = {{ var_outcome_s }}
      )
    )

  # add a trendline if requested
  if (trendline) {
    p <-
      p +
      ggplot2::geom_smooth(
        method = "lm",
        colour = "#F9BF07CC",
        se = FALSE,
        linewidth = 1,
        formula = y ~ x
      )
  }

  # add intervention annotations if requested
  if (interventions) {
    p <-
      p +
      ggplot2::geom_vline(
        data = df_interventions,
        mapping = ggplot2::aes(xintercept = calc_month),
        colour = "#5881c1",
        linetype = "dotted"
      ) +
      ggplot2::geom_label(
        data = df_interventions,
        mapping = ggplot2::aes(
          x = calc_month,
          y = y_val,
          label = intervention_title
        ),
        hjust = 0,
        size = 6,
        colour = "#5881c1"
      )
  }

  # add points and lines
  p <-
    p +
    ggplot2::geom_line() +
    ggplot2::geom_point() +
    # format scales
    ggplot2::scale_y_continuous(
      limits = c(0, NA),
      labels = scales::label_percent(accuracy = 1)
    ) +
    zoo::scale_x_yearmon() +
    ggplot2::theme_minimal(base_size = 20) +
    ggplot2::labs(
      title = title,
      subtitle = subtitle,
      y = stringr::str_wrap(ls_labels[var_outcome], width = 40),
      x = "Month of discharge"
    )

  # return the result
  return(p)
}


#' Prepare the data for matching
#'
#' @description
#' Takes a set of data, 'df_matching', and prepares it for matching using {MatchIt} by
#' averaging pre-intervention data for the matching variables and returning monthly
#' outcome variables for each month specified in `yearmons_matching`
#'
#' @param df Tibble of data based on df_matching
#' @param yearmons_matching Character vector containing the string names of months to match the outcome variables on, e.g. c("Mar 2022", "Aug 2022", "Nov 2022")
#' @param intervention_ods_code Character containing the ODS code for the intervention service
#' @param preintervention_period zoo::as.yearmon vector containing two months to bound the pre-intervention period, e.g. zoo::as.yearmon(c("Jan 2022", "Dec 2022"))
#'
#' @returns Tibble of data ready for the matching process
prepare_df_for_matching <- function(
  df,
  yearmons_matching,
  intervention_ods_code,
  preintervention_period
) {
  # do necessary filtering
  df_matching_prep <-
    df |>
    # keep just the variables of interest
    dplyr::select(
      c(
        ods_code,
        name,
        calc_month,
        dplyr::contains("o1_denom"),
        dplyr::contains("_rate")
      )
    ) |>
    # flag records for the intervention service
    dplyr::mutate(
      flag_intervention = ods_code %in% intervention_ods_code
    ) |>
    dplyr::filter(
      # keep services that have activity up to June 2025
      max(calc_month, na.rm = TRUE) >= zoo::as.yearmon("Jun 2025"),
      # keep services that have activity in each of the matching yearmonths
      all(yearmons_matching %in% calc_month),
      # exclude other services that have implemented an intervention
      (ods_code == intervention_ods_code | flag_intervention == 0),
      .by = ods_code
    ) |>
    # shorten the 'o1_denom' variable name
    dplyr::rename("o1_denom" = o1_denom_discharges_count) |>
    # drop the intervention flag to avoid duplicates later on
    dplyr::select(-flag_intervention)

  # get average values for all 'm' matching vars
  df_matching_average <-
    df_matching_prep |>
    # limit to months in the pre-intervention period
    dplyr::filter(
      dplyr::between(
        x = calc_month,
        left = preintervention_period[1],
        right = preintervention_period[2]
      )
    ) |>
    # average out each of the matching vars
    dplyr::summarise(
      dplyr::across(
        .cols = dplyr::everything(),
        .fns = ~ mean(., na.rm = TRUE)
      ),
      .by = c(ods_code, name)
    ) |>
    # exclucde the outcome variables
    dplyr::select(
      -dplyr::any_of(c("calc_month", "o1_denom", "o1_rate", "o2_rate"))
    )

  # get three months worth of values for all 'o' matching vars
  df_matching_pivot <-
    df_matching_prep |>
    # limit to the matching months
    dplyr::filter(calc_month %in% zoo::as.yearmon(yearmons_matching)) |>
    # order rows by service and month
    dplyr::arrange(ods_code, calc_month) |>
    dplyr::mutate(
      # format the month to be lowercase and without spaces (makes it easier to concatenate with values in the pivot)
      calc_month = calc_month |>
        as.character() |>
        stringr::str_to_lower() |>
        stringr::str_remove_all(" ")
    ) |>
    # select just the columns we are interested in
    dplyr::select(dplyr::any_of(c(
      "ods_code",
      "calc_month",
      "o1_denom",
      "o1_rate",
      "o2_rate"
    ))) |>
    tidyr::pivot_wider(
      # pivot all variables except those in the exception list
      values_from = c(
        dplyr::everything(),
        -dplyr::any_of(c("ods_code", "calc_month"))
      ),
      names_from = c(calc_month)
    ) |>
    # flag the intervention service
    dplyr::mutate(
      flag_intervention = ods_code %in% intervention_ods_code |> as.integer()
    )

  # finally, join together df_matching_average and df_matching_pivot
  df_matching_final <-
    df_matching_average |>
    dplyr::left_join(
      y = df_matching_pivot,
      by = dplyr::join_by("ods_code" == "ods_code")
    )

  # return the value
  return(df_matching_final)
}

#' Prepare the df for SynthDiD
#'
#' @description
#' Prepares a df ready for analysis using {synthdid}.
#'
#' @details
#' Takes the whole df and prepares it ready for {synthdid} analysis by:
#' - simplifying the details to ODS code, month and outcome,
#' - filtering the df to
#'   -  exclude other services that we know have interventions,
#'   -  limit to outcome activity in `yearmon_intervention`,
#'   -  limit to services that have no gaps in activity in `yearmon_intervention`
#'
#' @param df Tibble of data from `get_matching_variables()`
#' @param outcome String - the name of the outcome variable
#' @param yearmon_period Vector of two zoo::yearmon objects defining the start (position 1) and end (position 2) of the period to be used for the evaluation
#' @param yearmon_intervention zoo::yearmon object defining the start of the intervention
#' @param ods_treated String ODS code for the service that received the intervention
#' @param ods_controls String Vector of ODS codes to use as controls. If NULL (default) then it will use all available controls in df
#' @param df_intervention_services Tibble containing details for known TT services that have implemented some form of intervention
#'
#' @returns Tibble of data ready for passing to {synthdid}
prepare_df_for_synthdid <- function(
  df,
  outcome,
  yearmon_period,
  yearmon_intervention,
  ods_treated,
  ods_controls = NA,
  df_intervention_services
) {
  outcome <- as.symbol(outcome)

  # filter the data for just those in `ods_controls` if this has been specified
  if (!is.na(ods_controls[1])) {
    df <-
      df |>
      dplyr::filter(ods_code %in% c(ods_treated, ods_controls))
  }

  df_prep <-
    df |>
    # simplify
    dplyr::select(
      ods_code,
      calc_month,
      o1_rate,
    ) |>
    dplyr::filter(
      # exclude other services that have implemented an intervention
      ods_code == ods_treated |
        !ods_code %in% df_intervention_services$iapt_code,
      # limit activity to within the intervention period
      dplyr::between(
        calc_month,
        left = yearmon_period[1],
        right = yearmon_period[2]
      )
    ) |>
    dplyr::filter(
      # keep services that have activity up to the end of the period
      max(calc_month, na.rm = TRUE) >= yearmon_period[2],
      # keep services that have activity going back to the start of the period
      min(calc_month, na.rm = TRUE) <= yearmon_period[1],
      .by = ods_code
    ) |>
    # transform data
    dplyr::mutate(
      # convert month to number
      month_n = as.numeric(calc_month),
      # flag treatment
      treated = dplyr::if_else(
        condition = ods_code == ods_treated & calc_month > yearmon_intervention,
        true = 1L,
        false = 0L
      )
    ) |>
    # simplify again and put variables in correct order
    dplyr::select(
      ods_code,
      # month_n,
      calc_month,
      o1_rate,
      treated
    ) |>
    # expand so each ods_code has all the month_n values
    tidyr::complete(ods_code, calc_month) |>
    # exclude services that have outcome gaps for any month
    dplyr::filter(
      all(!is.na({{ outcome }})),
      .by = ods_code
    ) |>
    # convert to data.frame (required by synthdid)
    as.data.frame()

  # return the df
  return(df_prep)
}


#' Compare time series data for matched services in the pre-intervention period
#'
#' @description
#' Produces a {ggplot2} plot comparing two services {selected_ods} time-series
#' performance in the pre-intervention {period_preintervention}.
#'
#' @details
#' This is part of the parallel trends assessment - looking to see whether a matched
#' service performed a trajctory to the intervention service.
#'
#' @param df Tibble of data
#' @param selected_ods Character vector listing the ODS codes for the services to be compared
#' @param period_preintervention Vector (length = 2) of zoo::yearmon objects defining the start and end of the pre-intervention period
#' @param trendline Boolean, TRUE = show a trendline, FALSE = don't show a trendline
#'
#' @returns {ggplot2} plot
compare_matches_preintervention <- function(
  df,
  selected_ods,
  period_preintervention,
  trendline = FALSE
) {
  # prepare the data
  df <-
    df |>
    # filter for just our selected services (ODS codes) and the pre-intervention period
    dplyr::filter(
      ods_code %in% selected_ods,
      dplyr::between(
        x = calc_month,
        left = period_preintervention[1],
        right = period_preintervention[2]
      )
    ) |>
    # pivot longer to put different outcomes on their own rows
    tidyr::pivot_longer(
      cols = c(o1_rate, o2_rate),
      names_to = "outcome"
    ) |>
    # make the outcome names a little easier to read
    dplyr::mutate(
      outcome = dplyr::case_match(
        .x = outcome,
        "o1_rate" ~ "Outcome 1",
        "o2_rate" ~ "Outcome 2"
      )
    )

  # start the plot
  p <-
    df |>
    ggplot2::ggplot(
      mapping = ggplot2::aes(
        x = calc_month,
        y = value,
        colour = ods_code
      )
    ) +
    ggplot2::facet_wrap(
      ncol = 1,
      facets = ~outcome
    )

  # add in trendline if requested
  if (trendline) {
    p <-
      p +
      ggplot2::geom_smooth(
        method = "lm",
        se = FALSE,
        linewidth = 1,
        linetype = "dotted",
        formula = y ~ x
      )
  }

  # continue the plot
  p <-
    p +
    ggplot2::geom_line() +
    ggplot2::geom_point() +
    zoo::scale_x_yearmon() +
    ggplot2::scale_y_continuous(
      limits = c(0, NA),
      labels = scales::label_percent(accuracy = 1)
    ) +
    ggplot2::scale_colour_manual(values = c("RWV" = "#f9bf07")) +
    ggplot2::theme_minimal(base_size = 20) +
    ggplot2::labs(title = selected_ods[2]) +
    ggplot2::theme(
      axis.title = ggplot2::element_blank(),
      legend.position = "none"
    )

  # return the plot
  return(p)
}


#' Get matching variables data
#'
#' @description
#' Returns a single tibble containing the exported matching variable summaries
#' which is suitable for use in matching and outcome analysis.
#'
#' @returns Tibble of data for use in the matching and outcome analysis.
#'
get_matching_variables <- function() {
  # reference files -----------------------------------------------------------
  # get a list of projects that have implemneted something to improve adherence
  df_intervention_services <- read_an_open_excel(
    path = here::here('data', 'project', 'tt_intervention_projects_copy.xlsx'),
    sheet = "intervention_projects"
  )

  # get a summary of interventions introduced by projects
  df_interventions <- read_an_open_excel(
    path = here::here('data', 'project', 'tt_interventions_summary.xlsx'),
    sheet = 'interventions'
  ) |>
    # prepare the data
    dplyr::mutate(calc_month = zoo::as.yearmon(month))

  # get a list of TT providers with at least 80% DQMI
  df_dqmi <-
    readr::read_csv(
      file = here::here('data', 'project', 'dqmi_2025_05.csv'),
      show_col_types = FALSE
    ) |>
    # make sure column names are easier to work with
    janitor::clean_names() |>
    # only work with the IAPT dataset
    dplyr::filter(dataset == "IAPT") |>
    # simplify to essential variables
    dplyr::select(
      dplyr::any_of(c(
        "data_provider_code",
        "data_provider_name",
        "org_type",
        "dataset_score"
      ))
    ) |>
    # remove duplicates (i.e. where they reported item data quality)
    dplyr::distinct() |>
    # identify providers with low scores
    dplyr::filter(dataset_score < 80)

  # load ------------------------------------------------------------------------
  # load the data files and calculate rates
  df_matching_referrals <-
    readRDS(
      file = here::here('data', '.secret', 'df_matching_referrals.Rds')
    ) |>
    janitor::clean_names() |>
    dplyr::mutate(
      # calculate the rates for each matching variable and rename to m1_rate, m2_rate, etc.
      dplyr::across(
        .cols = dplyr::starts_with("m"),
        .fns = \(.x) (dplyr::coalesce(.x, 0) / o1_denom_discharges_count),
        .names = "{stringr::str_extract(string = .col, pattern = '^[^_]*_')}rate"
      )
    )

  df_matching_contacts <-
    readRDS(file = here::here('data', '.secret', 'df_matching_contacts.Rds')) |>
    janitor::clean_names() |>
    dplyr::mutate(
      # calculate the rates for each matching variable and rename to m10_rate, m11_rate, etc.
      dplyr::across(
        .cols = dplyr::starts_with("m"),
        .fns = \(.x) (dplyr::coalesce(.x, 0) / contacts_count),
        .names = "{stringr::str_extract(string = .col, pattern = '^[^_]*_')}rate"
      )
    )

  # left-join contacts to referrals to create a single df
  df_matching <-
    df_matching_referrals |>
    dplyr::left_join(
      y = df_matching_contacts |> dplyr::select(-name),
      by = dplyr::join_by(
        "ods_code" == "ods_code",
        "calc_referral_discharged_ym" == "calc_contact_ym"
      )
    ) |>
    # filter for services with at least 80% DQMI
    dplyr::filter(!ods_code %in% df_dqmi$data_provider_code) |>
    dplyr::mutate(
      # services that have implemented an intervention
      flag_intervention = ods_code %in%
        df_intervention_services$iapt_code |>
        as.integer(),

      # time dimension
      calc_month = zoo::as.yearmon(calc_referral_discharged_ym),

      # outcomes
      o1_rate = o1_num_discharges_5_or_more_completed_treatment /
        o1_denom_discharges_count,
      o2_rate = o2_num_discharges_2_to_4_completed_treatment /
        o2_denom_discharges_2_or_more_completed_treatment,
    ) |>
    # put in organisation and date order
    dplyr::arrange(ods_code, calc_month)

  # return the result
  return(df_matching)
}

#' Get a list of variable labels
#'
#' @description
#' Returns a list of variable labels which matches variable name to variable descripion.
#'
#' @returns List
get_variable_labels <- function() {
  ls_labels <- list(
    o1_rate = "O1 Proportion of discharges with 5+ treatment contacts",
    o2_rate = "O2 Proportion of discharges that 'complete a course of treatment' and have between 2 and 4 treatment contacts",
    o1_denom_discharges_count = "M1 Number of discharges for referrals that were seen and taken on for treatment",
    o1_denom = "M1 Number of discharges for referrals that were seen and taken on for treatment",
    m2_rate = "M2 Proportion of discharges for people aged under 26 years at referral",
    m3_rate = "M3 Proportion of discharges for people aged 60 years and older at referral",
    m4_rate = "M4 Proportion of discharges for people whose gender is female",
    m5_rate = "M5 Proportion of discharges for people living in 20% most deprived areas",
    m6_rate = "M6 Proportion of discharges for people living in the 20% least deprived areas",
    m7_rate = "M7 Proportion of discharges for people with a White broad ethnic background",
    m8_rate = "M8 Proportion of discharges for people who had a referral-to-treatment time within six weeks",
    m9_rate = "M9 Proportion of discharges where there was a step-up in therapy",
    m10_rate = "M10 Proportion of contacts where there was a qualified therapist present",
    m11_rate = "M11 Proportion of contacts which were conducted on hospital premises",
    m12_rate = "M12 Proportion of contacts conducted face-to-face",
    m13_rate = "M13 Proportion of contacts conducted outside of weekdays, 9am to 5pm",
    m14_rate = "M14 Proportion of contacts conducted in English",
    m15_rate = "M15 Proportion of contacts with an interpreter present",
    m16_rate = "M16 Proportion of contacts conducted via Internet Enabled Therapy",
    m17_rate = "M17 Proportion of contacts conducted one-to-one"
  )
}

#' Returns a VIF score table for the given matching variables
#'
#' @param matching_vars Character string listing the matching variables to use
#' @param df Tibble of data to run the analysis model on
#'
#' @returns {gt} table showing the results
get_vif_score_table <- function(matching_vars, df) {
  # set the matching formula
  formula_matching <-
    formula(
      paste0(
        "flag_intervention ~ ",
        paste0(
          matching_vars,
          collapse = " + "
        )
      )
    )

  # construct a linear regression model
  fit <- glm(formula = formula_matching, data = df, family = binomial())

  # assess for variance inflation factor / multicollinearity
  vif <- car::vif(fit)

  # display as a table
  t <-
    vif |>
    tibble::as_tibble(rownames = "matching_var") |>
    dplyr::rename("vif" = value) |>
    dplyr::arrange(dplyr::desc(vif)) |>
    gt::gt() |>
    gt::tab_options(quarto.disable_processing = TRUE) |>
    gt::cols_label(
      matching_var = "Matching variable",
      vif = "Variance Inflation Factor (VIF)"
    ) |>
    gt::fmt_number(columns = vif, decimals = 1) |>
    # colour cells based on their value
    gt::data_color(
      columns = vif,
      fn = scales::col_bin(
        palette = c("white", "#fcdf83", "#f5b2aa"),
        bins = c(0, 5, 10, Inf),
        na.color = "transparent"
      ),
      domain = range(vif)
    )

  # return the result
  return(t)
}

#' Plot spaghetti plot
#'
#' @description
#' Plots a spaghetti plot showing timeseries for lots of TT services as
#' grey lines and the intervention service highlighted in orange.
#'
#' @details
#' This plot is helpful to visualise the full timeseries of all services
#' involved in an analysis, to see the post-intervention data as well as
#' the pre-intervention data reviewed for the parallel trends assessments.
#'
#' @param df Tibble - data filtered for the correct time period and limited to required intervention and control services
#' @param str_outcome String - the name of the variable which contains the outcome data
#' @param ods_intervention String - the ODS code of the service which received the intervention - will be highlighted in orange in the plot
#' @param str_title String - the title for the plot
#' @param str_subtitle String - the subtitle for the plot
#' @param zoo_intervention zoo::yearmon - the month when the intervention was introduced
#' @param bool_intervention Boolean, TRUE = display the intervention start month, FALSE do not
#'
#' @returns {ggplot2} object
plot_spaghetti_plot <- function(
  df,
  str_outcome,
  ods_intervention,
  str_title = "",
  str_subtitle = "",
  zoo_intervention,
  bool_intervention = FALSE
) {
  # convert to masked variable
  var_outcome <- as.symbol(str_outcome)

  # create the plot
  p <-
    ggplot2::ggplot()

  # add the intervention line (if requested)
  if (bool_intervention & !is.na(zoo_intervention)) {
    p <-
      p +
      ggplot2::geom_vline(
        xintercept = zoo_intervention,
        linetype = "dotted"
      )
  }

  # continue with the plot
  p <-
    p +
    # add the spaghetti lines
    ggplot2::geom_line(
      data = df,
      mapping = ggplot2::aes(
        x = calc_month,
        y = {{ var_outcome }},
        group = ods_code,
        colour = line_colour
      ),
      linewidth = 0.5
    ) +
    # colour the lines
    ggplot2::scale_colour_identity() +
    # label the lines with the code for the service
    ggrepel::geom_text_repel(
      data = df |> dplyr::slice_max(order_by = calc_month, n = 1),
      mapping = ggplot2::aes(
        x = calc_month,
        y = {{ var_outcome }},
        group = ods_code,
        label = ods_code
      ),
      hjust = "left",
      direction = "y",
      nudge_x = 2 / 12 # move to the right by two months
    ) +
    # plot intervention service trace in orange
    ggplot2::geom_line(
      data = df |> dplyr::filter(ods_code == ods_intervention),
      mapping = ggplot2::aes(
        x = calc_month,
        y = {{ var_outcome }}
      ),
      colour = "#f9bf07",
      linewidth = 2
    ) +
    # adjust the scales
    zoo::scale_x_yearmon(limits = c(NA, zoo::as.yearmon("Aug 2025"))) +
    ggplot2::scale_y_continuous(
      limits = c(0, NA),
      labels = scales::label_percent(accuracy = 1)
    ) +
    # themes
    ggplot2::theme_minimal(base_size = 20) +
    ggplot2::theme(axis.title = ggplot2::element_blank()) +
    ggplot2::labs(
      title = str_title,
      subtitle = stringr::str_wrap(str_subtitle, width = 50)
    )

  # return the plot
  return(p)
}

get_manual_did_estimation <- function() {}
