#' -----------------------------------------------------------------------------
#' UPDATE PROJECT PLAN STUFF
#'
#' These are functions to:
#' * Produce up-dated Gantt chart
#' -----------------------------------------------------------------------------

# load utility functions
source(here::here('scripts', 'utility_functions.R'))

#' Load Gantt data
#'
#' Loads the data from the 'gantt' sheet of the project plan Excel file. This
#' contains details of milestones, actions, start, finish and whether the
#' action has been done.
#'
#' @param download_fresh Logical - Set to TRUE to download a fresh copy from the MS Teams site
#'
#' @returns Tibble - processed data from the 'gantt' sheet
load_gantt_data <- function(download_fresh = FALSE, actions = FALSE) {
  # set the location for the copy of the project plan
  str_dest <- here::here('data', 'project', 'tt_proj_plan_copy.xlsx')

  # download a fresh copy from MS Teams
  if (download_fresh) {
    # get the path to the downloaded file
    proj_plan <- download_file_from_channel(
      str_path = "2. Project delivery/Project plan/TT project plan.xlsx",
      str_dest = str_dest
    )
  } else {
    proj_plan <- str_dest
  }

  # read Excel file to df
  df <-
    readxl::read_excel(
      path = proj_plan,
      sheet = "gantt",
      range = "A2:D1000",
      col_types = c("text", "date", "date", "logical")
    ) |>
    # clean names
    janitor::clean_names() |>
    dplyr::rename(actions = milestones_and_actions) |>
    # ensure dates are processed properly
    dplyr::mutate(
      start = start |> as.Date(),
      finish = finish |> as.Date(),
    ) |>
    # keep only rows with data
    dplyr::filter(!is.na(actions)) |>
    # move milestones to their own column
    dplyr::mutate(
      # flag rows where 'milestone' appears in actions
      flag_milestone = actions |>
        stringr::str_to_lower() |>
        stringr::str_detect(pattern = "milestone"),

      # copy these to a new column
      milestone = dplyr::case_when(flag_milestone ~ actions)
    ) |>
    # fill down milestones
    tidyr::fill(milestone, .direction = "down") |>
    # remove rows beginning with 'milestone'
    dplyr::filter(!flag_milestone) |>
    # create a milestone id number
    dplyr::filter(!is.na(milestone)) |>
    dplyr::mutate(milestone_id = dplyr::consecutive_id(milestone)) |>
    # arrange actions by date
    dplyr::arrange(
      start,
      finish,
      .by = milestone
    ) |>
    # create an action id number (per milestone)
    dplyr::mutate(
      action_id = dplyr::row_number(),
      .by = milestone
    ) |>
    # sort the whole table
    dplyr::arrange(milestone_id, action_id) |>
    # update action name
    dplyr::mutate(
      action = glue::glue("{milestone_id}.{action_id} {actions}")
    )
}


#' Generates a Gantt chart using {ggplot2}
#'
#' Returns a ggplot2 chart showing a Gantt plot.
#'
#' @param df Tibble - the data produced by `load_gantt_data()`
#' @param date_units String - the units to use as the x-axis. Choose from `1_weeks` for major breaks at the start of each week and minor breaks for each day, `4_weeks` for major breaks every four weeks with minor breaks each week, and `yearmons` for monthly breaks using the {zoo} package.
#' @param milestones_only Logical - TRUE to summarise df by the milestones, FALSE to show all actions.
#' @param show_days Logical - TRUE to show the duration of each task (in days) as a `geom_text()`
#'
#' @returns ggplot2 object
plot_gantt <- function(
  df,
  date_units = c("1_weeks", "4_weeks", "yearmonths"),
  milestones_only = TRUE,
  show_days = TRUE
) {
  if (milestones_only) {
    # prepare the df for milestones summary
    df <-
      df |>
      dplyr::summarise(
        start = min(as.Date(start), na.rm = TRUE),
        finish = max(as.Date(finish), na.rm = TRUE),
        .by = milestone
      )
  }

  # process the df to work out durations
  df <-
    df |>
    dplyr::mutate(
      #milestone = milestone |> forcats::fct_rev(),
      #actions = actions |> forcats::fct_rev(),
      duration = as.numeric(as.Date(finish) - as.Date(start)) + 1,
      duration_text = glue::glue("{duration} days"),
      duration_mid = start + floor(duration / 2)
    )

  # prepare dates to required format
  if (date_units == "yearmonths") {
    df <-
      df |>
      dplyr::mutate(
        start = start |> zoo::as.yearmon(),
        finish = finish |> zoo::as.yearmon(),
        duration_mid = duration_mid |> zoo::as.yearmon()
      )
  }

  # dplyr::glimpse(df)
  # df |> View()

  # plot
  if (milestones_only) {
    plot <-
      df |>
      dplyr::mutate(milestone = milestone |> forcats::fct_rev()) |>
      ggplot2::ggplot(
        ggplot2::aes(
          x = start,
          xend = finish,
          y = milestone,
          yend = milestone,
          colour = milestone
        )
      )
  } else {
    plot <-
      df |>
      dplyr::mutate(action = action |> forcats::fct_rev()) |>
      ggplot2::ggplot(
        ggplot2::aes(
          x = start,
          xend = finish,
          y = action,
          yend = action,
          colour = milestone
        )
      )
  }

  # add in the gantt lines
  plot <-
    plot +
    ggplot2::geom_segment(linewidth = 10, lineend = "round")

  # add the days text if required
  if (show_days) {
    plot <-
      plot +
      shadowtext::geom_shadowtext(
        ggplot2::aes(
          x = duration_mid,
          label = duration_text,
          fontface = "bold"
        ),
        colour = "white",
        hjust = 0.5,
        family = "Arial Narrow",
        bg.color = adjustcolor(col = "black", alpha.f = 0.05)
      )
  }

  # customise the x-axis units
  if (date_units == "1_weeks") {
    plot <-
      plot +
      ggplot2::scale_x_date(
        date_breaks = "1 week",
        date_labels = "%e %b",
        minor_breaks = "1 day"
      )
  } else if (date_units == "4_weeks") {
    plot <-
      plot +
      ggplot2::scale_x_date(
        date_breaks = "4 weeks",
        date_labels = "%e %b",
        minor_breaks = "1 week"
      )
  } else if (date_units == "yearmonths") {
    plot <-
      plot +
      zoo::scale_x_yearmon(n = 12)
  }

  # add in plot themes
  plot <-
    plot +
    ggplot2::theme_minimal(base_family = "Arial Narrow") +
    ggplot2::theme(
      legend.position = "none",
      axis.title = ggplot2::element_blank(),
      panel.grid.major.y = ggplot2::element_line(linetype = "dotted")
    ) +
    StrategyUnitTheme::scale_colour_su()

  # return the plot
  return(plot)
}

# # get the gantt data
# df <- load_gantt_data(download_fresh = FALSE)

# # show a summary of milestones
# plot_gantt(
#   df = df,
#   date_units = "4_weeks",
#   milestones_only = TRUE,
#   show_days = TRUE
# )

# # show summary for two milestones
# plot_gantt(
#   df = df |>
#     dplyr::filter(
#       milestone %in% c("MILESTONE 1: set-up", "MILESTONE 2: scoping")
#     ),
#   date_units = "1_weeks",
#   milestones_only = TRUE,
#   show_days = FALSE
# )

# # show details for two milestones
# plot_gantt(
#   df = df |>
#     dplyr::filter(
#       milestone %in% c("MILESTONE 1: set-up", "MILESTONE 2: scoping")
#     ),
#   date_units = "1_weeks",
#   milestones_only = FALSE,
#   show_days = TRUE
# )
