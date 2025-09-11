#' ----------------------------------------------------------------------------
#' DATA QUALITY MATURITY INDEX (DQMI)
#'
#' Produce a report from a DQMI .csv file that shows our intervention sites
#' compared with the other providers
#'
#' NOTE:
#' The DQMI data is available from this site:
#' https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/data-quality
#' ----------------------------------------------------------------------------

# list the organisation codes for our intervention projects
intervention_projects <- c(
  "RJ8",
  "RW1",
  "NCH",
  "RDY",
  "NR5",
  "RAT",
  "RWK",
  "RWV"
)

# load the file
df <-
  readr::read_csv(
    file = here::here('data', 'project', 'dqmi_2025_05.csv'),
    col_names = TRUE
  )

# filter and process
df2 <-
  df |>
  # make the fieldnames sensible
  janitor::clean_names() |>
  # limit to the IAPT dataset
  dplyr::filter(dataset == "IAPT") |>
  # limit to required fields
  dplyr::select(
    dplyr::any_of(c(
      "data_provider_code",
      "data_provider_name",
      "org_type",
      "dataset_score"
    ))
  ) |>
  # set one row per provider (ignore item scores)
  dplyr::distinct() |>
  dplyr::mutate(
    # flag our intervention services
    flag_intervention = data_provider_code %in% intervention_projects,
    # make data provider code a factor and order by scores
    data_provider_code_f = data_provider_code |>
      forcats::fct() |>
      forcats::fct_reorder(.x = dataset_score)
  )

# get the average value
mean_dqmi <- mean(df2$dataset_score) |> round(digits = 1)

# display as a chart
df2 |>
  ggplot2::ggplot(
    ggplot2::aes(
      x = dataset_score,
      y = data_provider_code_f,
      fill = flag_intervention
    )
  ) +
  ggplot2::geom_col(width = 1, colour = "#f5f6fa") +
  # add a reference line showing the mean DQMI score
  ggplot2::geom_vline(
    xintercept = mean_dqmi,
    colour = "#5881c1"
  ) +
  ggplot2::annotate(
    geom = "text",
    label = glue::glue("Mean DQMI: {mean_dqmi}%"),
    x = mean_dqmi + 1,
    y = 1,
    angle = -90,
    hjust = 1,
    colour = "#5881c1"
  ) +
  # styling
  ggplot2::theme_minimal(base_size = 16) +
  ggplot2::scale_fill_manual(
    values = list("TRUE" = "#f9bf07", "FALSE" = "#dcdde1")
  ) +
  ggplot2::scale_x_continuous(
    label = scales::label_percent(scale = 1),
    limits = c(0, 100),
    expand = c(0, 0)
  ) +
  ggplot2::labs(
    title = "Data Quality Maturity Index (DQMI) - May 2025",
    subtitle = scales::label_wrap(width = 80)(
      "NHS Talking Therapies services have very high IAPT dataset scores, and intervention services (highlighted) are well above the average"
    ),
    x = "DQMI score (IAPT dataset)",
    y = "Data provider code",
  ) +
  ggplot2::theme(
    legend.position = "none",
    axis.text.y = ggplot2::element_text(size = 7),
    panel.grid.major.y = ggplot2::element_blank()
  )

# save as an image
ggplot2::ggsave(
  filename = here::here('outputs', 'images', 'dqmi_2025_05.svg'),
  width = 21,
  height = 30,
  units = "cm"
)
