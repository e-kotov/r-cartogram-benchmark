# targets: quick access --------------------------------------------
# Sys.setenv(TAR_PROJECT = "main")
# targets::tar_visnetwork(label = "branches") # view pipeline
# targets::tar_make() # run pipeline
#
# targets::tar_prune() # delete stored targets that are no longer part of the pipeline
# targets::tar_destroy() # delete saved pipeline outputs to be able to run the pipeline from scratch
# targets::tar_load() # load any pipeline output by name
# x <- targets::tar_read() # read any pipeline output into an R object
# read more at https://docs.ropensci.org/targets/ and https://books.ropensci.org/targets/

library(targets)

tar_option_set(
  format = "qs"
)

options("global.spod_data_dir" = "data/mitms/")
options("global.ncont_show_progress" = TRUE)
options("global.cont_show_progress" = TRUE)

# parameter vectors
sizes <- c(200, 500, 1000, 2000, 3000, 3700)
n_cores <- c(16, 12, 8, 6, 4, 3, 2, 1)
itermax_vals <- c(3, 7, 15)
plans <- c("mirai_multisession", "future_multisession")

list(
  # load & simplify zones once
  tar_target(
    name = zones_full,
    packages = c("spanishoddata", "sf", "dplyr"),
    command = {
      spanishoddata::spod_set_data_dir(getOption("global.spod_data_dir"))
      spanishoddata::spod_get_zones("distr", ver = 2) |>
        dplyr::filter(population > 0) |>
        sf::st_simplify(dTolerance = 200)
    }
  ),

  # non-continuous cartogram ------------------------------------------------

  # Build the full parameter grid as a data.frame
  tar_target(
    name = ncont_speed_params_grid,
    command = expand.grid(
      size = sizes,
      cores = n_cores,
      plan = plans,
      stringsAsFactors = FALSE
    )
  ),

  # Split into a list of 1-row data.frames, one per combo
  tar_target(
    name = ncont_parameters_list,
    command = split(
      ncont_speed_params_grid,
      seq_len(nrow(ncont_speed_params_grid))
    ),
    iteration = "list"
  ),

  # save full bench object for each combo
  tar_target(
    name = ncont_bench_raw,
    packages = c(
      "cartogram",
      "bench",
      "tibble",
      "dplyr",
      "sf",
      "future",
      "future.mirai"
    ),
    command = {
      params <- ncont_parameters_list
      size <- params$size
      cores <- params$cores
      plan <- params$plan

      message(sprintf(
        "Running non-continuous cartogram. size: %d, cores: %d, plan: %s",
        size,
        cores,
        plan
      ))
      z <- dplyr::slice(zones_full, 1L:size)

      if (plan == "mirai_multisession") {
        future::plan(future.mirai::mirai_multisession, workers = cores)
      } else if (plan == "future_multisession") {
        future::plan(future::multisession, workers = cores)
      }

      bm <- bench::mark(
        cartogram::cartogram_ncont(
          x = z,
          weight = "population",
          n_cpu = "respect_future_plan",
          show_progress = getOption("global.ncont_show_progress")
        ),
        iterations = 3L,
        filter_gc = FALSE,
        check = FALSE,
        memory = FALSE,
        time_unit = "s"
      )

      future::plan(future::sequential)

      list(
        size = size,
        cores = cores,
        plan = plan,
        bench = bm
      )
    },
    pattern = map(ncont_parameters_list),
    iteration = "list",
    memory = "transient"
  ),

  # extract just the median times into a summary tibble
  tar_target(
    name = ncont_speed_summary,
    packages = c("tibble", "purrr"),
    command = {
      purrr::map_dfr(
        ncont_bench_raw,
        ~ tibble::tibble(
          size = .x$size,
          cores = .x$cores,
          plan = .x$plan,
          median_s = .x$bench$median
        )
      )
    }
  ),

  # continuous cartogram ---------------------------------------------------

  # Build the full parameter grid as a data.frame
  tar_target(
    name = cont_speed_params_grid,
    command = expand.grid(
      size = sizes,
      cores = n_cores,
      itmax = itermax_vals,
      plan = plans,
      stringsAsFactors = FALSE
    )
  ),

  # Split into a list of 1-row data.frames, one per combo
  tar_target(
    name = cont_parameters_list,
    command = split(
      cont_speed_params_grid,
      seq_len(nrow(cont_speed_params_grid))
    ),
    iteration = "list"
  ),

  # save full bench object for each combo
  tar_target(
    name = cont_bench_raw,
    packages = c(
      "cartogram",
      "bench",
      "tibble",
      "dplyr",
      "sf",
      "future",
      "future.mirai"
    ),
    command = {
      params <- cont_parameters_list
      size <- params$size
      cores <- params$cores
      itmax <- params$itmax
      plan <- params$plan

      message(sprintf(
        "Running continuous cartogram. size: %d, cores: %d, itmax: %d, plan: %s",
        size,
        cores,
        itmax,
        plan
      ))
      z <- dplyr::slice(zones_full, 1L:size)

      if (plan == "mirai_multisession") {
        future::plan(future.mirai::mirai_multisession, workers = cores)
      } else if (plan == "future_multisession") {
        future::plan(future::multisession, workers = cores)
      }

      bm <- bench::mark(
        cartogram::cartogram_cont(
          x = z,
          weight = "population",
          itermax = itmax,
          n_cpu = "respect_future_plan",
          show_progress = getOption("global.cont_show_progress")
        ),
        iterations = 3L,
        filter_gc = FALSE,
        check = FALSE,
        memory = FALSE,
        time_unit = "s"
      )

      future::plan(future::sequential)

      list(
        size = size,
        cores = cores,
        itmax = itmax,
        plan = plan,
        bench = bm
      )
    },
    pattern = map(cont_parameters_list),
    iteration = "list",
    memory = "transient"
  ),

  # extract just the median times into a summary tibble
  tar_target(
    name = cont_speed_summary,
    packages = c("tibble", "purrr"),
    command = {
      purrr::map_dfr(
        cont_bench_raw,
        ~ tibble::tibble(
          size = .x$size,
          cores = .x$cores,
          itmax = .x$itmax,
          plan = .x$plan,
          median_s = .x$bench$median
        )
      )
    }
  )
)
