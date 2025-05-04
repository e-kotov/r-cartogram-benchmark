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

options("spod_data_dir" = "~/home/nosync/cache/mitms/")

# parameter vectors
sizes <- c(200, 500, 1000, 2000, 3000, 3900)
# n_cores <- c(16, 12, 8, 6, 4, 3, 2, 1)
n_cores <- c(6, 4, 3, 2, 1)
itermax_vals <- c(3, 7, 15)

list(
  # load & simplify zones once
  tar_target(
    name = zones_full,
    packages = c("spanishoddata", "sf", "dplyr"),
    command = {
      spanishoddata::spod_set_data_dir(getOption("spod_data_dir"))
      spanishoddata::spod_get_zones("distr", ver = 2) |>
        dplyr::filter(population > 0) |>
        sf::st_simplify(dTolerance = 200)
    }
  ),

  # non-continuous cartogram -----------------------------------------------

  # Build the full parameter grid as a data.frame
  tar_target(
    name = ncont_speed_params_grid,
    command = expand.grid(
      size = sizes,
      cores = n_cores,
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

  # Benchmark each combo with bench::mark(), printing status and handling errors
  tar_target(
    name = ncont_bench_result,
    packages = c("cartogram", "bench", "tibble"),
    command = {
      # each ncont_parameters_list element is a 1-row df
      params <- ncont_parameters_list
      size <- params$size
      cores <- params$cores

      # print the current combination
      message(sprintf(
        "Testing ncont combo -> size: %d, cores: %d",
        size,
        cores
      ))

      # slice zones once per combo
      z <- dplyr::slice(zones_full, 1L:size)

      # attempt benchmark and catch errors
      median_time <- tryCatch(
        {
          bm <- bench::mark(
            cartogram::cartogram_cont(
              x = z,
              weight = "population",
              n_cpu = cores,
              show_progress = TRUE
            ),
            iterations = 1L,
            check = FALSE,
            time_unit = "s"
          )
          bm$median
        },
        error = function(e) {
          message(sprintf(
            "Benchmark error for size=%d, cores=%d: %s",
            size,
            cores,
            e$message
          ))
          NA_real_
        }
      )

      # return a one-row tibble, even on error
      tibble::tibble(
        size = size,
        cores = cores,
        median_s = median_time
      )
    },
    pattern = map(ncont_parameters_list),
    iteration = "list",
    memory = "transient"
  ),

  # Collate into one summary table
  tar_target(
    ncont_speed_summary,
    dplyr::bind_rows(ncont_bench_result)
  ),

  # continuous cartogram ---------------------------------------------------

  # # Build the full parameter grid as a data.frame
  # tar_target(
  #   cont_speed_params_grid,
  #   expand.grid(
  #     size = sizes,
  #     cores = n_cores,
  #     itmax = itermax_vals,
  #     stringsAsFactors = FALSE
  #   )
  # ),

  # # Split into a list of 1-row data.frames, one per combo
  # tar_target(
  #   cont_parameters_list,
  #   split(cont_speed_params_grid, seq_len(nrow(cont_speed_params_grid))),
  #   iteration = "list"
  # ),

  # # Benchmark each combo with bench::mark(), printing status and handling errors
  # tar_target(
  #   name = cont_bench_result,
  #   packages = c("cartogram", "bench", "tibble"),
  #   command = {
  #     # each cont_parameters_list element is a 1-row df
  #     params <- cont_parameters_list
  #     size <- params$size
  #     cores <- params$cores
  #     itmax <- params$itmax

  #     # print the current combination
  #     message(sprintf(
  #       "Testing combo -> size: %d, cores: %d, itermax: %d",
  #       size,
  #       cores,
  #       itmax
  #     ))

  #     # slice zones once per combo
  #     z <- dplyr::slice(zones_full, 1L:size)

  #     # attempt benchmark and catch errors
  #     median_time <- tryCatch(
  #       {
  #         bm <- bench::mark(
  #           cartogram::cartogram_cont(
  #             x = z,
  #             weight = "population",
  #             itermax = itmax,
  #             n_cpu = cores,
  #             show_progress = FALSE
  #           ),
  #           iterations = 1L,
  #           check = FALSE,
  #           time_unit = "s"
  #         )
  #         bm$median
  #       },
  #       error = function(e) {
  #         message(sprintf(
  #           "Benchmark error for size=%d, cores=%d, itermax=%d: %s",
  #           size,
  #           cores,
  #           itmax,
  #           e$message
  #         ))
  #         NA_real_
  #       }
  #     )

  #     # return a one-row tibble, even on error
  #     tibble::tibble(
  #       size = size,
  #       cores = cores,
  #       itermax = itmax,
  #       median_s = median_time
  #     )
  #   },
  #   pattern = map(cont_parameters_list),
  #   iteration = "list",
  #   memory = "transient"
  # ),

  # # Collate into one summary table
  # tar_target(
  #   cont_speed_summary,
  #   dplyr::bind_rows(cont_bench_result)
  # )
  NULL
)
