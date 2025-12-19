#' Arrow IPC Client Example for CubeSQL
#'
#' This example demonstrates how to connect to CubeSQL with the Arrow IPC output format
#' and read query results using Apache Arrow's IPC streaming format.
#'
#' Arrow IPC (Inter-Process Communication) is a columnar format that provides:
#' - Zero-copy data transfer
#' - Efficient memory usage for large datasets
#' - Native support in data processing libraries (tidyverse, data.table, etc.)
#'
#' Prerequisites:
#'   install.packages(c("RPostgres", "arrow", "tidyverse", "dplyr"))

library(RPostgres)
library(arrow)
library(dplyr)
library(readr)

#' CubeSQL Arrow IPC Client
#'
#' R6 class for connecting to CubeSQL with Arrow IPC output format
#'
#' @examples
#' \dontrun{
#'   client <- CubeSQLArrowIPCClient$new()
#'   client$connect()
#'   client$set_arrow_ipc_output()
#'   results <- client$execute_query("SELECT * FROM information_schema.tables")
#'   client$close()
#' }
#'
#' @export
CubeSQLArrowIPCClient <- R6::R6Class(
  "CubeSQLArrowIPCClient",
  public = list(
    #' @field config PostgreSQL connection configuration
    config = NULL,

    #' @field connection Active database connection
    connection = NULL,

    #' Initialize client with connection parameters
    #'
    #' @param host CubeSQL server hostname (default: "127.0.0.1")
    #' @param port CubeSQL server port (default: 4445)
    #' @param user Database user (default: "username")
    #' @param password Database password (default: "password")
    #' @param dbname Database name (default: "test")
    initialize = function(host = "127.0.0.1", port = 4445L, user = "username",
                          password = "password", dbname = "test") {
      self$config <- list(
        host = host,
        port = port,
        user = user,
        password = password,
        dbname = dbname
      )
      self$connection <- NULL
    },

    #' Connect to CubeSQL server
    connect = function() {
      tryCatch({
        self$connection <- dbConnect(
          RPostgres::Postgres(),
          host = self$config$host,
          port = self$config$port,
          user = self$config$user,
          password = self$config$password,
          dbname = self$config$dbname
        )
        cat(sprintf("Connected to CubeSQL at %s:%d\n",
                    self$config$host, self$config$port))
      }, error = function(e) {
        stop(sprintf("Failed to connect to CubeSQL: %s", e$message))
      })
    },

    #' Enable Arrow IPC output format for this session
    set_arrow_ipc_output = function() {
      tryCatch({
        dbExecute(self$connection, "SET output_format = 'arrow_ipc'")
        cat("Arrow IPC output format enabled for this session\n")
      }, error = function(e) {
        stop(sprintf("Failed to set output format: %s", e$message))
      })
    },

    #' Execute query and return results as tibble
    #'
    #' @param query SQL query to execute
    #'
    #' @return tibble with query results
    execute_query = function(query) {
      tryCatch({
        dbGetQuery(self$connection, query, n = -1)
      }, error = function(e) {
        stop(sprintf("Query execution failed: %s", e$message))
      })
    },

    #' Execute query with chunked processing for large result sets
    #'
    #' @param query SQL query to execute
    #' @param chunk_size Number of rows to fetch at a time (default: 1000)
    #' @param callback Function to call for each chunk
    #'
    #' @return Number of rows processed
    execute_query_chunks = function(query, chunk_size = 1000L, callback = NULL) {
      tryCatch({
        result <- dbSendQuery(self$connection, query)
        row_count <- 0L

        while (!dbHasCompleted(result)) {
          chunk <- dbFetch(result, n = chunk_size)
          row_count <- row_count + nrow(chunk)

          if (!is.null(callback)) {
            callback(chunk, row_count)
          }
        }

        dbClearResult(result)
        row_count
      }, error = function(e) {
        stop(sprintf("Query execution failed: %s", e$message))
      })
    },

    #' Close connection to CubeSQL
    close = function() {
      if (!is.null(self$connection)) {
        dbDisconnect(self$connection)
        cat("Disconnected from CubeSQL\n")
      }
    }
  )
)

#' Example 1: Basic query with Arrow IPC output
#'
#' @export
example_basic_query <- function() {
  cat("\n=== Example 1: Basic Query with Arrow IPC ===\n")

  client <- CubeSQLArrowIPCClient$new()

  tryCatch({
    client$connect()
    client$set_arrow_ipc_output()

    query <- "SELECT * FROM information_schema.tables LIMIT 10"
    results <- client$execute_query(query)

    cat(sprintf("Query: %s\n", query))
    cat(sprintf("Rows returned: %d\n", nrow(results)))
    cat("\nFirst few rows:\n")
    print(head(results, 3))
  }, finally = {
    client$close()
  })
}

#' Example 2: Convert to Arrow Table and manipulate with dplyr
#'
#' @export
example_arrow_manipulation <- function() {
  cat("\n=== Example 2: Arrow Table Manipulation ===\n")

  client <- CubeSQLArrowIPCClient$new()

  tryCatch({
    client$connect()
    client$set_arrow_ipc_output()

    query <- "SELECT * FROM information_schema.columns LIMIT 100"
    results <- client$execute_query(query)

    # Convert to Arrow Table for columnar operations
    arrow_table <- arrow::as_arrow_table(results)

    cat(sprintf("Query: %s\n", query))
    cat(sprintf("Result: Arrow Table with %d rows and %d columns\n",
                nrow(arrow_table), ncol(arrow_table)))
    cat("\nColumn names and types:\n")
    for (i in seq_along(arrow_table$column_names)) {
      col_name <- arrow_table$column_names[[i]]
      col_type <- arrow_table[[col_name]]$type
      cat(sprintf("  %s: %s\n", col_name, col_type))
    }
  }, finally = {
    client$close()
  })
}

#' Example 3: Stream and process large result sets
#'
#' @export
example_stream_results <- function() {
  cat("\n=== Example 3: Stream Large Result Sets ===\n")

  client <- CubeSQLArrowIPCClient$new()

  tryCatch({
    client$connect()
    client$set_arrow_ipc_output()

    query <- "SELECT * FROM information_schema.columns LIMIT 1000"

    total_rows <- client$execute_query_chunks(
      query,
      chunk_size = 100L,
      callback = function(chunk, processed) {
        if (processed %% 100 == 0) {
          cat(sprintf("Processed %d rows...\n", processed))
        }
      }
    )

    cat(sprintf("Total rows processed: %d\n", total_rows))
  }, finally = {
    client$close()
  })
}

#' Example 4: Save results to Parquet format
#'
#' @export
example_save_to_parquet <- function() {
  cat("\n=== Example 4: Save Results to Parquet ===\n")

  client <- CubeSQLArrowIPCClient$new()

  tryCatch({
    client$connect()
    client$set_arrow_ipc_output()

    query <- "SELECT * FROM information_schema.tables LIMIT 100"
    results <- client$execute_query(query)

    # Convert to Arrow Table
    arrow_table <- arrow::as_arrow_table(results)

    # Save to Parquet
    output_file <- "/tmp/cubesql_results.parquet"
    arrow::write_parquet(arrow_table, output_file)

    cat(sprintf("Query: %s\n", query))
    cat(sprintf("Results saved to: %s\n", output_file))

    file_size <- file.size(output_file)
    cat(sprintf("File size: %s bytes\n", format(file_size, big.mark = ",")))
  }, finally = {
    client$close()
  })
}

#' Example 5: Performance comparison
#'
#' @export
example_performance_comparison <- function() {
  cat("\n=== Example 5: Performance Comparison ===\n")

  client <- CubeSQLArrowIPCClient$new()

  tryCatch({
    client$connect()

    test_query <- "SELECT * FROM information_schema.columns LIMIT 1000"

    # Test with PostgreSQL format (default)
    cat("\nTesting with PostgreSQL wire format (default):\n")
    start <- Sys.time()
    results_pg <- client$execute_query(test_query)
    pg_time <- as.numeric(difftime(Sys.time(), start, units = "secs"))
    cat(sprintf("  Rows: %d, Time: %.4f seconds\n", nrow(results_pg), pg_time))

    # Test with Arrow IPC
    cat("\nTesting with Arrow IPC output format:\n")
    client$set_arrow_ipc_output()
    start <- Sys.time()
    results_arrow <- client$execute_query(test_query)
    arrow_time <- as.numeric(difftime(Sys.time(), start, units = "secs"))
    cat(sprintf("  Rows: %d, Time: %.4f seconds\n", nrow(results_arrow), arrow_time))

    # Compare
    if (arrow_time > 0) {
      speedup <- pg_time / arrow_time
      direction <- if (speedup > 1) "faster" else "slower"
      cat(sprintf("\nArrow IPC speedup: %.2fx %s\n", speedup, direction))
    }
  }, finally = {
    client$close()
  })
}

#' Example 6: Data analysis with tidyverse
#'
#' @export
example_tidyverse_analysis <- function() {
  cat("\n=== Example 6: Data Analysis with Tidyverse ===\n")

  client <- CubeSQLArrowIPCClient$new()

  tryCatch({
    client$connect()
    client$set_arrow_ipc_output()

    query <- "SELECT * FROM information_schema.tables LIMIT 200"
    results <- client$execute_query(query)

    cat(sprintf("Query: %s\n", query))
    cat(sprintf("Retrieved %d rows\n\n", nrow(results)))

    # Example dplyr operations
    cat("Sample statistics:\n")
    summary_stats <- results %>%
      dplyr::group_by_all() %>%
      dplyr::count() %>%
      dplyr::slice_head(n = 5)

    print(summary_stats)
  }, finally = {
    client$close()
  })
}

#' Main function to run all examples
#'
#' @export
run_all_examples <- function() {
  cat("CubeSQL Arrow IPC Client Examples\n")
  cat(strrep("=", 50), "\n")

  # Check if required packages are installed
  required_packages <- c("RPostgres", "arrow", "tidyverse", "dplyr", "R6")
  missing_packages <- required_packages[!sapply(required_packages, require,
                                                 character.only = TRUE,
                                                 quietly = TRUE)]

  if (length(missing_packages) > 0) {
    cat("Missing required packages:\n")
    for (pkg in missing_packages) {
      cat(sprintf("  - %s\n", pkg))
    }
    cat("\nInstall with:\n")
    cat(sprintf("  install.packages(c(%s))\n",
                paste(sprintf('"%s"', missing_packages), collapse = ", ")))
    return(invisible(NULL))
  }

  # Check if CubeSQL is running
  tryCatch({
    test_client <- CubeSQLArrowIPCClient$new()
    test_client$connect()
    test_client$close()
  }, error = function(e) {
    cat("Warning: Could not connect to CubeSQL at 127.0.0.1:4444\n")
    cat(sprintf("Error: %s\n\n", e$message))
    cat("To run the examples, start CubeSQL with:\n")
    cat("  CUBESQL_CUBE_URL=... CUBESQL_CUBE_TOKEN=... cargo run --bin cubesqld\n")
    cat("\nOr run individual examples manually after starting CubeSQL.\n")
    return(invisible(NULL))
  })

  # Run examples
  tryCatch({
    example_basic_query()
    example_arrow_manipulation()
    example_stream_results()
    example_save_to_parquet()
    example_performance_comparison()
    example_tidyverse_analysis()
  }, error = function(e) {
    cat(sprintf("Example execution error: %s\n", e$message))
  })
}

# Run if this file is sourced interactively
if (interactive()) {
  cat("Run 'run_all_examples()' to execute all examples\n")
}
