# Main Script

# Load necessary libraries
library(tidyverse)
library(data.table)

# Set working directory if needed
#setwd("Code\\pipeline - Marcos\\")

# Function to log messages with timestamps
log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(paste0("[", timestamp, "] ", message, "\n"))
}

# Execute data collection script
log_message("Starting data collection...")
source("data_collection_joinkey.R")
log_message("Data collection completed.")

# Execute data collection script
#log_message("Starting data collection...")
#source("data_collection_raw.R")
#log_message("Data collection completed.")

# Execute data processing script
log_message("Starting data processing...")
source("data_processing.R")
log_message("Data processing completed.")

# Execute models script
log_message("Starting model fitting...")
source("models.R")
log_message("Model fitting completed.")

# Final message
log_message("All scripts executed successfully. Project pipeline complete.")

# Optional: Save the workspace
#save.image("ETF_research_workspace.RData")
#log_message("Workspace saved.")
