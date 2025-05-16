
#################

check_tables <- function(conn, sql_dialect) {
  # Define the query based on the SQL dialect
  query <- switch(sql_dialect,
                           "snowflake" = paste0("SHOW VIEWS IN SCHEMA ", db_name, ".", omop_schema_name),
                           "mysql" = paste0("SHOW FULL TABLES IN ", db_name, " WHERE TABLE_TYPE LIKE 'VIEW'"),
                           "postgresql" = paste0("SELECT table_name FROM information_schema.views WHERE table_schema = '", omop_schema_name, "'"),
                           "sqlite" = "SELECT name FROM sqlite_master WHERE type='view'",
                           "sqlserver" = paste0("SELECT table_name FROM information_schema.views WHERE table_schema = '", omop_schema_name, "'"),
                           "redshift" = paste0("SELECT table_name FROM information_schema.views WHERE table_schema = '", omop_schema_name, "'"),
                           stop("Unsupported SQL dialect"))
  
  # Execute the query
  tables <- dbGetQuery(conn, query)
  
  # Extract the table names based on the SQL dialect
  table_names <- switch(sql_dialect,
                        "snowflake" = tables$name,
                        "mysql" = tables[[1]],
                        "postgresql" = tables$table_name,
                        "sqlite" = tables$name,
                        "sqlserver" = tables$table_name)
  
  # Check if the tables are present
  episode_present <- "EPISODE" %in% table_names
  episode_event_present <- "EPISODE_EVENT" %in% table_names
  
  # Create the result messages
  episode_message <- paste("episode table is present:", toupper(episode_present))
  episode_event_message <- paste("episode_event table is present:", toupper(episode_event_present))
  
  # Return the messages
  return(list(episode_message, episode_event_message))
}



################ documentation req ###################

get_cdm_details <- function(conn, db_name, omop_schema_name) {
  
  cdm_name <- dbGetQuery(conn, paste0("SELECT CDM_SOURCE_NAME FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_date <- dbGetQuery(conn, paste0("SELECT CDM_RELEASE_DATE FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_info <- dbGetQuery(conn, paste0("SELECT SOURCE_DESCRIPTION FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_vocab <- dbGetQuery(conn, paste0("SELECT VOCABULARY_VERSION FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_desc <- cbind(cdm_name, cdm_date, cdm_info, cdm_vocab)
  
  return(cdm_desc)
}

################ documentation req #################
get_cdm_counts <- function(cdm) {

  tables <- list(
    person = cdm$person,
    care_site = cdm$care_site,
    condition_era = cdm$condition_era,
    condition_occurrence = cdm$condition_occurrence,
    drug_exposure = cdm$drug_exposure,
    cost = cdm$cost,
    death = cdm$death,
    device_exposure = cdm$device_exposure,
    dose_era = cdm$dose_era,
    drug_era = cdm$drug_era,
    location = cdm$location,
    measurement = cdm$measurement,
    note = cdm$note,
    note_nlp = cdm$note_nlp,
    observation = cdm$observation,
    observation_period = cdm$observation_period,
    payer_plan_period = cdm$payer_plan_period,
    procedure_occurrence = cdm$procedure_occurrence,
    provider = cdm$provider,
    specimen = cdm$specimen,
    visit_detail = cdm$visit_detail,
    visit_occurrence = cdm$visit_occurrence,
    fact_relationship = cdm$fact_relationship,
    metadata = cdm$metadata,
    cdm_source = cdm$cdm_source
  )

  if ("episode" %in% names(cdm)) {
    tables$episode <- cdm$episode
  }
  if ("episode_event" %in% names(cdm)) {
    tables$episode_event <- cdm$episode_event
  }
  

  get_counts <- function(df, tablename) {
    if ("person_id" %in% colnames(df)) {
      df %>%
        summarise(
          table = tablename,
          records = n(),
          persons = as.numeric(n_distinct(person_id))
        ) %>%
        collect()
    } else {
      df %>%
        summarise(
          table = tablename,
          records = n(),
          persons = as.numeric(NA_real_)
        ) %>%
        collect()
    }
  }
  

  get_person_ids <- function(df) {
    if ("person_id" %in% colnames(df)) {
      df %>%
        select(person_id) %>%
        distinct() %>%
        collect()
    } else {
      tibble(person_id = integer())
    }
  }
  
  results <- lapply(names(tables), function(name) {
    result <- get_counts(tables[[name]], name)
    as.data.frame(result)
  })
  
  result <- bind_rows(results)
  
  all_person_ids <- bind_rows(lapply(tables, get_person_ids))
  total_persons <- n_distinct(all_person_ids$person_id)
  
  result <- result %>%
    mutate(`person %` = ifelse(is.na(persons), NA, (round((persons / total_persons) * 100, digits = 2))))
  
  result <- result %>%
    rename(
      table = table,
      records = records,
      persons = persons,
      `person %` = `person %`
    )
  
  result <- result %>%
    arrange(desc(records))
  
  return(result)
}


################# documentation re ##################

process_vocab_table <- function(table, source_value_col, concept_id_col, domain_name) {
  table %>%
    mutate(
      source_value = ifelse(is.na(!!sym(source_value_col)), '', !!sym(source_value_col)),
      concept_id = !!sym(concept_id_col),
      is_mapped = ifelse(!!sym(concept_id_col) == 0 | !!sym(concept_id_col) > 2000000000, 0, 1)
    ) %>%
    group_by(!!sym(concept_id_col), !!sym(source_value_col)) %>%
    summarise(
      num_records = n(),
      is_mapped = min(is_mapped),  # Use min() to get the same effect as first()
      .groups = 'drop'
    ) %>%
    ungroup() %>%
    summarise(
      domain = domain_name,
      num_codes_source = n(),
      num_codes_mapped = sum(is_mapped),
      percent_codes_mapped = 100.0 * sum(is_mapped) / n(),
      num_records_source = sum(num_records),
      num_records_mapped = sum(is_mapped * num_records),
      percent_records_mapped = 100.0 * sum(is_mapped * num_records) / sum(num_records)
    ) %>%
    collect()
}

################ documentation req #############


check_omop_variables <- function(cdm, omop_variables_to_check, medoc_concepts) {
  # Create an empty data frame to store results
  results <- data.frame(
    omop_variable = character(),
    table_name = character(),
    medoc_concept = character(),
    is_present = logical(),
    stringsAsFactors = FALSE
  )
  
  # Iterate over each variable to check
  for (variable in omop_variables_to_check) {
    variable_found <- FALSE
    
    # Iterate over each table in the cdm
    for (table_name in names(cdm)) {
      table <- cdm[[table_name]]
      
      if (variable %in% colnames(table)) {
        is_present <- TRUE
        
        # Check if the variable contains any 0 or NA values
        if (all(!is.na(table[[variable]]) & table[[variable]] != 0)) {
          # Check if the variable exists in medoc_concepts
          if (variable %in% names(medoc_concepts)) {
            # Get the corresponding medoc concept
            medoc_concept <- medoc_concepts[[variable]]
            medoc_concept_str <- paste(medoc_concept, collapse = ", ")
          } else {
            medoc_concept_str <- NA
          }
          
          # Add the result to the data frame
          results <- rbind(results, data.frame(
            omop_variable = variable,
            table_name = table_name,
            medoc_concept = medoc_concept_str,
            is_present = TRUE,
            stringsAsFactors = FALSE
          ))
          
          # If medoc_concept is not NA, mark variable as found
          if (!is.na(medoc_concept_str)) {
            variable_found <- TRUE
            break
          }
        }
      } else {
        is_present <- FALSE
      }
      
      # Add the result to the data frame with is_present = FALSE if variable not found
      if (!variable_found && is_present) {
        results <- rbind(results, data.frame(
          omop_variable = variable,
          table_name = table_name,
          medoc_concept = NA,
          is_present = is_present,
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Filter out rows where medoc_concept is NA and is_present is FALSE
  results <- results %>%
    filter(!(is.na(medoc_concept) & !is_present))
  
  # Order by medoc_concept
  results <- results %>%
    arrange(medoc_concept)
  
  return(results)
}


################# documentation re #############

execute_drug_checks <- function(drug_class) {
  drug_codes <- drug_code_list %>% filter(class == drug_class)
  checks <- DrugExposureDiagnostics::executeChecks(
    cdm = cdm,
    ingredients = drug_codes[['concept_id']],
    byConcept = FALSE,
    checks = c("exposureDuration","diagnosticsSummary")
  )
  return(checks$diagnosticsSummary)
}


############### documentation req ################

execute_rt_checks <- function(cdm, radiotherapy_codes_path) {
  radiotherapy_codes <- read.csv(radiotherapy_codes_path)
  
  rt_checks <- cdm$procedure_occurrence %>%
    filter(procedure_concept_id %in% radiotherapy_codes$concept_id) %>%
    left_join(cdm$death, by = "person_id") %>%
    group_by(procedure_concept_id) %>%
    summarise(
      unique_person_count = n_distinct(person_id),
      unique_record_count = n(),
      negative_procedure_days_count = sum(as.integer(procedure_end_date < procedure_date)),
      after_death_count = sum(as.integer(procedure_date > death_date)),
      .groups = 'drop'
    ) %>%
    mutate(
      proportion_of_records_with_negative_procedure_days = paste0(
        negative_procedure_days_count, " (",
        round((negative_procedure_days_count / unique_record_count) * 100, 2),
        "%)"
      ),
      proportion_of_after_death_procedures = paste0(
        after_death_count, " (",
        round((after_death_count / unique_person_count) * 100, 2),
        "%)"
      )
    ) %>%
    ungroup() %>%
    collect() %>%
    left_join(radiotherapy_codes, by = c("procedure_concept_id" = "concept_id")) %>%
    select(name, everything())
  
  return(rt_checks)
}
  

############################## doc req

check_radiation_dose_info <- function(cdm) {

  ids_to_check <- c(40483776, 4155148, 35918606, 46236014)
  
  is_present <- cdm$measurement %>%
    filter(measurement_concept_id %in% ids_to_check) %>%
    summarise(present = n() > 0) %>%
    pull(present)
  
  result <- tibble(
    `radiation dose` = "radiation dose info available?",
    `status` = is_present
  )
  
  return(result)
}




##########################

# check_cancer_codes <- function(df, primary_mets_level, cdm) {
#   df_filtered <- df %>% 
#     filter(primary_mets == primary_mets_level)
#   
#   ids <- df_filtered$id
#   
#   measurement_results <- cdm$measurement %>%
#     inner_join(df_filtered, by = c("measurement_concept_id" = "id")) %>%
#     mutate(variable_name = "measurement_concept_id") %>%
#     select(vocabulary, variable_name) %>%
#     collect()
#   
#   condition_results <- cdm$condition_occurrence %>%
#     filter(condition_concept_id %in% ids) %>%
#     select(condition_concept_id, condition_status_concept_id, condition_type_concept_id) %>%
#     collect() %>%
#     inner_join(df_filtered, by = c("condition_concept_id" = "id")) %>%
#     mutate(
#       variable_name = "condition_concept_id",
#       diagnosis_type = case_when(
#         condition_status_concept_id == 32890 ~ "Admission diagnosis",
#         condition_status_concept_id == 32898 ~ "Postop diagnosis",
#         condition_status_concept_id == 32893 ~ "Confirmed diagnosis",
#         condition_status_concept_id == 32892 ~ "Condition to be diagnosed by procedure",
#         condition_status_concept_id == 32899 ~ "Preliminary diagnosis",
#         TRUE ~ 'type of diagnosis not specified'
#       ),
#       provenance_record = case_when(
#         condition_type_concept_id == 32878 ~ "ENCR - Registry",
#         condition_type_concept_id == 32841 ~ "EHR - radiology report",
#         condition_type_concept_id == 32835 ~ "EHR - pathology report",
#         
#         TRUE ~ 'provenance not specified'
#       )
#     ) %>%
#     select(vocabulary, variable_name, diagnosis_type, provenance_record)
#   
#   result <- bind_rows(measurement_results, condition_results) %>%
#     distinct(vocabulary, variable_name, diagnosis_type, provenance_record)
#   
#   return(result)
# }
# 

###################

  check_tnm <- function(cdm, tnm_codes) {

    measurement_data <- cdm$measurement %>%
      filter(measurement_concept_id %in% tnm_codes$measurement_concept_id | 
               value_as_concept_id %in% tnm_codes$measurement_concept_id) %>%
      collect()
    
    # Join with measurement_concept_id
    result_measurement <- measurement_data %>%
      filter(measurement_concept_id %in% tnm_codes$measurement_concept_id) %>%
      left_join(tnm_codes, by = c("measurement_concept_id" = "measurement_concept_id")) %>%
      select(measurement_concept_id, concept_name) %>%
      distinct() %>%
      mutate(variable = "measurement_concept_id")
    
    # Join with value_as_concept_id
    result_value <- measurement_data %>%
      filter(value_as_concept_id %in% tnm_codes$measurement_concept_id) %>%
      left_join(tnm_codes, by = c("value_as_concept_id" = "measurement_concept_id")) %>%
      select(value_as_concept_id, concept_name) %>%
      distinct() %>%
      mutate(variable = "value_as_concept_id")
    
    # Combine results
    result <- bind_rows(result_measurement, result_value)
    
    return(result)
  }

#####################################

# check_diag_pattern <- function(cdm, cancer_codes) {
#   # Filter condition_occurrence table for relevant condition_concept_ids
#   condition_data <- cdm$condition_occurrence %>%
#     filter(condition_concept_id %in% cancer_codes$id) %>%
#     collect()
#   
#   # Check for multiple visit_occurrence_ids
#   visit_occurrence_check <- condition_data %>%
#     group_by(person_id, condition_start_date) %>%
#     summarise(visit_occurrence_id_count = n_distinct(visit_occurrence_id)) %>%
#     mutate(multiple_visits = visit_occurrence_id_count > 1) %>%
#     collect()
#   
#   # Calculate proportions
#   total_person_ids <- n_distinct(visit_occurrence_check$person_id)
#   proportion_multiple_visits <- (n_distinct(visit_occurrence_check$person_id[visit_occurrence_check$multiple_visits]) / total_person_ids) * 100
#   proportion_single_or_less_visits <- (n_distinct(visit_occurrence_check$person_id[!visit_occurrence_check$multiple_visits]) / total_person_ids) * 100
#   
#   # Format the output as a dataframe
#   result_df <- data.frame(
#     proportion_multiple_visits = proportion_multiple_visits,
#     proportion_single_or_less_visits = proportion_single_or_less_visits
#   )
#   
#   return(result_df)
# }
