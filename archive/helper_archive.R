
###############################################################################

#' check_tables
#'
#' @param conn connection to the sql database built in CodeToRun using dbConnect
#' @param sql_dialect assigned in CodeToRun, one of 'snowflake', 'mysql', 'postgresql', 'sqlite', 'sqlserver', 'redshift'
#'
#' @returns results
#' @export
#'
#' @details returns summary of cdm details table including full person count, vocab an cdm versions, 
#' observation period dates and source type sql
#' 
check_tables <- function(conn, sql_dialect) {
  query <- switch(sql_dialect,
                  "snowflake" = paste0("SHOW VIEWS IN SCHEMA ", db_name, ".", omop_schema_name),
                  "mysql" = paste0("SHOW FULL TABLES IN ", db_name, " WHERE TABLE_TYPE LIKE 'VIEW'"),
                  "postgresql" = paste0("SELECT table_name FROM information_schema.views WHERE table_schema = '", omop_schema_name, "'"),
                  "sqlite" = "SELECT name FROM sqlite_master WHERE type='view'",
                  "sqlserver" = paste0("SELECT table_name FROM information_schema.views WHERE table_schema = '", omop_schema_name, "'"),
                  "redshift" = paste0("SELECT table_name FROM information_schema.views WHERE table_schema = '", omop_schema_name, "'"),
                  stop("Unsupported SQL dialect"))
  
  tables <- dbGetQuery(conn, query)
  
  table_names <- switch(sql_dialect,
                        "snowflake" = tables$name,
                        "mysql" = tables[[1]],
                        "postgresql" = tables$table_name,
                        "sqlite" = tables$name,
                        "sqlserver" = tables$table_name,
                        "redshift" = tables$table_name)
  
  table_names_upper <- toupper(table_names)
  
  results <- data.frame(
    Table = c("EPISODE", "EPISODE_EVENT"),
    Present = c("EPISODE" %in% table_names_upper, "EPISODE_EVENT" %in% table_names_upper)
  )
  
  
  return(results)
}


###############################################################################

#' get_cdm_details
#'
#' @param conn connection to the sql database built in CodeToRun using dbConnect
#' @param db_name assigned in CodeToRun.R
#' @param omop_schema_name assigned in CodeToRun.R
#'
#' @returns cdm_desc
#' @export
#'
#' @details provides cdm source information from database

get_cdm_details <- function(conn, db_name, omop_schema_name) {
  
  cdm_name <- dbGetQuery(conn, paste0("SELECT CDM_SOURCE_NAME FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_date <- dbGetQuery(conn, paste0("SELECT CDM_RELEASE_DATE FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_info <- dbGetQuery(conn, paste0("SELECT SOURCE_DESCRIPTION FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_vocab <- dbGetQuery(conn, paste0("SELECT VOCABULARY_VERSION FROM ", db_name, ".", omop_schema_name, ".CDM_SOURCE;")) %>% collect()
  
  cdm_desc <- cbind(cdm_name, cdm_date, cdm_info, cdm_vocab)
  
  return(cdm_desc)
}

###############################################################################
#' get_cdm_counts 
#'
#' @param cdm cdm reference object built in CodeToRun
#'
#' @returns result
#' @export
#'
#' @details counts the unique person ids in each omop table of interest and identifies the
#' presence of episode table

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

###############################################################################


#' check_icod3_matches
#'
#' @param cdm cdm reference object built in CodeToRun, uses cdm$main_cohort which requires 
#' create subsetted cohort to be run in main_onboarding.R
#'
#' @returns bound dataframe 
#' @export
#'
#' @details checks main_cohort person_ids for ICDO3 observation records as per MEDOC 
#' data guide to define MEDOC histological cell type. Calculates % of patients with 
#' histological cell type information. Designed to bind into main medoc_concept_table
#' 

check_icdo3_matches <- function(cdm) {
  cohort_ids <- cdm$main_cohort %>% select(subject_id)
  total <- cohort_ids %>% collect() %>% nrow()
  
  icdo3_concepts <- cdm$concept %>%
    filter(vocabulary_id == "ICDO3") %>%
    select(concept_id)
  
  observation_matches <- cdm$observation %>%
    inner_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
    semi_join(icdo3_concepts, by = c("observation_concept_id" = "concept_id")) %>%
    distinct(person_id) %>%
    collect()
  
  obs_percent <- if (total > 0) round(100 * nrow(observation_matches) / total, 2) else NA
  
  observation_result <- tibble(
    medoc_concept = "histological cell type",
    omop_table = "observation",
    check = "concepts present in omop tables",
    result = !is.na(obs_percent) && obs_percent > 0,
    percent_pass = obs_percent
  )
  
  condition_matches <- cdm$condition_occurrence %>%
    inner_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
    semi_join(icdo3_concepts, by = c("condition_concept_id" = "concept_id")) %>%
    distinct(person_id) %>%
    collect()
  
  cond_percent <- if (total > 0) round(100 * nrow(condition_matches) / total, 2) else NA
  
  condition_result <- tibble(
    medoc_concept = "histological cell type",
    omop_table = "condition_occurrence",
    check = "concepts present in omop tables",
    result = !is.na(cond_percent) && cond_percent > 0,
    percent_pass = cond_percent
  )
  
  bind_rows(observation_result, condition_result)
}




###############################################################################


#' evaluate_concept
#'
#' @param concept individual concept to check or list of concepts from lookup
#' @param visited empty character vector to track whether the concept has already been 
#' checked in the function - if true, there is a warning for dependency, due to the dependencies
#' of some concepts on each other
#' 
#' Depends on the generation of cdm$main_cohort
#'
#' @returns tibble
#' @export
#'
#' @details uses lookup to check through all medoc concepts provided, assigning the correct check 
#' either checking whether the concepts required to define the variable are present and 
#' calculates % of cancer subcohort, OR check whether the concept is present in the omop database
#' Concepts where information is provided from other functions such as radiotherapy or drugs
#' are not summarised in this table but referred to later in the document
#' 

evaluate_concept <- function(concept, visited = character()) {
  if (concept %in% visited) {
    warning(paste("Circular dependency detected for:", concept))
    return(tibble(medoc_concept = concept, omop_table = NA, check = "circular dependency", result = NA, percent_pass = NA))
  }
  
  rows <- lookup %>% filter(medoc_concept == concept)
  if (nrow(rows) == 0) {
    return(tibble(medoc_concept = concept, omop_table = NA, check = NA, result = NA, percent_pass = NA))
  }
  
  check_type <- rows$check[!is.na(rows$check)][1]
  omop_table <- rows$omop_table[!is.na(rows$omop_table)][1]
  filter_level <- rows$filter_level[!is.na(rows$filter_level)][1]
  concept_set <- rows$concept_set[!is.na(rows$concept_set)][1]
  
  result <- FALSE
  percent_pass <- NA
  check_label <- NA
  
  visited <- c(visited, concept)
  cohort_ids <- cdm$main_cohort %>% select(subject_id)
  total <- cohort_ids %>% collect() %>% nrow()
  
  if (!is.na(filter_level)) {
    filter_result <- evaluate_concept(filter_level, visited)
    if (!isTRUE(filter_result$result[1])) {
      return(tibble(
        medoc_concept = concept,
        omop_table = omop_table,
        check = paste0("filtered by ", filter_level),
        result = FALSE,
        percent_pass = 0
      ))
    }
  }
  
  if (!is.na(omop_table) && (startsWith(omop_table, "see_") || startsWith(omop_table, "derived"))) {
    return(tibble(medoc_concept = concept, omop_table = omop_table, check = NA, result = NA, percent_pass = NA))
  }
  
  if (!is.na(check_type) && check_type == "present") {
    check_label <- "variable is present in cdm"
    present_rows <- rows %>% filter(check == "present", !is.na(omop_variable))
    
    if (concept == "biomarker_measure") {
      biomarker_name_rows <- lookup %>% filter(medoc_concept == "biomarker_name", check == "concept")
      biomarker_var <- biomarker_name_rows$omop_concept_variable[!is.na(biomarker_name_rows$omop_concept_variable)][1]
      concept_codes <- genomic_codes
      
      joined <- cdm[[omop_table]] %>%
        inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
        filter(measurement_concept_id %in% concept_codes) %>% collect()
      
      passed <- joined %>%
        filter(if_any(all_of(present_rows$omop_variable), ~ !is.na(.))) %>%
        distinct(person_id) %>%
        nrow()
      
      result <- passed > 0
      percent_pass <- if (total > 0) round(100 * passed / total, 2) else NA
    } else {
      if (any(present_rows$omop_variable == "omop_table")) {
        result <- omop_table %in% names(cdm)
        percent_pass <- if (result) 100 else 0
      } else if (!is.null(cdm[[omop_table]]) &&
                 all(present_rows$omop_variable %in% colnames(cdm[[omop_table]]))) {
        joined <- cdm[[omop_table]] %>%
          inner_join(cohort_ids, by = c("person_id" = "subject_id"))
        
        passed <- joined %>%
          filter(if_any(all_of(present_rows$omop_variable), ~ !is.na(.))) %>%
          distinct(person_id) %>%
          collect() %>%
          nrow()
        
        result <- passed > 0
        percent_pass <- if (total > 0) round(100 * passed / total, 2) else NA
      }
    }
  } else if (!is.na(check_type) && check_type == "concept") {
    check_label <- "concepts present in omop tables"
    
    concept_codes <- if (!is.na(concept_set) && concept_set == "genomic_codes") {
      genomic_codes
    } else if (!is.na(concept_set) && concept_set == "tumour_stage_codes") {
      tumour_stage_codes
    } else if (!is.na(concept_set) && concept_set %in% names(medoc_concept_codes)) {
      medoc_concept_codes[[concept_set]]
    } else {
      NULL
    }
    
    concept_var <- rows$omop_concept_variable[!is.na(rows$omop_concept_variable)][1]
    
    if (!is.null(concept_codes) &&
        !is.null(cdm[[omop_table]]) &&
        concept_var %in% colnames(cdm[[omop_table]])) {
      joined <- cdm[[omop_table]] %>%
        inner_join(cohort_ids, by = c("person_id" = "subject_id"))
      
      passed <- joined %>%
        filter(.data[[concept_var]] %in% concept_codes) %>%
        distinct(person_id) %>%
        collect() %>%
        nrow()
      
      result <- passed > 0
      percent_pass <- if (total > 0) round(100 * passed / total, 2) else NA
    }
  }
  
  if (!is.na(percent_pass) && percent_pass == 0) {
    result <- FALSE
  }
  
  tibble(
    medoc_concept = concept,
    omop_table = omop_table,
    check = check_label,
    result = result,
    percent_pass = percent_pass
  )
}



###############################################################################

#' Post-process concept table
#'
#' @param medoc_concept_table generated in evaluate_concept
#'
#' @returns summary table
#' @export
#'
#' @details processes the medoc_concept_table to ensure formatting is consistent and
#' make ammendments for any non sensical results for dependent concepts such as 
#' metastasis location and presence


postprocess_concept_table <- function(medoc_concept_table) {
  summary_table <- medoc_concept_table %>%
    group_by(medoc_concept) %>%
    summarise(
      result = any(result %in% TRUE, na.rm = TRUE),
      percent_pass = if (all(is.na(percent_pass))) NA_real_ else max(percent_pass, na.rm = TRUE),
      omop_table = first(na.omit(omop_table)),
      check = first(na.omit(check)),
      .groups = "drop"
    ) %>%
    mutate(percent_pass = ifelse(is.infinite(percent_pass), NA, percent_pass))
  
  all_concepts <- tibble(medoc_concept = unique(lookup$medoc_concept))
  summary_table <- all_concepts %>%
    left_join(summary_table, by = "medoc_concept")
  
  location_row <- summary_table %>% filter(medoc_concept == "metastasis_location")
  presence_row <- summary_table %>% filter(medoc_concept == "metastasis_presence")
  
  if (nrow(location_row) == 1 && nrow(presence_row) == 1) {
    combined_percent <- sum(
      c(location_row$percent_pass, presence_row$percent_pass),
      na.rm = TRUE
    )
    combined_percent <- min(combined_percent, 100)
    
    summary_table <- summary_table %>%
      mutate(
        percent_pass = ifelse(
          medoc_concept == "metastasis_presence",
          combined_percent,
          percent_pass
        ),
        result = ifelse(
          medoc_concept == "metastasis_presence" & combined_percent > 0,
          TRUE,
          result
        )
      )
  }
  
  summary_table <- summary_table %>%
    mutate(medoc_concept = factor(medoc_concept, levels = unique(lookup$medoc_concept))) %>%
    arrange(medoc_concept) %>%
    select(medoc_concept, check, omop_table, result, percent_pass) %>%
    rename(`MEDOC concept` = medoc_concept, Check = check, `OMOP Table`= omop_table, Result = result, `Percentage of patients` = percent_pass)
  
  return(summary_table)
}


###############################################################################

#' process_vocab_table
#'
#' @param table one of the cdm reference object omop tables created in mappings 
#' @param source_value_col identifies source value column in omop table for the cdm table specified
#' specified in mappings 
#' @param concept_id_col identifies concept id columns in omop table for cdm table specified
#' specified in mappings
#' @param domain_name provides description of the domain 
#'
#' @returns
#' @export
#'
#' @details function to check all omop tables of interest and identify the proportion
#' of which records for medoc concepts are mapped to standard vocabulary
#' 

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

###############################################################################

#' execute_drug_checks
#'
#' @param drug_class drug class as identified in drug_code_list
#'
#'dependent on DrugExposureDiagnostics
#'
#' @returns checks$diagnosticsSummary
#' @export
#'
#' @details checks through the class of drugs to determine coverage in patients
#' and identify QA issues such as drugs received before diagnosis 
#' 
#' 

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

###############################################################################

#' execute_rt_checks
#'
#' @param cdm cdm reference object built in CodeToRun.R
#' @param radiotherapy_codes_path concept ids for radiotherapy provided via csv file
#'
#' @returns rt_checks
#' @export
#'
#' @details checks the presence on radiotherapy treatment codes in main_cohort and identifies 
#' proportion of patients with the treatment including any patients where the radiotherapy
#' was given after date of death
#' 

execute_rt_checks <- function(cdm, radiotherapy_codes_path) {
  radiotherapy_codes <- read.csv(radiotherapy_codes_path)
  
  rt_checks <- cdm$procedure_occurrence %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
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

###############################################################################

#' execute_procedure_checks
#'
#' @param cdm cdm reference object built in CodeToRun.R
#'
#' @returns checks
#' @export
#'
#' @details checks the proportion of main_cohort who have any procedure, and the 
#' validity of that procedure record 
#' 

execute_procedure_checks <- function(cdm) {
  
  checks <- cdm$procedure_occurrence %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
    left_join(cdm$death, by = "person_id") %>%
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
    collect() 
  
  return(checks)
}

###############################################################################

#' Title
#'
#' @param cdm cdm reference object built in CodeToRun.R
#'
#' @returns result
#' @export
#'
#' @details provides the concept ids which are currently recommended by MEDOC
#' to represent radiation dose information and summarises whether this information is present 
#' 
#' 
#' 

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

###############################################################################

#' Title
#'
#' @param cdm cdm reference object built in CodeToRun.R
#' @param medoc_concept_codes concept set lists including surgery concept set
#'
#' @returns rt_checks
#' @export
#'
#' @details checks main_cohort person_ids against cancer surgery concept ids. Concept
#' set is based on MEDOC recommendation. Proportion of after death procedures or 
#' negative procedure days is also included as a QA check 
#' 

execute_surgery_checks <- function(cdm, medoc_concept_codes) {
  surgery_codes <- medoc_concept_codes[["surgery"]]
  
  rt_checks <- cdm$procedure_occurrence %>%
    filter(procedure_concept_id %in% surgery_codes) %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
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

###############################################################################

#' Check TNM
#'
#' @param cdm cdm reference object built in CodeToRun.R
#' @param tnm_codes codes referring to concept_ids for T,N,M cancer measurements
#'
#' @returns result
#' @export
#'
#' @details checks for person_ids in main_cohorts, what the format of TNM codes are:
#' either stored as measurement_concept_id, value_as_concept_id or value
#'

check_tnm <- function(cdm, tnm_codes) {
  
  measurement_data <- cdm$measurement %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
    filter(measurement_concept_id %in% tnm_codes$measurement_concept_id | 
             value_as_concept_id %in% tnm_codes$measurement_concept_id) %>%
    collect()
  
  # Join with measurement_concept_id
  result_measurement <- measurement_data %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
    filter(measurement_concept_id %in% tnm_codes$measurement_concept_id) %>%
    left_join(tnm_codes, by = c("measurement_concept_id" = "measurement_concept_id")) %>%
    select(measurement_concept_id, concept_name) %>%
    distinct() %>%
    mutate(variable = "measurement_concept_id")
  
  # Join with value_as_concept_id
  result_value <- measurement_data %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
    filter(value_as_concept_id %in% tnm_codes$measurement_concept_id) %>%
    left_join(tnm_codes, by = c("value_as_concept_id" = "measurement_concept_id")) %>%
    select(value_as_concept_id, concept_name) %>%
    distinct() %>%
    mutate(variable = "value_as_concept_id")
  
  # Combine results
  result <- bind_rows(result_measurement, result_value)
  
  return(result)
}

###############################################################################
#' summarise_concept_counts
#'
#' @param cdm_table a table from cdm object
#' @param concept_id_col the concept id column relating to that cdm table
#' @param concept_table the cdm$concept table 
#' @param codelist list of concept ids, generated by CodeListGenerator 
#'
#' @returns
#' @export
#'
#' @details creates a summary table for all the relavant concept codes generated for 
#' a particular check, for example primary cancer diagnosis codes are checked against and summarised, 
#' including the concept ids, for future reference of diagnostic coverage
#' 

summarise_concept_counts <- function(cdm_table, concept_id_col, concept_table, codelist) {
  concept_id_sym <- sym(concept_id_col)
  
  cdm_table %>%
    inner_join(cdm$main_cohort, by = c('person_id' = 'subject_id')) %>%
    filter(!!concept_id_sym %in% codelist) %>%
    group_by(!!concept_id_sym) %>%
    summarise(person_id_count = n_distinct(person_id), .groups = "drop") %>%
    left_join(concept_table, by = setNames("concept_id", concept_id_col)) %>%
    select(concept_name, concept_id = !!concept_id_sym, person_id_count) %>%
    collect()
}
