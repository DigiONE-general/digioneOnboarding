
# -----------------------------------------------------------------------------
# internal utility functions
# -----------------------------------------------------------------------------
.safe_eval <- function(expr, fallback) {
  tryCatch(expr, error = function(e) fallback)
}

.safe_db_getquery <- function(conn, query) {
  tryCatch(DBI::dbGetQuery(conn, query), error = function(e) data.frame())
}

.has_cdm_table <- function(cdm, nm) {
  !is.null(cdm) && nm %in% names(cdm) && !is.null(cdm[[nm]])
}

# dbplyr-safe boolean-to-int aggregation helper
.bool_to_int <- function(x) {
  if_else(x %in% TRUE, 1L, 0L)
}

# -----------------------------------------------------------------------------
# check_tables
# -----------------------------------------------------------------------------
check_tables <- function(conn, sql_dialect, db_name = NULL, omop_schema_name = NULL) {
  
  if (is.null(db_name)) db_name <- get0("db_name", ifnotfound = NULL)
  if (is.null(omop_schema_name)) omop_schema_name <- get0("omop_schema_name", ifnotfound = NULL)
  
  out <- data.frame(
    Table = c("EPISODE", "EPISODE_EVENT"),
    Present = c(FALSE, FALSE),
    stringsAsFactors = FALSE
  )
  
  if (identical(sql_dialect, "sqlite")) {
    tables <- .safe_db_getquery(conn, "SELECT name FROM sqlite_master WHERE type='table'")
    if (nrow(tables) == 0 || !"name" %in% names(tables)) return(out)
    present_upper <- toupper(tables$name)
    out$Present <- out$Table %in% present_upper
    return(out)
  }

  schema_filter <- ""
  if (!is.null(omop_schema_name) && nzchar(omop_schema_name)) {
    schema_filter <- paste0(" AND LOWER(table_schema) = LOWER('", omop_schema_name, "')")
  } else if (!is.null(db_name) && nzchar(db_name) && identical(sql_dialect, "mysql")) {
    schema_filter <- paste0(" AND LOWER(table_schema) = LOWER('", db_name, "')")
  }
  
  q <- paste0(
    "SELECT table_name FROM information_schema.tables ",
    "WHERE LOWER(table_name) IN ('episode','episode_event')",
    schema_filter
  )
  
  tables <- .safe_db_getquery(conn, q)
  if (nrow(tables) == 0) return(out)
  
  nm <- if ("table_name" %in% names(tables)) "table_name" else names(tables)[1]
  present_upper <- toupper(tables[[nm]])
  out$Present <- out$Table %in% present_upper
  out
}

# -----------------------------------------------------------------------------
# get_cdm_details
# -----------------------------------------------------------------------------

get_cdm_details <- function(conn, db_name, omop_schema_name, sql_dialect = NULL) {
  
  
  if (is.null(sql_dialect)) {
    sql_dialect <- get0("sql_dialect", ifnotfound = NA_character_)
  }
  
  if (length(sql_dialect) != 1 || is.na(sql_dialect)) {
    stop("sql_dialect must be a single string (e.g. 'postgresql', 'snowflake')")
  }
  
  
  tbl_ref <- switch(
    sql_dialect,
    "snowflake"  = paste0(db_name, ".", omop_schema_name, ".CDM_SOURCE"),
    "mysql"      = paste0(db_name, ".CDM_SOURCE"),
    "postgresql" = paste0(omop_schema_name, ".CDM_SOURCE"),
    "sqlserver"  = paste0(omop_schema_name, ".CDM_SOURCE"),
    "redshift"   = paste0(omop_schema_name, ".CDM_SOURCE"),
    "sqlite"     = "CDM_SOURCE",
    paste0(omop_schema_name, ".CDM_SOURCE")
  )
  
  q <- paste0(
    "SELECT CDM_SOURCE_NAME, CDM_RELEASE_DATE, SOURCE_DESCRIPTION, VOCABULARY_VERSION ",
    "FROM ", tbl_ref
  )
  
  res <- .safe_db_getquery(conn, q)
  as.data.frame(res)
}

# -----------------------------------------------------------------------------
# get_cdm_counts
# -----------------------------------------------------------------------------
get_cdm_counts <- function(cdm) {
  
  if (is.null(cdm)) return(tibble(
    table = character(),
    records = numeric(),
    persons = numeric(),
    `person %` = numeric()
  ))

  total_persons <- tryCatch(
    cdm$person %>% summarise(n = n_distinct(person_id)) %>% collect() %>% pull(n),
    error = function(e) NA
  )
  total_persons <- suppressWarnings(as.numeric(total_persons))
  
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
  
  if ("episode" %in% names(cdm)) tables$episode <- cdm$episode
  if ("episode_event" %in% names(cdm)) tables$episode_event <- cdm$episode_event
  
  get_counts <- function(df, tablename) {
    
    cols <- tryCatch(colnames(df), error = function(e) character())
  
    fallback_row <- tibble(
      table   = as.character(tablename),
      records = 0,
      persons = NA_real_
    )
    
    if ("person_id" %in% cols) {
      out <- tryCatch(
        df %>%
          summarise(
            table = tablename,
            records = n(),
            persons = n_distinct(person_id)
          ) %>%
          collect(),
        error = function(e) fallback_row
      )
    } else {
      out <- tryCatch(
        df %>%
          summarise(
            table = tablename,
            records = n(),
            persons = NA_real_
          ) %>%
          collect(),
        error = function(e) fallback_row
      )
    }
    
    out <- out %>%
      mutate(
        table = as.character(table),
        records = suppressWarnings(as.numeric(records)),
        persons = suppressWarnings(as.numeric(persons))
      )
    
    if (!("table" %in% names(out)))   out$table <- as.character(tablename)
    if (!("records" %in% names(out))) out$records <- 0
    if (!("persons" %in% names(out))) out$persons <- NA_real_
    
    out %>% select(table, records, persons)
  }
  
  result <- bind_rows(lapply(names(tables), function(nm) as_tibble(get_counts(tables[[nm]], nm))))
  
  result <- result %>%
    mutate(
      table = as.character(table),
      records = suppressWarnings(as.numeric(records)),
      persons = suppressWarnings(as.numeric(persons))
    )
  
  result <- result %>%
    mutate(
      `person %` = ifelse(!is.na(total_persons) & total_persons > 0 & !is.na(persons),
                          round((persons / total_persons) * 100, 2),
                          NA_real_)
    ) %>%
    arrange(desc(records))
  
  result
}

# -----------------------------------------------------------------------------
# check_icdo3_matches
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# check_icdo3_matches
# -----------------------------------------------------------------------------
check_icdo3_matches <- function(cdm) {
  
  empty_out <- tibble(
    medoc_concept = "histological cell type",
    omop_table = c("observation", "condition_occurrence"),
    check = "concepts present in omop tables",
    result = c(NA, NA),
    percent_pass = c(NA_real_, NA_real_)
  )
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "main_cohort") || !.has_cdm_table(cdm, "concept")) {
    return(empty_out)
  }
  
  cohort_ids <- cdm$main_cohort %>%
    select(subject_id) %>%
    distinct()
  
  total <- .safe_eval(
    cohort_ids %>%
      summarise(n = n_distinct(subject_id)) %>%
      collect() %>%
      pull(n),
    NA_integer_
  )
  
  if (is.na(total) || total == 0) {
    return(empty_out %>% mutate(result = FALSE, percent_pass = 0))
  }
  
  icdo3_ids <- .safe_eval(
    cdm$concept %>%
      filter(vocabulary_id == "ICDO3") %>%
      distinct(concept_id) %>%
      collect() %>%
      pull(concept_id),
    numeric()
  )
  
  if (!length(icdo3_ids)) {
    return(empty_out %>% mutate(result = FALSE, percent_pass = 0))
  }
  
  obs_passed <- if (.has_cdm_table(cdm, "observation")) {
    .safe_eval(
      cdm$observation %>%
        semi_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
        filter(.data[["observation_concept_id"]] %in% icdo3_ids) %>%
        summarise(passed = n_distinct(person_id)) %>%
        collect() %>%
        pull(passed),
      NA_integer_
    )
  } else NA_integer_
  
  obs_percent <- if (is.na(obs_passed)) NA_real_ else round(100 * obs_passed / total, 2)
  
  observation_result <- tibble(
    medoc_concept = "histological cell type",
    omop_table = "observation",
    check = "concepts present in omop tables",
    result = ifelse(is.na(obs_passed), NA, obs_passed > 0),
    percent_pass = obs_percent
  )
  
  cond_passed <- if (.has_cdm_table(cdm, "condition_occurrence")) {
    .safe_eval(
      cdm$condition_occurrence %>%
        semi_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
        filter(.data[["condition_concept_id"]] %in% icdo3_ids) %>%
        summarise(passed = n_distinct(person_id)) %>%
        collect() %>%
        pull(passed),
      NA_integer_
    )
  } else NA_integer_
  
  cond_percent <- if (is.na(cond_passed)) NA_real_ else round(100 * cond_passed / total, 2)
  
  condition_result <- tibble(
    medoc_concept = "histological cell type",
    omop_table = "condition_occurrence",
    check = "concepts present in omop tables",
    result = ifelse(is.na(cond_passed), NA, cond_passed > 0),
    percent_pass = cond_percent
  )
  
  bind_rows(observation_result, condition_result)
}

# -----------------------------------------------------------------------------
# evaluate_concept
# -----------------------------------------------------------------------------
evaluate_concept <- function(concept, visited = character()) {
  
  safe_row <- function(check_label = NA, res = NA, pct = NA_real_, table = NA) {
    tibble(medoc_concept = concept, omop_table = table, check = check_label, result = res, percent_pass = pct)
  }
  
  lookup <- get0("lookup", ifnotfound = NULL)
  cdm <- get0("cdm", ifnotfound = NULL)
  genomic_codes <- get0("genomic_codes", ifnotfound = integer())
  tumour_stage_codes <- get0("tumour_stage_codes", ifnotfound = integer())
  medoc_concept_codes <- get0("medoc_concept_codes", ifnotfound = NULL)
  
  if (is.null(lookup) || nrow(as.data.frame(lookup)) == 0) return(safe_row())
  if (is.null(cdm) || !.has_cdm_table(cdm, "main_cohort")) return(safe_row("missing main_cohort", FALSE, 0))
  
  if (concept %in% visited) {
    warning(paste("Circular dependency detected for:", concept))
    return(safe_row("circular dependency", NA, NA_real_))
  }
  
  rows <- lookup %>% filter(medoc_concept == concept)
  if (nrow(rows) == 0) return(safe_row())
  
  check_type   <- rows$check[!is.na(rows$check)][1]
  omop_table   <- rows$omop_table[!is.na(rows$omop_table)][1]
  filter_level <- rows$filter_level[!is.na(rows$filter_level)][1]
  concept_set  <- rows$concept_set[!is.na(rows$concept_set)][1]
  
  visited <- c(visited, concept)
  
  cohort_ids <- cdm$main_cohort %>% select(subject_id)
  total <- .safe_eval(cohort_ids %>% summarise(n = n()) %>% collect() %>% pull(n), NA_integer_)
  if (is.na(total) || total == 0) return(safe_row("empty cohort", FALSE, 0, omop_table))
  
  if (!is.na(filter_level)) {
    filter_result <- evaluate_concept(filter_level, visited)
    if (!isTRUE(filter_result$result[1])) {
      return(safe_row(paste0("filtered by ", filter_level), FALSE, 0, omop_table))
    }
  }
  
  if (!is.na(omop_table) && (startsWith(omop_table, "see_") || startsWith(omop_table, "derived"))) {
    return(safe_row(NA, NA, NA_real_, omop_table))
  }
  
  if (is.na(omop_table) || !(omop_table %in% names(cdm)) || is.null(cdm[[omop_table]])) {
    return(safe_row("table missing", FALSE, 0, omop_table))
  }
  
  if (!is.na(check_type) && check_type == "present") {
    check_label <- "variable is present in cdm"
    present_rows <- rows %>% filter(check == "present", !is.na(omop_variable))
    if (nrow(present_rows) == 0) return(safe_row(check_label, NA, NA_real_, omop_table))
    
    if (concept == "biomarker_measure") {
      if (length(genomic_codes) == 0) return(safe_row(check_label, FALSE, 0, omop_table))
      
      vars <- present_rows$omop_variable
      
      condition <- purrr::reduce(
        lapply(vars, function(v) rlang::expr(!is.na(.data[[!!v]]))),
        function(x, y) rlang::expr(!!x | !!y)
      )
      
      passed <- .safe_eval(
        cdm[[omop_table]] %>%
          inner_join(
            cdm$main_cohort %>%
              distinct(subject_id),
            by = c("person_id" = "subject_id")
          ) %>%
          filter(.data[["measurement_concept_id"]] %in% genomic_codes) %>%
          filter(!!condition) %>%
          summarise(passed = n_distinct(person_id)) %>%
          collect() %>%
          pull(passed),
        NA_integer_
      )
      
      pct <- round(100 * passed / total, 2)
      return(safe_row(check_label, passed > 0, pct, omop_table))
    }
    
    if (any(present_rows$omop_variable == "omop_table", na.rm = TRUE)) {
      res <- omop_table %in% names(cdm)
      return(safe_row(check_label, res, if (res) 100 else 0, omop_table))
    }
    
    cols <- .safe_eval(colnames(cdm[[omop_table]]), character())
    if (!all(present_rows$omop_variable %in% cols)) {
      return(safe_row(check_label, FALSE, 0, omop_table))
    }
    
    passed <- .safe_eval(
      cdm[[omop_table]] %>%
        inner_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
        filter(if_any(all_of(present_rows$omop_variable), ~ !is.na(.))) %>%
        summarise(passed = n_distinct(person_id)) %>%
        collect() %>%
        pull(passed),
      0L
    )
    
    pct <- round(100 * passed / total, 2)
    return(safe_row(check_label, passed > 0, pct, omop_table))
  }

  if (!is.na(check_type) && check_type == "concept") {
    check_label <- "concepts present in omop tables"
    
    concept_codes <- NULL
    if (!is.na(concept_set) && concept_set == "genomic_codes") {
      concept_codes <- genomic_codes
    } else if (!is.na(concept_set) && concept_set == "tumour_stage_codes") {
      concept_codes <- tumour_stage_codes
    } else if (!is.na(concept_set) && !is.null(medoc_concept_codes) && concept_set %in% names(medoc_concept_codes)) {
      concept_codes <- medoc_concept_codes[[concept_set]]
    }
    
    concept_var <- rows$omop_concept_variable[!is.na(rows$omop_concept_variable)][1]
    cols <- .safe_eval(colnames(cdm[[omop_table]]), character())
    
    if (is.null(concept_codes) || length(concept_codes) == 0 || is.na(concept_var) || !(concept_var %in% cols)) {
      return(safe_row(check_label, FALSE, 0, omop_table))
    }
    
    passed <- .safe_eval(
      cdm[[omop_table]] %>%
        inner_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
        filter(.data[[concept_var]] %in% concept_codes) %>%
        summarise(passed = n_distinct(person_id)) %>%
        collect() %>%
        pull(passed),
      0L
    )
    
    pct <- round(100 * passed / total, 2)
    return(safe_row(check_label, passed > 0, pct, omop_table))
  }
  
  safe_row(NA, NA, NA_real_, omop_table)
}

# -----------------------------------------------------------------------------
# postprocess_concept_table
# -----------------------------------------------------------------------------
postprocess_concept_table <- function(medoc_concept_table) {
  
  lookup <- get0("lookup", ifnotfound = NULL)
  
  if (is.null(medoc_concept_table) || nrow(as.data.frame(medoc_concept_table)) == 0) {
    if (!is.null(lookup) && "medoc_concept" %in% names(lookup)) {
      medoc_concept_table <- tibble(medoc_concept = unique(lookup$medoc_concept))
    } else {
      return(tibble())
    }
  }
  
  if (is.null(lookup) || nrow(as.data.frame(lookup)) == 0 || !"medoc_concept" %in% names(lookup)) {
    return(medoc_concept_table %>%
             rename(`MEDOC concept` = medoc_concept,
                    Check = check,
                    `OMOP Table` = omop_table,
                    Result = result,
                    `Percentage of patients` = percent_pass))
  }
  
  summary_table <- medoc_concept_table %>%
    group_by(medoc_concept) %>%
    summarise(
      result = case_when(
        any(result %in% TRUE, na.rm = TRUE) ~ TRUE,
        all(is.na(result)) ~ NA,
        TRUE ~ FALSE
      ),
      percent_pass = case_when(
        all(is.na(percent_pass)) ~ NA_real_,
        any(is.finite(percent_pass)) ~ max(percent_pass, na.rm = TRUE),
        TRUE ~ NA_real_
      ),
      omop_table = dplyr::first(na.omit(omop_table), default = NA_character_),
      check = dplyr::first(na.omit(check), default = NA_character_),
      .groups = "drop"
    ) %>%
    mutate(percent_pass = ifelse(is.infinite(percent_pass), NA, percent_pass))
  
  all_concepts <- tibble(medoc_concept = unique(lookup$medoc_concept))
  summary_table <- all_concepts %>% left_join(summary_table, by = "medoc_concept")
  
  location_row <- summary_table %>% filter(medoc_concept == "metastasis_location")
  presence_row <- summary_table %>% filter(medoc_concept == "metastasis_presence")
  
  if (nrow(location_row) == 1 && nrow(presence_row) == 1) {
    combined_percent <- sum(c(location_row$percent_pass, presence_row$percent_pass), na.rm = TRUE)
    combined_percent <- min(combined_percent, 100)
    
    summary_table <- summary_table %>%
      mutate(
        percent_pass = ifelse(medoc_concept == "metastasis_presence", combined_percent, percent_pass),
        result = ifelse(medoc_concept == "metastasis_presence" & combined_percent > 0, TRUE, result)
      )
  }
  
  summary_table %>%
    mutate(medoc_concept = factor(medoc_concept, levels = unique(lookup$medoc_concept))) %>%
    arrange(medoc_concept) %>%
    select(medoc_concept, check, omop_table, result, percent_pass) %>%
    rename(
      `MEDOC concept` = medoc_concept,
      Check = check,
      `OMOP Table` = omop_table,
      Result = result,
      `Percentage of patients` = percent_pass
    )
}

# -----------------------------------------------------------------------------
# process_vocab_table
# -----------------------------------------------------------------------------
process_vocab_table <- function(table, source_value_col, concept_id_col, domain_name) {
  
  safe_collect <- function(x) .safe_eval(x %>% collect(), tibble())
  
  res <- table %>%
    mutate(
      source_value = if_else(is.na(.data[[source_value_col]]), "", as.character(.data[[source_value_col]])),
      concept_id = .data[[concept_id_col]],
      is_mapped = if_else(.data[[concept_id_col]] == 0L | .data[[concept_id_col]] > 2000000000L, 0L, 1L)
    ) %>%
    group_by(.data[[concept_id_col]], .data[[source_value_col]]) %>%
    summarise(
      num_records = n(),
      is_mapped = min(is_mapped),
      .groups = "drop"
    ) %>%
    summarise(
      domain = domain_name,
      num_codes_source = n(),
      num_codes_mapped = sum(is_mapped, na.rm = TRUE),
      num_records_source = sum(num_records, na.rm = TRUE),
      num_records_mapped = sum(is_mapped * num_records, na.rm = TRUE),
      .groups = "drop"
    )
  
  out <- safe_collect(res)
  
  if (nrow(out) == 0) {
    return(tibble(
      domain = domain_name,
      num_codes_source = 0,
      num_codes_mapped = 0,
      percent_codes_mapped = NA_real_,
      num_records_source = 0,
      num_records_mapped = 0,
      percent_records_mapped = NA_real_
    ))
  }
  
  out %>%
    mutate(
      percent_codes_mapped = ifelse(num_codes_source > 0, 100.0 * num_codes_mapped / num_codes_source, NA_real_),
      percent_records_mapped = ifelse(num_records_source > 0, 100.0 * num_records_mapped / num_records_source, NA_real_)
    )
}

# -----------------------------------------------------------------------------
# execute_drug_checks
# -----------------------------------------------------------------------------
execute_drug_checks <- function(drug_class) {
  
  cdm <- get0("cdm", ifnotfound = NULL)
  drug_code_list <- get0("drug_code_list", ifnotfound = NULL)
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "drug_exposure")) return(tibble())
  if (is.null(drug_code_list) || nrow(as.data.frame(drug_code_list)) == 0) return(tibble())
  
  drug_codes <- drug_code_list %>% filter(class == drug_class)
  if (nrow(drug_codes) == 0 || !"concept_id" %in% names(drug_codes)) return(tibble())
  
  ingr <- drug_codes[["concept_id"]]
  if (is.null(ingr) || length(ingr) == 0) return(tibble())
  
  checks <- tryCatch(
    DrugExposureDiagnostics::executeChecks(
      cdm = cdm,
      ingredients = ingr,
      byConcept = TRUE,
      checks = c("exposureDuration", "diagnosticsSummary")
    ),
    error = function(e) NULL
  )
  
  if (is.null(checks) || is.null(checks$diagnosticsSummary)) tibble() else checks$diagnosticsSummary
}

# -----------------------------------------------------------------------------
# execute_rt_checks
# -----------------------------------------------------------------------------
execute_rt_checks <- function(cdm, radiotherapy_codes_path) {
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "procedure_occurrence") || !.has_cdm_table(cdm, "main_cohort")) return(tibble())
  
  radiotherapy_codes <- .safe_eval(read.csv(radiotherapy_codes_path), data.frame())
  if (nrow(radiotherapy_codes) == 0 || !"concept_id" %in% names(radiotherapy_codes)) return(tibble())
  
  has_death <- .has_cdm_table(cdm, "death")
  
  q <- cdm$procedure_occurrence %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
    filter(procedure_concept_id %in% radiotherapy_codes$concept_id)
  
  if (has_death) q <- q %>% left_join(cdm$death, by = "person_id")
  
  out <- .safe_eval(
    q %>%
      group_by(procedure_concept_id) %>%
      summarise(
        unique_person_count = n_distinct(person_id),
        unique_record_count = n(),
        negative_procedure_days_count = sum(if_else(!is.na(procedure_end_date) & !is.na(procedure_date) & procedure_end_date < procedure_date, 1L, 0L), na.rm = TRUE),
        after_death_count = if (has_death) sum(if_else(!is.na(death_date) & !is.na(procedure_date) & procedure_date > death_date, 1L, 0L), na.rm = TRUE) else 0L,
        .groups = "drop"
      ) %>%
      mutate(
        proportion_of_records_with_negative_procedure_days = if_else(
          unique_record_count > 0,
          paste0(negative_procedure_days_count, " (", round((negative_procedure_days_count / unique_record_count) * 100, 2), "%)"),
          paste0(negative_procedure_days_count, " (NA%)")
        ),
        proportion_of_after_death_procedures = if_else(
          unique_person_count > 0,
          paste0(after_death_count, " (", round((after_death_count / unique_person_count) * 100, 2), "%)"),
          paste0(after_death_count, " (NA%)")
        )
      ) %>%
      collect(),
    tibble()
  )
  
  out %>% left_join(radiotherapy_codes, by = c("procedure_concept_id" = "concept_id"))
}

# -----------------------------------------------------------------------------
# execute_procedure_checks
# -----------------------------------------------------------------------------
execute_procedure_checks <- function(cdm) {
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "procedure_occurrence") || !.has_cdm_table(cdm, "main_cohort")) return(tibble())
  
  has_death <- .has_cdm_table(cdm, "death")
  
  q <- cdm$procedure_occurrence %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id"))
  
  if (has_death) q <- q %>% left_join(cdm$death, by = "person_id")
  
  .safe_eval(
    q %>%
      summarise(
        unique_person_count = n_distinct(person_id),
        unique_record_count = n(),
        negative_procedure_days_count = sum(if_else(!is.na(procedure_end_date) & !is.na(procedure_date) & procedure_end_date < procedure_date, 1L, 0L), na.rm = TRUE),
        after_death_count = if (has_death) sum(if_else(!is.na(death_date) & !is.na(procedure_date) & procedure_date > death_date, 1L, 0L), na.rm = TRUE) else 0L,
        .groups = "drop"
      ) %>%
      mutate(
        proportion_of_records_with_negative_procedure_days = if_else(
          unique_record_count > 0,
          paste0(negative_procedure_days_count, " (", round((negative_procedure_days_count / unique_record_count) * 100, 2), "%)"),
          paste0(negative_procedure_days_count, " (NA%)")
        ),
        proportion_of_after_death_procedures = if_else(
          unique_person_count > 0,
          paste0(after_death_count, " (", round((after_death_count / unique_person_count) * 100, 2), "%)"),
          paste0(after_death_count, " (NA%)")
        )
      ) %>%
      collect(),
    tibble()
  )
}

# -----------------------------------------------------------------------------
# check_radiation_dose_info
# -----------------------------------------------------------------------------
check_radiation_dose_info <- function(cdm) {
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "measurement")) {
    return(tibble(`radiation dose` = "radiation dose info available?", status = NA))
  }
  
  ids_to_check <- c(40483776, 4155148, 35918606, 46236014)
  
  is_present <- .safe_eval(
    cdm$measurement %>%
      filter(measurement_concept_id %in% ids_to_check) %>%
      summarise(present = n() > 0) %>%
      collect() %>%
      pull(present),
    NA
  )
  
  tibble(`radiation dose` = "radiation dose info available?", status = is_present)
}

# -----------------------------------------------------------------------------
# execute_surgery_checks
# -----------------------------------------------------------------------------
execute_surgery_checks <- function(cdm, medoc_concept_codes) {
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "procedure_occurrence") || !.has_cdm_table(cdm, "main_cohort")) return(tibble())
  if (is.null(medoc_concept_codes) || !("surgery" %in% names(medoc_concept_codes))) return(tibble())
  
  surgery_codes <- medoc_concept_codes[["surgery"]]
  if (is.null(surgery_codes) || length(surgery_codes) == 0) return(tibble())
  
  has_death <- .has_cdm_table(cdm, "death")
  
  q <- cdm$procedure_occurrence %>%
    inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
    filter(procedure_concept_id %in% surgery_codes)
  
  if (has_death) q <- q %>% left_join(cdm$death, by = "person_id")
  
  .safe_eval(
    q %>%
      group_by(procedure_concept_id) %>%
      summarise(
        unique_person_count = n_distinct(person_id),
        unique_record_count = n(),
        negative_procedure_days_count = sum(if_else(!is.na(procedure_end_date) & !is.na(procedure_date) & procedure_end_date < procedure_date, 1L, 0L), na.rm = TRUE),
        after_death_count = if (has_death) sum(if_else(!is.na(death_date) & !is.na(procedure_date) & procedure_date > death_date, 1L, 0L), na.rm = TRUE) else 0L,
        .groups = "drop"
      ) %>%
      collect(),
    tibble()
  )
}

# -----------------------------------------------------------------------------
# check tnm
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# check_tnm
# -----------------------------------------------------------------------------
check_tnm <- function(cdm, tnm_codes) {
  
  if (is.null(cdm) || !.has_cdm_table(cdm, "measurement") || !.has_cdm_table(cdm, "main_cohort")) {
    return(tibble())
  }
  
  if (is.null(tnm_codes) || !"measurement_concept_id" %in% names(tnm_codes)) {
    return(tibble())
  }
  
  tnm_ids <- unique(stats::na.omit(tnm_codes$measurement_concept_id))
  if (!length(tnm_ids)) return(tibble())
  
  cohort_ids <- cdm$main_cohort %>%
    select(subject_id) %>%
    distinct()
  
  # Match on measurement_concept_id only
  measurement_hits <- .safe_eval(
    cdm$measurement %>%
      semi_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
      filter(.data[["measurement_concept_id"]] %in% tnm_ids) %>%
      distinct(measurement_concept_id) %>%
      collect(),
    tibble(measurement_concept_id = numeric())
  )
  
  # Match on value_as_concept_id only
  value_hits <- .safe_eval(
    cdm$measurement %>%
      semi_join(cohort_ids, by = c("person_id" = "subject_id")) %>%
      filter(.data[["value_as_concept_id"]] %in% tnm_ids) %>%
      distinct(value_as_concept_id) %>%
      collect(),
    tibble(value_as_concept_id = numeric())
  )
  
  result_measurement <- tibble()
  if (nrow(measurement_hits) > 0) {
    result_measurement <- measurement_hits %>%
      left_join(
        tnm_codes %>%
          distinct(measurement_concept_id, concept_name),
        by = "measurement_concept_id"
      ) %>%
      transmute(
        measurement_concept_id,
        concept_name,
        variable = "measurement_concept_id"
      ) %>%
      distinct()
  }
  
  result_value <- tibble()
  if (nrow(value_hits) > 0) {
    result_value <- value_hits %>%
      left_join(
        tnm_codes %>%
          distinct(measurement_concept_id, concept_name),
        by = c("value_as_concept_id" = "measurement_concept_id")
      ) %>%
      transmute(
        value_as_concept_id,
        concept_name,
        variable = "value_as_concept_id"
      ) %>%
      distinct()
  }
  
  bind_rows(result_measurement, result_value)
}


# -----------------------------------------------------------------------------
# summarise_concept_counts
# -----------------------------------------------------------------------------

summarise_concept_counts <- function(cdm_table, concept_id_col, concept_table, codelist, cdm = NULL) {
  
  if (is.null(cdm)) cdm <- get0("cdm", ifnotfound = NULL)
  if (is.null(cdm) || !.has_cdm_table(cdm, "main_cohort")) {
    return(tibble(
      concept_name = character(),
      concept_id = integer(),
      person_id_count = integer()
    ))
  }
  
  if (is.null(codelist) || length(codelist) == 0) {
    return(tibble(
      concept_name = character(),
      concept_id = integer(),
      person_id_count = integer()
    ))
  }
  
  concept_id_sym <- sym(concept_id_col)
  
  .safe_eval(
    cdm_table %>%
      inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
      filter(!!concept_id_sym %in% codelist) %>%
      group_by(!!concept_id_sym) %>%
      summarise(person_id_count = n_distinct(person_id), .groups = "drop") %>%
      left_join(concept_table, by = setNames("concept_id", concept_id_col)) %>%
      select(concept_name, concept_id = !!concept_id_sym, person_id_count) %>%
      collect(),
    tibble(
      concept_name = character(),
      concept_id = integer(),
      person_id_count = integer()
    )
  )
}
