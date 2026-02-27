library(DBI)
library(CDMConnector)
library(odbc)
library(dplyr)
library(dbplyr)
library(DrugExposureDiagnostics)
library(here)
library(rmarkdown)
library(knitr)
library(kableExtra)
library(OmopSketch)
library(visOmopResults)
library(flextable)
library(gt)
library(purrr)
library(cli)
library(tidyr)
library(PatientProfiles)
library(CodelistGenerator)
library(tibble)
library(stringr)

suppressWarnings(suppressMessages(requireNamespace("snakecase", quietly = TRUE)))

###############################################################################
# SAFETY HELPERS 
###############################################################################

safe_eval <- function(expr, fallback = NULL, msg = NULL) {
  tryCatch(
    expr,
    error = function(e) {
      if (!is.null(msg)) cli::cli_alert_warning("{msg}: {conditionMessage(e)}")
      fallback
    }
  )
}

ensure_dir <- function(path) {
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

safe_read_csv <- function(path, ...) {
  if (!file.exists(path)) {
    cli::cli_alert_warning("Missing file: {path}")
    return(tibble())
  }
  as_tibble(safe_eval(read.csv(path, ...),
                      fallback = data.frame(),
                      msg = paste0("Failed reading ", path)))
}

cdm_has <- function(cdm, nm) {
  !is.null(cdm) && nm %in% names(cdm) && !is.null(cdm[[nm]])
}

safe_collect <- function(x, msg = "collect() failed") {
  safe_eval(dplyr::collect(x), fallback = tibble(), msg = msg)
}

extract_gt_data <- function(x) {
  if (is.null(x)) return(tibble())
  out <- safe_eval(as.data.frame(x[["_data"]]), fallback = NULL)
  if (is.null(out)) out <- safe_eval(as.data.frame(x), fallback = NULL)
  if (is.null(out)) tibble() else as_tibble(out)
}

ensure_renderable <- function(x, note = "No data available") {
  x <- safe_eval(as.data.frame(x), fallback = data.frame())
  if (is.null(x) || ncol(x) == 0 || nrow(x) == 0) {
    return(data.frame(Note = note, stringsAsFactors = FALSE))
  }
  x
}

empty_concept_summary <- function() {
  tibble(
    concept_id = integer(),
    concept_name = character(),
    person_id_count = integer(),
    record_count = integer()
  )
}

summarise_concept_counts_safe <- function(cdm_table,
                                          concept_id_col,
                                          concept_table,
                                          codelist,
                                          person_id_col = "person_id") {
  
  if (is.null(codelist) || length(codelist) == 0) {
    cli::cli_alert_warning("Codelist empty for {concept_id_col}; returning empty summary.")
    return(empty_concept_summary())
  }
  
  out <- tryCatch({
    
    counts <- cdm_table %>%
      dplyr::filter(.data[[concept_id_col]] %in% codelist) %>%
      dplyr::group_by(concept_id = .data[[concept_id_col]]) %>%
      dplyr::summarise(
        person_id_count = dplyr::n_distinct(.data[[person_id_col]]),
        record_count = dplyr::n(),
        .groups = "drop"
      )
    
    joined <- counts %>%
      dplyr::left_join(
        concept_table %>% dplyr::select(concept_id, concept_name),
        by = "concept_id"
      )
    
    dplyr::collect(joined)
    
  }, error = function(e) {
    cli::cli_alert_warning("summarise_concept_counts_safe failed: {conditionMessage(e)}")
    empty_concept_summary()
  })
  
  out %>%
    dplyr::mutate(
      concept_id = suppressWarnings(as.integer(concept_id)),
      person_id_count = suppressWarnings(as.integer(person_id_count)),
      record_count = suppressWarnings(as.integer(record_count)),
      concept_name = as.character(concept_name)
    )
}

slice_top_counts_safe <- function(df, n = 20, count_col = "person_id_count") {
  
  df <- safe_eval(as_tibble(df), fallback = tibble())
  if (ncol(df) == 0) return(tibble(total_patient_count = character()))
  
  if (!count_col %in% names(df)) df[[count_col]] <- NA_integer_
  
  df %>%
    dplyr::mutate(!!count_col := suppressWarnings(as.integer(.data[[count_col]]))) %>%
    dplyr::arrange(dplyr::desc(dplyr::coalesce(.data[[count_col]], 0L))) %>%
    dplyr::slice_head(n = n) %>%
    dplyr::mutate(
      total_patient_count = dplyr::if_else(
        is.na(.data[[count_col]]) | .data[[count_col]] < 5L,
        "<5",
        as.character(.data[[count_col]])
      )
    ) %>%
    dplyr::select(-dplyr::any_of(count_col))
}

render_safe <- function(...) {
  ok <- tryCatch({
    rmarkdown::render(...)
    TRUE
  }, error = function(e) {
    cli::cli_alert_warning("Render failed: {conditionMessage(e)}")
    FALSE
  })
  ok
}

###############################################################################
# GET CDM DETAILS FROM CDM_SOURCE
###############################################################################
cli::cli_alert("Gathering CDM details - {Sys.time()}")

cdm_details <- safe_eval(
  {
    if (exists("sql_dialect")) {
      safe_eval(get_cdm_details(conn, db_name, omop_schema_name, sql_dialect),
                fallback = get_cdm_details(conn, db_name, omop_schema_name))
    } else {
      get_cdm_details(conn, db_name, omop_schema_name)
    }
  },
  fallback = tibble(),
  msg = "get_cdm_details failed"
)

cdm_record_counts <- safe_eval(
  get_cdm_counts(cdm),
  fallback = tibble(
    table = character(),
    records = numeric(),
    persons = numeric(),
    `person %` = numeric()
  ),
  msg = "get_cdm_counts failed"
)

tmp_overall <- safe_eval(
  OmopSketch::summariseOmopSnapshot(cdm) %>% OmopSketch::tableOmopSnapshot(),
  fallback = NULL,
  msg = "summariseOmopSnapshot failed"
)

cdm_overall_snapshot <- extract_gt_data(tmp_overall)
if (nrow(cdm_overall_snapshot) > 0 && ncol(cdm_overall_snapshot) >= 3) {
  colnames(cdm_overall_snapshot)[1:3] <- c("Level", "Variable", "Value")
}

cli::cli_alert("Gathering CDM details - complete! - {Sys.time()}")

###############################################################################
# CREATE SUMMARIES OF CLINICAL AND OBSERVATION TABLES
###############################################################################
cli::cli_alert("Creating clinical snapshot tables - {Sys.time()}")

tmp_clinical <- safe_eval(
  OmopSketch::summariseClinicalRecords(
    cdm,
    c("condition_occurrence", "drug_exposure", "measurement")
  ) %>% OmopSketch::tableClinicalRecords(),
  fallback = NULL,
  msg = "summariseClinicalRecords failed"
)

cdm_snapshot_clinical <- extract_gt_data(tmp_clinical)
if (nrow(cdm_snapshot_clinical) > 0 && ncol(cdm_snapshot_clinical) >= 5) {
  colnames(cdm_snapshot_clinical)[1:5] <- c("Omop Table", "Variable", "Response", "Measurement", "Value")
}

cdm_snapshot_clinical_condition <- cdm_snapshot_clinical %>%
  dplyr::filter(`Omop Table` == "condition_occurrence") %>%
  dplyr::select(-`Omop Table`)

cdm_snapshot_clinical_drug <- cdm_snapshot_clinical %>%
  dplyr::filter(`Omop Table` == "drug_exposure") %>%
  dplyr::select(-`Omop Table`)

cdm_snapshot_clinical_meas <- cdm_snapshot_clinical %>%
  dplyr::filter(`Omop Table` == "measurement") %>%
  dplyr::select(-`Omop Table`)

tmp_obs <- safe_eval(
  if (cdm_has(cdm, "observation_period")) {
    OmopSketch::summariseObservationPeriod(cdm$observation_period) %>%
      OmopSketch::tableObservationPeriod()
  } else NULL,
  fallback = NULL,
  msg = "summariseObservationPeriod failed"
)

cdm_snapshot_obs <- extract_gt_data(tmp_obs) %>%
  dplyr::select(-dplyr::any_of("Variable level"))

if (nrow(cdm_snapshot_obs) > 0 && ncol(cdm_snapshot_obs) >= 4) {
  colnames(cdm_snapshot_obs)[1:4] <- c("Observation Period", "Variable", "Measurement", "Value")
}

cli::cli_alert("Creating clinical snapshot tables - complete! - {Sys.time()}")

###############################################################################
# CHECK VOCABULARY MAPPING COMPLETEDNESS FOR ALL MEDOC VARIABLES
###############################################################################
cli::cli_alert("Assessing vocabulary mapping completedness - {Sys.time()}")

mappings <- list(
  list(table_name = "visit_occurrence", source_value = "visit_source_value", concept_id = "visit_concept_id", description = "visit"),
  list(table_name = "observation", source_value = "observation_source_value", concept_id = "observation_concept_id", description = "observation"),
  list(table_name = "observation", source_value = "unit_source_value", concept_id = "unit_concept_id", description = "observation unit"),
  list(table_name = "condition_occurrence", source_value = "condition_source_value", concept_id = "condition_concept_id", description = "condition"),
  list(table_name = "condition_occurrence", source_value = "condition_status_source_value", concept_id = "condition_status_concept_id", description = "condition status"),
  list(table_name = "death", source_value = "cause_source_value", concept_id = "cause_concept_id", description = "death cause"),
  list(table_name = "procedure_occurrence", source_value = "procedure_source_value", concept_id = "procedure_concept_id", description = "procedure occurrence"),
  list(table_name = "measurement", source_value = "measurement_source_value", concept_id = "measurement_concept_id", description = "measurement"),
  list(table_name = "measurement", source_value = "unit_source_value", concept_id = "unit_concept_id", description = "measurement unit"),
  list(table_name = "measurement", source_value = "value_source_value", concept_id = "value_as_concept_id", description = "measurement value"),
  list(table_name = "drug_exposure", source_value = "drug_source_value", concept_id = "drug_concept_id", description = "drug exposure"),
  list(table_name = "drug_exposure", source_value = "route_source_value", concept_id = "route_concept_id", description = "drug route"),
  list(table_name = "provider", source_value = "specialty_source_value", concept_id = "specialty_concept_id", description = "provider speciality")
)

results <- list()

for (mapping in mappings) {
  
  fallback_row <- tibble(
    domain = mapping$description,
    num_codes_source = 0,
    num_codes_mapped = 0,
    percent_codes_mapped = NA_real_,
    num_records_source = 0,
    num_records_mapped = 0,
    percent_records_mapped = NA_real_
  )
  
  if (!cdm_has(cdm, mapping$table_name)) {
    results <- append(results, list(fallback_row))
    next
  }
  
  tbl_obj <- cdm[[mapping$table_name]]
  cols <- safe_eval(colnames(tbl_obj), fallback = character())
  if (!(mapping$source_value %in% cols) || !(mapping$concept_id %in% cols)) {
    results <- append(results, list(fallback_row))
    next
  }
  
  result <- safe_eval(
    process_vocab_table(tbl_obj, mapping$source_value, mapping$concept_id, mapping$description),
    fallback = fallback_row,
    msg = paste0("process_vocab_table failed for ", mapping$description)
  )
  
  results <- append(results, list(result))
}

mappingCompleteness <- safe_eval(do.call(rbind, results), fallback = tibble())
if (nrow(mappingCompleteness) > 0 && "domain" %in% names(mappingCompleteness)) {
  mappingCompleteness <- mappingCompleteness %>%
    dplyr::arrange(domain) %>%
    dplyr::mutate(
      Domain = domain,
      `#Codes Source` = num_codes_source,
      `#Codes Mapped` = num_codes_mapped,
      `%Codes Mapped` = percent_codes_mapped,
      `#Records Source` = num_records_source,
      `#Records Mapped` = num_records_mapped,
      `%Records Mapped` = percent_records_mapped,
      .keep = "none"
    )
}

cli::cli_alert("Assessing vocabulary mapping completedness - complete! - {Sys.time()}")

###############################################################################
# CREATE SUBSETTED CANCER COHORT
###############################################################################
cli::cli_alert("Create subsetted cohort - {Sys.time()}")

path_to_json <- here::here("inst/cohorts/")

cohort_details <- tibble()
if (dir.exists(path_to_json)) {
  cohort_details <- safe_eval(
    CDMConnector::readCohortSet(path_to_json) %>%
      dplyr::mutate(cohort_name = snakecase::to_snake_case(cohort_name)),
    fallback = tibble(),
    msg = "readCohortSet failed"
  )
} else {
  cli::cli_alert_warning("Cohort folder missing: {path_to_json}. main_cohort will be empty.")
}

if (nrow(cohort_details) > 0) {
  cdm <- safe_eval(
    CDMConnector::generateCohortSet(cdm = cdm, cohortSet = cohort_details, name = "main_cohort"),
    fallback = cdm,
    msg = "generateCohortSet failed"
  )
} else {
  
  cdm$main_cohort <- tibble(subject_id = integer(), cohort_name = character())
}

if (cdm_has(cdm, "main_cohort")) {
  cdm$main_cohort <- safe_eval(
    cdm$main_cohort %>%
      PatientProfiles::addCohortName() %>%
      dplyr::select(1, cohort_name, dplyr::everything()),
    fallback = cdm$main_cohort,
    msg = "addCohortName failed"
  )
}

cli::cli_alert("Create subsetted cohort - complete! - {Sys.time()}")

###############################################################################
# ASSESS MEDOC CONCEPT COVERAGE
###############################################################################
cli::cli_alert("Assessing MEDOC concept coverage - {Sys.time()}")

genomic_codes <- safe_eval(
  CodelistGenerator::getCandidateCodes(
    cdm = cdm,
    keywords = c("PD-L1","PDL1","EGFR","KRAS","ALK1","ROS1","BRAF","NTRK",
                 "ERBB2","TP53","BRCA1","BRCA2","CDH1","PALB2","PTEN","TP53",
                 "PIK3CA","AKT","ESR1","STK11","HER2", "PSA", ),
    domains = "Measurement",
    includeDescendants = FALSE
  ) %>% dplyr::pull("concept_id"),
  fallback = integer(),
  msg = "getCandidateCodes (genomic) failed"
)

tnm_codes <- safe_read_csv(here::here("inst/code_lists/tnm_codes.csv"), fileEncoding = "UTF-8-BOM")
tumour_stage_codes <- if ("measurement_concept_id" %in% names(tnm_codes)) tnm_codes$measurement_concept_id else integer()

histological_cell_type <- safe_eval(
  check_icdo3_matches(cdm),
  fallback = tibble(),
  msg = "check_icdo3_matches failed"
)

lookup <- safe_read_csv(here::here("inst/medoc_concept_lookup.csv"), fileEncoding = "UTF-8-BOM")

json_folder_path <- here::here("inst/concepts")
medoc_concept_codes <- safe_eval(
  if (dir.exists(json_folder_path)) CodelistGenerator::codesFromConceptSet(json_folder_path, cdm) else tibble(),
  fallback = tibble(),
  msg = "codesFromConceptSet failed"
)

medoc_concept_table <- tibble()
if ("medoc_concept" %in% names(lookup) && nrow(lookup) > 0) {
  medoc_concept_table <- safe_eval(
    purrr::map_dfr(unique(lookup$medoc_concept), evaluate_concept),
    fallback = tibble(),
    msg = "evaluate_concept failed"
  )
} else {
  cli::cli_alert_warning("lookup missing medoc_concept or empty; medoc_concept_table will be empty.")
}

episode_table <- safe_eval(
  if (exists("sql_dialect")) check_tables(conn, sql_dialect) else check_tables(conn),
  fallback = tibble(),
  msg = "check_tables failed"
)

# Bind histology checks safely
medoc_concept_table <- safe_eval(
  dplyr::bind_rows(medoc_concept_table, histological_cell_type),
  fallback = medoc_concept_table,
  msg = "bind_rows(histological_cell_type) failed"
)

medoc_concept_table <- safe_eval(
  postprocess_concept_table(medoc_concept_table),
  fallback = medoc_concept_table,
  msg = "postprocess_concept_table failed"
)

cli::cli_alert("Assessing MEDOC concept coverage - complete! - {Sys.time()}")

###############################################################################
# GENERATE SUMMARY OF PRIMARY DIAGNOSIS CONCEPTS
###############################################################################
cli::cli_alert("Summarising diagnosis codes - {Sys.time()}")

cancer_codelist <- safe_eval(
  CodelistGenerator::getCandidateCodes(
    cdm = cdm,
    keywords = c("cancer","Primary Malignancy","Neoplasm","Lymphoma","Carcinoma",
                 "Melanoma","Leukemia","Panmyelosis","Primary malignant neoplasm",
                 "Cancer","Malignant","neoplasm","Tumor","tumor",
                 "adamantinoma","adenocarcinoma","sarcoma","astrocytoma",
                 "astroblastoma","carcinofibroma","chordoma",
                 "malignant","blastoma","seminoma","paraganglioma","neoplasia",
                 "glioma","Dysgerminoma","Ectomesenchymoma","carcinoid",
                 "Ependymoma","hemangioendothelioma",
                 "thrombocythemia","paraganglioma","tumour","ganglioma",
                 "seminoma","germinona","gastrioma","gliomatosis","Glucagonoma","Hodgkin","lymphoproliferative",
                 "Insulinoma","Langerhans","Medulloepithelioma","Mycosis fungoides",
                 "Myelodysplastic","neurocytoma","Oligodendroglioma","Paget",
                 "Paraganglioma","Pheochromocytoma","myeloma","Plasmacytoma",
                 "Polyembryoma","mesothelioma","myelofibrosis","oligodendroglioma",
                 "Sezary syndrome","Somatostatinoma","Vipoma","macroglobulinemia",
                 "paraganglioma","hemangioendothelioma","thrombocythemia","Gastrinoma",
                 "heavy chain disease","Medulloepithelioma"),
    domains = "Condition",
    includeDescendants = FALSE
  ) %>% dplyr::pull("concept_id"),
  fallback = integer(),
  msg = "getCandidateCodes (cancer) failed"
)

primary_snapshot <- summarise_concept_counts_safe(
  cdm_table = cdm$condition_occurrence,
  concept_id_col = "condition_concept_id",
  concept_table = cdm$concept,
  codelist = cancer_codelist
)

primary_snap_sliced <- slice_top_counts_safe(primary_snapshot, n = 20)

cli::cli_alert("Summarising diagnosis codes - complete! - {Sys.time()}")

###############################################################################
# GENERATE SUMMARY OF METASTASIS CONCEPTS
###############################################################################
cli::cli_alert("Summarising metastasis codes - {Sys.time()}")

mets_meas_codes <- safe_eval(
  CodelistGenerator::getCandidateCodes(
    cdm = cdm,
    keywords = c("metastasis", "metastatic", "mets", "metastases"),
    domains = "Measurement",
    includeDescendants = TRUE
  ) %>% dplyr::pull("concept_id"),
  fallback = integer(),
  msg = "getCandidateCodes (mets measurement) failed"
)

mets_snapshot_meas_raw <- summarise_concept_counts_safe(
  cdm_table = cdm$measurement,
  concept_id_col = "measurement_concept_id",
  concept_table = cdm$concept,
  codelist = mets_meas_codes
)

mets_snapshot_meas <- slice_top_counts_safe(mets_snapshot_meas_raw, n = 20)

mets_cond_codes <- safe_eval(
  CodelistGenerator::getCandidateCodes(
    cdm = cdm,
    keywords = c("metastasis", "metastatic", "mets", "metastases"),
    domains = "Condition",
    includeDescendants = TRUE
  ) %>% dplyr::pull("concept_id"),
  fallback = integer(),
  msg = "getCandidateCodes (mets condition) failed"
)

mets_snapshot_cond_raw <- summarise_concept_counts_safe(
  cdm_table = cdm$condition_occurrence,
  concept_id_col = "condition_concept_id",
  concept_table = cdm$concept,
  codelist = mets_cond_codes
)

mets_snap_sliced_condition <- slice_top_counts_safe(mets_snapshot_cond_raw, n = 20)

cli::cli_alert("Summarising metastasis codes - complete! - {Sys.time()}")

###############################################################################
# ASSESS CANCER STAGING CODES (backend-safe booleans)
###############################################################################
cli::cli_alert("Summarising Cancer staging checks - {Sys.time()}")

staging_stored_as_value <- FALSE
value_as_concept_id_contains_tumour_stage <- FALSE
tumour_stage_stored_in_measurement_concept_id <- FALSE

if (cdm_has(cdm, "measurement")) {
  
  staging_stored_as_value <- safe_eval(
    cdm$measurement %>%
      dplyr::summarise(flag = max(dplyr::if_else(measurement_concept_id == 4111627, 1L, 0L))) %>%
      dplyr::pull(flag) == 1L,
    fallback = FALSE,
    msg = "staging stored-as-value check failed"
  )
  
  if (cdm_has(cdm, "main_cohort") && length(tumour_stage_codes) > 0) {
    
    value_as_concept_id_contains_tumour_stage <- safe_eval(
      cdm$measurement %>%
        dplyr::inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
        dplyr::mutate(is_match = value_as_concept_id %in% tumour_stage_codes) %>%
        dplyr::summarise(flag = max(dplyr::if_else(is_match, 1L, 0L))) %>%
        dplyr::pull(flag) == 1L,
      fallback = FALSE,
      msg = "value_as_concept_id staging check failed"
    )
    
    tumour_stage_stored_in_measurement_concept_id <- safe_eval(
      cdm$measurement %>%
        dplyr::inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
        dplyr::mutate(is_match = measurement_concept_id %in% tumour_stage_codes) %>%
        dplyr::summarise(flag = max(dplyr::if_else(is_match, 1L, 0L))) %>%
        dplyr::pull(flag) == 1L,
      fallback = FALSE,
      msg = "measurement_concept_id staging check failed"
    )
  }
}

tnm_result <- tibble(
  `staging stored as value` = staging_stored_as_value,
  `value_as_concept_id contains tumour stage` = value_as_concept_id_contains_tumour_stage,
  `tumour stage stored in measurement_concept_id` = tumour_stage_stored_in_measurement_concept_id
)

cli::cli_alert("Summarising Cancer staging checks - complete! - {Sys.time()}")

###############################################################################
# ASSESS CANCER DRUG CONCEPTS AND COVERAGE (safe even if none)
###############################################################################
cli::cli_alert("Summarising cancer drug therapies - {Sys.time()}")

drugs_file_path <- here::here("inst/code_lists/drug_list.csv")
drug_code_list <- safe_read_csv(drugs_file_path)

summary_immuno_drugs <- safe_eval(execute_drug_checks("immunotherapy"), fallback = tibble(), msg = "execute_drug_checks immunotherapy failed")
summary_chemo_drugs  <- safe_eval(execute_drug_checks("chemotherapy"),  fallback = tibble(), msg = "execute_drug_checks chemotherapy failed")
summary_therapy_drugs<- safe_eval(execute_drug_checks("targeted therapy"), fallback = tibble(), msg = "execute_drug_checks targeted therapy failed")

cli::cli_alert("Summarising cancer drug therapies - complete! - {Sys.time()}")

###############################################################################
# ASSESS RADIOTHERAPY CONCEPTS AND COVERAGE
###############################################################################
cli::cli_alert("Summarising radiotherapy concept checks - {Sys.time()}")

radiotherapy_codes_path <- here::here("inst/code_lists/radiotherapy_codes.csv")

summary_radiotherapy <- safe_eval(
  execute_rt_checks(cdm, radiotherapy_codes_path),
  fallback = tibble(),
  msg = "execute_rt_checks failed"
)

radiotherapy_dose_result <- safe_eval(
  check_radiation_dose_info(cdm),
  fallback = tibble(),
  msg = "check_radiation_dose_info failed"
)

cli::cli_alert("Summarising radiotherapy concept checks - complete! - {Sys.time()}")

###############################################################################
# ASSESS PROCEDURES COVERAGE
###############################################################################
cli::cli_alert("Summarising all procedure concept checks - {Sys.time()}")

summary_procedure <- safe_eval(
  execute_procedure_checks(cdm),
  fallback = tibble(),
  msg = "execute_procedure_checks failed"
)

cli::cli_alert("Summarising all procedure concept checks - complete! - {Sys.time()}")

###############################################################################
# ASSESS GENOMIC CONCEPT COVERAGE
###############################################################################
cli::cli_alert("Summarising genomic concept coverage - {Sys.time()}")

gene_snap <- summarise_concept_counts_safe(
  cdm_table = cdm$measurement,
  concept_id_col = "measurement_concept_id",
  concept_table = cdm$concept,
  codelist = genomic_codes
)

gene_snapshot <- gene_snap %>%
  dplyr::filter(
    !is.na(concept_name),
    concept_name != "overall",
    !grepl("pyogenes", concept_name, ignore.case = TRUE),
    !grepl("general", concept_name, ignore.case = TRUE),
    !grepl("stool", concept_name, ignore.case = TRUE)
  )

gene_snap_sliced <- slice_top_counts_safe(gene_snapshot, n = 20)

cli::cli_alert("Summarising genomic concept coverage - complete! - {Sys.time()}")

###############################################################################
# MVP TABLE 
###############################################################################
if (!exists("medoc_concept_table") || is.null(medoc_concept_table) || nrow(as.data.frame(medoc_concept_table)) == 0) {
  table5 <- tibble(
    `MEDOC concept` = character(),
    Result = logical(),
    `Percentage of patients` = numeric()
  )
} else {
  
  df <- as.data.frame(medoc_concept_table)
  cn <- names(df)
  cn_lower <- tolower(cn)
  
  medoc_col <- cn[cn_lower %in% c("medoc concept", "medoc_concept", "medocconcept")]
  if (length(medoc_col) == 0) {
    idx <- which(str_detect(cn_lower, "medoc"))
    medoc_col <- if (length(idx) > 0) cn[idx[1]] else NA_character_
  } else {
    medoc_col <- medoc_col[1]
  }
  
  result_col <- cn[cn_lower %in% c("result", "present", "variable is present", "variable_is_present")]
  if (length(result_col) == 0) {
    idx <- which(str_detect(cn_lower, "result|present"))
    result_col <- if (length(idx) > 0) cn[idx[1]] else NA_character_
  } else {
    result_col <- result_col[1]
  }
  
  pct_col <- cn[cn_lower %in% c("percentage of patients", "percent of patients", "pct_patients", "percent_patients")]
  if (length(pct_col) == 0) {
    idx <- which(str_detect(cn_lower, "percent|percentage"))
    pct_col <- if (length(idx) > 0) cn[idx[1]] else NA_character_
  } else {
    pct_col <- pct_col[1]
  }
  
  if (is.na(medoc_col)) df$`MEDOC concept` <- NA_character_
  if (is.na(result_col)) df$Result <- NA
  if (is.na(pct_col)) df$`Percentage of patients` <- NA_real_
  
  table5 <- df %>%
    {
      tmp <- .
      if (!is.na(medoc_col)) tmp <- tmp %>% rename(`MEDOC concept` = !!medoc_col)
      if (!is.na(result_col)) tmp <- tmp %>% rename(Result = !!result_col)
      if (!is.na(pct_col)) tmp <- tmp %>% rename(`Percentage of patients` = !!pct_col)
      tmp
    } %>%
    mutate(
      `MEDOC concept` = as.character(`MEDOC concept`),
      Result = as.logical(Result),
      `Percentage of patients` = suppressWarnings(as.numeric(`Percentage of patients`))
    ) %>%
    select(`MEDOC concept`, Result, `Percentage of patients`) %>%
    distinct()
}

mvp_concepts <- c(
  "date_of_birth",
  "sex",
  "primary_cancer_diagnosis",
  "primary_diagnosis_date",
  "disease_stage",
  "histological_cell_type",
  "radiotherapy_start_date",
  "surgery_type",
  "date_of_death",
  "metastasis_presence",
  "metastasis_location"
)

table5 <- table5 %>%
  mutate(
    Result = (Result %in% TRUE) &
      !is.na(`Percentage of patients`) &
      (`Percentage of patients` > 0)
  )

medoc_mvp <- table5 %>%
  filter(`MEDOC concept` %in% mvp_concepts)

missing_from_table5 <- setdiff(mvp_concepts, medoc_mvp$`MEDOC concept`)

if ("histological_cell_type" %in% missing_from_table5) {
  
  if (exists("histological_cell_type") && !is.null(histological_cell_type) && nrow(as.data.frame(histological_cell_type)) > 0) {
    
    hdf <- as.data.frame(histological_cell_type)
    hcn <- names(hdf)
    hcn_lower <- tolower(hcn)
    
    h_medoc_col <- hcn[hcn_lower %in% c("medoc concept", "medoc_concept", "medocconcept", "medoc_concept")]
    if (length(h_medoc_col) == 0) {
      idx <- which(str_detect(hcn_lower, "medoc"))
      h_medoc_col <- if (length(idx) > 0) hcn[idx[1]] else NA_character_
    } else {
      h_medoc_col <- h_medoc_col[1]
    }
    
    h_result_col <- hcn[hcn_lower %in% c("result", "present", "variable is present", "variable_is_present")]
    if (length(h_result_col) == 0) {
      idx <- which(str_detect(hcn_lower, "result|present"))
      h_result_col <- if (length(idx) > 0) hcn[idx[1]] else NA_character_
    } else {
      h_result_col <- h_result_col[1]
    }
    
    h_pct_col <- hcn[hcn_lower %in% c("percent_pass", "percentage of patients", "percent of patients", "pct_patients", "percent_patients")]
    if (length(h_pct_col) == 0) {
      idx <- which(str_detect(hcn_lower, "percent|percentage"))
      h_pct_col <- if (length(idx) > 0) hcn[idx[1]] else NA_character_
    } else {
      h_pct_col <- h_pct_col[1]
    }
    
    hist_any_pass <- hdf %>%
      mutate(
        .res = if (!is.na(h_result_col)) as.logical(.data[[h_result_col]]) else NA,
        .pct = if (!is.na(h_pct_col)) suppressWarnings(as.numeric(.data[[h_pct_col]])) else NA_real_
      ) %>%
      summarise(
        any_present = any(.res %in% TRUE, na.rm = TRUE),
        pct = suppressWarnings(max(.pct, na.rm = TRUE))
      )
    
    hist_pct <- hist_any_pass$pct
    if (is.infinite(hist_pct)) hist_pct <- NA_real_
    
    hist_row <- tibble(
      `MEDOC concept` = "histological_cell_type",
      Result = (hist_any_pass$any_present %in% TRUE) &
        !is.na(hist_pct) &
        (hist_pct > 0),
      `Percentage of patients` = hist_pct
    )
    
    medoc_mvp <- bind_rows(medoc_mvp, hist_row)
    
  } else {
    medoc_mvp <- bind_rows(
      medoc_mvp,
      tibble(`MEDOC concept` = "histological_cell_type",
             Result = NA,
             `Percentage of patients` = NA_real_)
    )
  }
}

if ("disease_stage" %in% missing_from_table5) {
  
  stage_row <- tibble(`MEDOC concept` = "disease_stage",
                      Result = NA,
                      `Percentage of patients` = NA_real_)
  
  if (exists("cdm") &&
      !is.null(cdm) &&
      "main_cohort" %in% names(cdm) &&
      exists("tumour_stage_codes") &&
      !is.null(tumour_stage_codes) &&
      length(tumour_stage_codes) > 0) {
    
    total_cohort_patients <- tryCatch({
      cdm$main_cohort %>% distinct(subject_id) %>% count() %>% pull(n)
    }, error = function(e) NA_integer_)
    
    if (!is.na(total_cohort_patients) && total_cohort_patients > 0 && "measurement" %in% names(cdm)) {
      
      stage_patients <- tryCatch({
        cdm$measurement %>%
          inner_join(cdm$main_cohort, by = c("person_id" = "subject_id")) %>%
          filter(
            measurement_concept_id %in% tumour_stage_codes |
              value_as_concept_id %in% tumour_stage_codes |
              measurement_concept_id == 4111627
          ) %>%
          distinct(person_id) %>%
          count() %>%
          pull(n)
      }, error = function(e) NA_integer_)
      
      if (!is.na(stage_patients)) {
        stage_row <- tibble(
          `MEDOC concept` = "disease_stage",
          Result = (stage_patients > 0),
          `Percentage of patients` = round((stage_patients / total_cohort_patients) * 100, 2)
        )
      }
    }
  }
  
  medoc_mvp <- bind_rows(medoc_mvp, stage_row)
}

pct_molecule_generic <- if (exists("pct_molecule_generic")) pct_molecule_generic else NA_real_
pct_anti_cancer_treatment <- if (exists("pct_anti_cancer_treatment")) pct_anti_cancer_treatment else NA_real_
pct_dose_form <- if (exists("pct_dose_form")) pct_dose_form else NA_real_

if (is.na(pct_molecule_generic) || is.na(pct_anti_cancer_treatment) || is.na(pct_dose_form)) {
  
  total_cohort_patients <- if (exists("total_cohort_patients")) total_cohort_patients else NA_integer_
  if (is.na(total_cohort_patients) && exists("cdm") && !is.null(cdm) && "main_cohort" %in% names(cdm)) {
    total_cohort_patients <- tryCatch({
      cdm$main_cohort %>% distinct(subject_id) %>% count() %>% pull(n)
    }, error = function(e) NA_integer_)
  }
  
  if (exists("all_drugs") &&
      !is.null(all_drugs) &&
      nrow(as.data.frame(all_drugs)) > 0) {
    
    ad <- as.data.frame(all_drugs)
    req_cols <- c("n_patients", "n_records", "proportion_of_records_with_dose_form")
    has_cols <- all(req_cols %in% names(ad))
    
    if (has_cols && !is.na(total_cohort_patients) && total_cohort_patients > 0) {
      
      all_drugs2 <- as_tibble(ad) %>%
        mutate(
          dose_form_count = suppressWarnings(as.numeric(str_extract(
            as.character(proportion_of_records_with_dose_form), "^[0-9]+"
          )))
        )
      
      total_drug_patients <- sum(all_drugs2$n_patients, na.rm = TRUE)
      
      pct_molecule_generic <- round((total_drug_patients / total_cohort_patients) * 100, 2)
      pct_anti_cancer_treatment <- pct_molecule_generic
      
      total_dose_form_count <- sum(all_drugs2$dose_form_count, na.rm = TRUE)
      total_records <- sum(all_drugs2$n_records, na.rm = TRUE)
      
      pct_dose_form <- if (!is.na(total_records) && total_records > 0) {
        round((total_dose_form_count / total_records) * 100, 2)
      } else {
        NA_real_
      }
      
    } else {
      pct_molecule_generic <- pct_molecule_generic
      pct_anti_cancer_treatment <- pct_anti_cancer_treatment
      pct_dose_form <- pct_dose_form
    }
    
  } else {
    pct_molecule_generic <- ifelse(is.na(pct_molecule_generic), 0, pct_molecule_generic)
    pct_anti_cancer_treatment <- ifelse(is.na(pct_anti_cancer_treatment), 0, pct_anti_cancer_treatment)
    pct_dose_form <- ifelse(is.na(pct_dose_form), 0, pct_dose_form)
  }
}

derived_rows <- tibble(
  `MEDOC concept` = c("molecule_generic_name", "anti_cancer_treatment_name", "drug_dose"),
  `Percentage of patients` = c(pct_molecule_generic, pct_anti_cancer_treatment, pct_dose_form)
) %>%
  mutate(
    Result = !is.na(`Percentage of patients`) & (`Percentage of patients` > 0)
  )

medoc_mvp_updated <- bind_rows(medoc_mvp, derived_rows) %>%
  rename(`Variable is present` = Result) %>%
  mutate(
    `Percentage of patients` = suppressWarnings(as.numeric(`Percentage of patients`)),
    `MVP pass` = (`Variable is present` %in% TRUE) &
      !is.na(`Percentage of patients`) &
      (`Percentage of patients` > 0)
  ) %>%
  arrange(match(`MEDOC concept`,
                c(mvp_concepts, "molecule_generic_name", "anti_cancer_treatment_name", "drug_dose")))

n_true  <- sum(medoc_mvp_updated$`MVP pass`, na.rm = TRUE)
n_total <- sum(!is.na(medoc_mvp_updated$`MVP pass`))

pct_total_coverage <- ifelse(n_total > 0, (n_true / n_total) * 100, NA_real_)

coverage_row <- tibble(
  `MVP total coverage` = "MVP total coverage",
  `Percentage of patients` = round(pct_total_coverage, 2)
) %>%
  mutate(
    `MVP result` = case_when(
      `Percentage of patients` >= 70.0 ~ "MVP has been reached",
      `Percentage of patients` < 70.0 ~ "MVP not reached",
      TRUE ~ "MVP coverage not computable"
    )
  )


###############################################################################
# ENSURE ALL TABLE-LIKE OBJECTS ARE RENDERABLE (prevents Rmd failures)
###############################################################################
cdm_overall_snapshot <- ensure_renderable(cdm_overall_snapshot)
cdm_snapshot_clinical_condition <- ensure_renderable(cdm_snapshot_clinical_condition)
cdm_snapshot_clinical_drug <- ensure_renderable(cdm_snapshot_clinical_drug)
cdm_snapshot_clinical_meas <- ensure_renderable(cdm_snapshot_clinical_meas)
cdm_snapshot_obs <- ensure_renderable(cdm_snapshot_obs)

mappingCompleteness <- ensure_renderable(mappingCompleteness)

medoc_concept_table <- ensure_renderable(medoc_concept_table)
episode_table <- ensure_renderable(episode_table)
histological_cell_type <- ensure_renderable(histological_cell_type)

primary_snap_sliced <- ensure_renderable(primary_snap_sliced)
mets_snapshot_meas <- ensure_renderable(mets_snapshot_meas)
mets_snap_sliced_condition <- ensure_renderable(mets_snap_sliced_condition)
gene_snap_sliced <- ensure_renderable(gene_snap_sliced)

tnm_result <- ensure_renderable(tnm_result)

summary_immuno_drugs <- ensure_renderable(summary_immuno_drugs)
summary_chemo_drugs <- ensure_renderable(summary_chemo_drugs)
summary_therapy_drugs <- ensure_renderable(summary_therapy_drugs)

summary_radiotherapy <- ensure_renderable(summary_radiotherapy)
radiotherapy_dose_result <- ensure_renderable(radiotherapy_dose_result)

summary_procedure <- ensure_renderable(summary_procedure)
medoc_mvp_updated <- ensure_renderable(medoc_mvp_updated)
###############################################################################
# CREATE OUTPUT REPORT AND CODELISTS
###############################################################################
cli::cli_alert("Rendering output report and generating full codelists - {Sys.time()}")

timestamp <- format(Sys.time(), "%Y-%m-%d_%H%M")  # filesystem-safe
out_dir <- here::here("inst/output_report/")
ensure_dir(out_dir)

html_ok <- render_safe(
  "inst/onboarding_report_template.Rmd",
  output_format = "html_document",
  output_file = paste0("MEDOC_cdm_report_", centre, "_", timestamp, ".html"),
  output_dir = out_dir,
  params = list(centre = centre, author = author)
)

word_ok <- render_safe(
  "inst/onboarding_report_template.Rmd",
  output_format = "word_document",
  output_file = paste0("MEDOC_cdm_report_", centre, "_", timestamp, ".docx"),
  output_dir = out_dir,
  params = list(centre = centre, author = author)
)

cli::cli_alert("DigiONE onboarding complete! Outputs saved to 'inst/output_report'. HTML ok={html_ok}; Word ok={word_ok}")
