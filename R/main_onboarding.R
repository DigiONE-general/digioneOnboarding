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

###############################################################################
# GET CDM DETAILS FROM CDM_SOURCE

cli::cli_alert("Gathering CDM details - {Sys.time()}")

cdm_details <- get_cdm_details(conn, db_name, omop_schema_name)

cdm_record_counts <- get_cdm_counts(cdm)

cdm_overall_snapshot <- OmopSketch::summariseOmopSnapshot(cdm) %>%
  OmopSketch::tableOmopSnapshot() %>%
  as.data.frame(cdm_overall_snapshot['_data'])
colnames(cdm_overall_snapshot) <- c("Level", "Variable", "Value")

cli::cli_alert("Gathering CDM details - complete! - {Sys.time()}")

###############################################################################
# CREATE SUMMARIES OF CLINICAL AND OBSERVATION TABLES

cli::cli_alert("Creating clinical snapshot tables - {Sys.time()}")

cdm_snapshot_clinical <- OmopSketch::summariseClinicalRecords(cdm, c("condition_occurrence", "drug_exposure", 
                                                                     "measurement")) %>% tableClinicalRecords() %>%
                                                as.data.frame(cdm_snapshot_clinical['_data'])
colnames(cdm_snapshot_clinical) <- c("Omop Table", "Variable", "Response", "Measurement", "Value")  
cdm_snapshot_clinical_condition <- cdm_snapshot_clinical %>% 
                                   filter(`Omop Table`== 'condition_occurrence') %>% 
                                   select(-`Omop Table`)
cdm_snapshot_clinical_drug <- cdm_snapshot_clinical %>% 
                              filter(`Omop Table`== 'drug_exposure') %>% 
                              select(-`Omop Table`)
cdm_snapshot_clinical_meas <- cdm_snapshot_clinical %>% 
                              filter(`Omop Table`== 'measurement') %>% 
                              select(-`Omop Table`)

cdm_snapshot_obs <- OmopSketch::summariseObservationPeriod(cdm$observation_period) %>%
                    tableObservationPeriod() %>%
                    as.data.frame(cdm_snapshot_obs['_data']) %>%
                    select(-`Variable level`)
colnames(cdm_snapshot_obs) <- c("Observation Period", "Variable", "Measurement", "Value")

cli::cli_alert("Creating clinical snapshot tables - complete! - {Sys.time()}")

###############################################################################
# CHECK VOCABULARY MAPPING COMPLETEDNESS FOR ALL MEDOC VARIABLES

cli::cli_alert("Assessing vocabulary mapping completedness - {Sys.time()}")

mappings <- list(
  list(table = cdm$visit_occurrence, source_value = "visit_source_value", concept_id = "visit_concept_id", description = "visit"),
  list(table = cdm$observation, source_value = "observation_source_value", concept_id = "observation_concept_id", description = "observation"),
  list(table = cdm$observation, source_value = "unit_source_value", concept_id = "unit_concept_id", description = "observation unit"),
  list(table = cdm$condition_occurrence, source_value = "condition_source_value", concept_id = "condition_concept_id", description = "condition"),
  list(table = cdm$condition_occurrence, source_value = "condition_status_source_value", concept_id = "condition_status_concept_id", description = "condition status"),
  list(table = cdm$death, source_value = "cause_source_value", concept_id = "cause_concept_id", description = "death cause"),
  list(table = cdm$procedure_occurrence, source_value = "procedure_source_value", concept_id = "procedure_concept_id", description = "procedure occurrence"),
  list(table = cdm$measurement, source_value = "measurement_source_value", concept_id = "measurement_concept_id", description = "measurement"),
  list(table = cdm$measurement, source_value = "unit_source_value", concept_id = "unit_concept_id", description = "measurement unit"),
  list(table = cdm$measurement, source_value = "value_source_value", concept_id = "value_as_concept_id", description = "measurement value"),
  list(table = cdm$drug_exposure, source_value = "drug_source_value", concept_id = "drug_concept_id", description = "drug exposure"),
  list(table = cdm$drug_exposure, source_value = "route_source_value", concept_id = "route_concept_id", description = "drug route"),
  list(table = cdm$provider, source_value = "specialty_source_value", concept_id = "specialty_concept_id", description = "provider speciality")
)

results <- list()
for (mapping in mappings) {
  result <- process_vocab_table(mapping$table, mapping$source_value, mapping$concept_id, mapping$description)
  results <- append(results, list(result))
}

mappingCompleteness <- do.call(rbind, results)
mappingCompleteness <- mappingCompleteness %>%
  arrange(domain) %>%
  mutate(Domain = domain,
         `#Codes Source` = num_codes_source,
         `#Codes Mapped` = num_codes_mapped,
         `%Codes Mapped` = percent_codes_mapped,
         `#Records Source` = num_records_source,
         `#Records Mapped` = num_records_mapped,
         `%Records Mapped` = percent_records_mapped,
         .keep = "none")

cli::cli_alert("Assessing vocabulary mapping completedness - complete! - {Sys.time()}")


###############################################################################
# CREATE SUBSETTED CANCER COHORT

cli::cli_alert("Create subsetted cohort - {Sys.time()}")

path_to_json <- here::here("inst/cohorts/")

cohort_details <- CDMConnector::readCohortSet(path_to_json) |>
  dplyr::mutate(cohort_name = snakecase::to_snake_case(cohort_name))

cdm <- CDMConnector::generateCohortSet(
  cdm = cdm,
  cohortSet = cohort_details,
  name = "main_cohort")

cdm$main_cohort <- cdm$main_cohort %>% PatientProfiles::addCohortName() %>%
  select(1,cohort_name, everything())

cli::cli_alert("Create subsetted cohort - complete! - {Sys.time()}")

###############################################################################
# ASSESS MEDOC CONCEPT COVERAGE 

cli::cli_alert("Assessing MEDOC concept coverage - {Sys.time()}")

genomic_codes <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("PD-L1", "PDL1", "EGFR", "KRAS", "ALK1", "ROS1", "BRAF", "NTRK",
               "ERBB2", "TP53", "BRCA1", "BRCA2", "CDH1", "PALB2", "PTEN", "TP53",
               "PIK3CA", "AKT", "ESR1", "STK11", "HER2"),
  domains = "Measurement",
  includeDescendants = FALSE
) |>
  dplyr::pull("concept_id")

tnm_codes <- read.csv(here::here('inst/code_lists/tnm_codes.csv'), fileEncoding = "UTF-8-BOM")
tumour_stage_codes <- tnm_codes$measurement_concept_id

histological_cell_type <- check_icdo3_matches(cdm)

lookup <- read.csv(here::here('inst/medoc_concept_lookup.csv'), fileEncoding = "UTF-8-BOM")

json_folder_path <- here::here("inst/concepts")
medoc_concept_codes <- CodelistGenerator::codesFromConceptSet(json_folder_path, cdm)

medoc_concept_table <- map_dfr(unique(lookup$medoc_concept), evaluate_concept)
episode_table <- check_tables(conn, sql_dialect)

medoc_concept_table <- medoc_concept_table %>% rbind(histological_cell_type)


medoc_concept_table <- postprocess_concept_table(medoc_concept_table)


cli::cli_alert("Assessing MEDOC concept coverage - complete! - {Sys.time()}")


###############################################################################
# GENERATE SUMMARY OF PRIMARY DIAGNOSIS CONCEPTS 

cli::cli_alert("Summarising diagnosis codes - {Sys.time()}")

cancer_codelist <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("cancer", "Primary Malignancy", "Neoplasm", "Lymphoma", "Carcinoma", 
               "Melanoma", "Leukemia", "Panmyelosis", "Primary malignant neoplasm",
               "Cancer", "Malignant", "neoplasm", "Tumor", "tumor", 
               "adamantinoma", "adenocarcinoma", "sarcoma", "astrocytoma", 
               "astroblastoma", "carcinofibroma", "chordoma",
               "malignant", "blastoma", "seminoma", "paraganglioma", "neoplasia", 
               "glioma", "Dysgerminoma", "Ectomesenchymoma", "carcinoid", 
               "Ependymoma", "hemangioendothelioma",
               "thrombocythemia", "paraganglioma", "tumour", "ganglioma", 
               "seminoma", "germinona", "gastrioma", "gliomatosis", "Glucagonoma", "Hodgkin", "lymphoproliferative",
               "Insulinoma", "Langerhans", "Medulloepithelioma", "Mycosis fungoides", 
               "Myelodysplastic", "neurocytoma", "Oligodendroglioma", "Paget", 
               "Paraganglioma", "Pheochromocytoma", "myeloma", "Plasmacytoma", 
               "Polyembryoma", "mesothelioma", "myelofibrosis", "oligodendroglioma", 
               "Sezary syndrome", "Somatostatinoma", "Vipoma", "macroglobulinemia", 
               "paraganglioma", "hemangioendothelioma", "thrombocythemia", "Gastrinoma", 
               "heavy chain disease", "Medulloepithelioma"),
  domains = "Condition",
  includeDescendants = FALSE
) |>
  dplyr::pull("concept_id")

primary_snapshot <- summarise_concept_counts(
  cdm_table = cdm$condition_occurrence,
  concept_id_col = "condition_concept_id",
  concept_table = cdm$concept,
  codelist = cancer_codelist
)

primary_snap_sliced <- primary_snapshot %>%
  arrange(desc(person_id_count)) %>%
  head(20) %>%
  mutate(total_patient_count = ifelse(person_id_count < 5, '<5', as.character(person_id_count))) %>%
  select(-person_id_count)


cli::cli_alert("Summarising diagnosis codes - complete! - {Sys.time()}")

###############################################################################
# GENERATE SUMMARY OF METASTASIS CONCEPTS

cli::cli_alert("Summarising metastasis codes - {Sys.time()}")

mets <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("metastasis", "metastatic", "mets", "metastases"),
  domains = "Measurement",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")

mets_snapshot <- summarise_concept_counts(
  cdm_table = cdm$measurement,
  concept_id_col = "measurement_concept_id",
  concept_table = cdm$concept,
  codelist = mets
)
mets_snapshot_meas <- mets_snapshot %>%
  arrange(desc(person_id_count)) %>%
  head(20) %>%
  mutate(total_patient_count = ifelse(person_id_count < 5, '<5', as.character(person_id_count))) %>%
  select(-person_id_count)

#mets_snapshot_meas <- mets_snapshot

mets <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("metastasis", "metastatic", "mets", "metastases"),
  domains = "Condition",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")

mets_snapshot <- summarise_concept_counts(
  cdm_table = cdm$condition_occurrence,
  concept_id_col = "condition_concept_id",
  concept_table = cdm$concept,
  codelist = mets
)
mets_snap_sliced_condition <- mets_snapshot %>%
  arrange(desc(person_id_count)) %>%
  head(20) %>%
  mutate(total_patient_count = ifelse(person_id_count < 5, '<5', as.character(person_id_count))) %>%
  select(-person_id_count)

cli::cli_alert("Summarising metastasis codes - complete! - {Sys.time()}")

###############################################################################
# ASSESS CANCER STAGING CODES 

cli::cli_alert("Summarising Cancer staging checks - {Sys.time()}")

staging_stored_as_value <- cdm$measurement %>%
  summarise(present = any(measurement_concept_id == 4111627)) %>%
  pull(present)

value_as_concept_id_contains_tumour_stage <- cdm$measurement %>%
  inner_join(cdm$main_cohort, by = c('person_id' = 'subject_id')) %>%
  mutate(is_match = value_as_concept_id %in% tumour_stage_codes) %>%
  summarise(any_present = max(as.integer(is_match), na.rm = TRUE)) %>%
  pull(any_present) == 1

tumour_stage_stored_in_measurement_concept_id <- cdm$measurement %>%
  inner_join(cdm$main_cohort, by = c('person_id' = 'subject_id')) %>%
  mutate(is_match = measurement_concept_id %in% tumour_stage_codes) %>%
  summarise(any_present = max(as.integer(is_match), na.rm = TRUE)) %>%
  pull(any_present) == 1 

tnm_result <- tibble(
  `staging stored as value` = staging_stored_as_value,
  `value_as_concept_id contains tumour stage` = value_as_concept_id_contains_tumour_stage,
  `tumour stage stored in measurement_concept_id` = tumour_stage_stored_in_measurement_concept_id
)

cli::cli_alert("Summarising Cancer staging checks - complete! - {Sys.time()}")


###############################################################################
# ASSESS CANCER DRUG CONCEPTS AND COVERAGE

cli::cli_alert("Summarising cancer drug therapies - {Sys.time()}")

drugs_file_path <- here::here('inst/code_lists/drug_list.csv')
drug_code_list <- read.csv(drugs_file_path)
summary_immuno_drugs <- execute_drug_checks('immunotherapy')
summary_chemo_drugs <- execute_drug_checks('chemotherapy')
summary_therapy_drugs <- execute_drug_checks('targeted therapy')

cli::cli_alert("Summarising cancer drug therapies - complete! - {Sys.time()}")

###############################################################################
# ASSESS RADIOTHERAPY CONCEPTS AND COVERAGE

cli::cli_alert("Summarising radiotherapy concept checks - {Sys.time()}")

radiotherapy_codes_path <- here::here('inst/code_lists/radiotherapy_codes.csv')
summary_radiotherapy <- execute_rt_checks(cdm, radiotherapy_codes_path) 

radiotherapy_dose_result <- check_radiation_dose_info(cdm)

cli::cli_alert("Summarising radiotherapy concept checks - complete! - {Sys.time()}")

###############################################################################
# ASSESS PROCEDURES COVERAGE 

cli::cli_alert("Summarising all procedure concept checks - {Sys.time()}")

summary_procedure <- execute_procedure_checks(cdm) 

cli::cli_alert("Summarising all procedure concept checks - complete! - {Sys.time()}")

###############################################################################
# ASSESS GENOMIC CONCEPT COVERAGE

cli::cli_alert("Summarising genomic concept coverage - {Sys.time()}")

 gene_snap <- summarise_concept_counts(
   cdm_table = cdm$measurement,
   concept_id_col = "measurement_concept_id",
   concept_table = cdm$concept,
   codelist = genomic_codes
 )

 gene_snapshot <- gene_snap %>% filter(concept_name != 'overall', !(grepl('pyogenes', concept_name)), !(grepl('general', concept_name)), !(grepl('Stool', concept_name))) 

 gene_snap_sliced <- gene_snapshot %>%
   arrange(desc(person_id_count)) %>%
   head(20) %>%
   mutate(total_patient_count = ifelse(person_id_count < 5, '<5', as.character(person_id_count))) %>%
   select(-person_id_count)
 
 cli::cli_alert("Summarising genomic concept coverage - complete! - {Sys.time()}")

###############################################################################


#anti cancer treatment name and molecule generic name and drug dates and dose comes from summary drugs
medoc_mvp <- medoc_concept_table %>% filter(`MEDOC concept` == 'date_of_birth'| 
                                            `MEDOC concept` == 'sex'|
                                            `MEDOC concept` == 'primary_cancer_diagnosis' | 
                                            `MEDOC concept` == 'primary_diagnosis_date' |
                                            `MEDOC concept` == 'disease_stage' | #histological cell type - get from other table 
                                            `MEDOC concept` == 'radiotherapy_start_date' |
                                            `MEDOC concept` == 'surgery_type' |
                                            `MEDOC concept` == 'date_of_death' |
                                            `MEDOC concept` == 'metastasis_presence' |
                                            `MEDOC concept` == 'metastasis_location') 

medoc_mvp_hist <- histological_cell_type 
names(medoc_mvp_hist) <- names(medoc_mvp)

medoc_mvp <- rbind(medoc_mvp, medoc_mvp_hist)

total_cohort_patients <- cdm$main_cohort %>% 
  distinct(subject_id) %>% 
  count() %>% 
  pull(n)

all_drugs <- bind_rows(
  summary_therapy_drugs,
  summary_chemo_drugs,
  summary_immuno_drugs
)

all_drugs <- all_drugs %>%
  mutate(
    dose_form_count = as.numeric(str_extract(
      proportion_of_records_with_dose_form, "^[0-9]+"
    ))
  )

total_drug_patients <- sum(all_drugs$n_patients, na.rm = TRUE)

pct_molecule_generic <- (total_drug_patients / total_cohort_patients) * 100

pct_anti_cancer_treatment <- pct_molecule_generic

total_dose_form_count <- sum(all_drugs$dose_form_count, na.rm = TRUE)
total_records <- sum(all_drugs$n_records, na.rm = TRUE)

pct_dose_form <- (total_dose_form_count / total_records) * 100

new_rows <- tibble(
  `MEDOC concept` = c(
    "molecule generic name",
    "anti cancer treatment name",
    "drug dose"
  ),
  Check = c(
    "derived from drug summary tables",
    "derived from molecule generic name",
    "derived from drug summary tables"
  ),
  `OMOP Table` = c("drug_exposure", "drug_exposure", "drug_exposure"),
  Result = c(TRUE, TRUE, TRUE),
  `Percentage of patients` = c(
    round(pct_molecule_generic, 2),
    round(pct_anti_cancer_treatment, 2),
    round(pct_dose_form, 2)
  )
)

medoc_mvp_updated <- bind_rows(medoc_mvp, new_rows)

medoc_mvp_updated <- medoc_mvp_updated %>% 
                     select(-Check, -`OMOP Table`) %>%
                     rename(`Variable is present` = Result)


n_true <- sum(medoc_mvp_updated$`Variable is present`, na.rm = TRUE)
n_total <- nrow(medoc_mvp_updated)
pct_total_coverage <- (n_true / n_total) * 100

coverage_row <- tibble(
  `MVP total coverage` = "MVP total coverage",
  `Percentage of patients` = round(pct_total_coverage, 2)
)

coverage_row <- coverage_row %>% mutate(`MVP result` = case_when(`Percentage of patients` >= 75.0 ~ 'Pass: MVP exceeded',
                                                                 `Percentage of patients` >= 50.0 & `Percentage of patients` < 75.0 ~ 'Pass: MVP reached',
                                                                 `Percentage of patients` < 50.0 ~ 'MVP not reached'))

###############################################################################
# CREATE OUTPUT REPORT AND CODELISTS 
 
cli::cli_alert("Rendering output report and generating full codelists - {Sys.time()}")

timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M")

rmarkdown::render("inst/onboarding_report_template.Rmd", 
                  output_format = "html_document",
                  output_file = paste0("MEDOC_cdm_report_", centre, "_", timestamp, ".html"),
                  output_dir = here::here("inst/output_report/"),
                  params = list(centre = centre, author = author))


rmarkdown::render("inst/onboarding_report_template.Rmd", 
                  output_format = "word_document",
                  output_file = paste0("MEDOC_cdm_report_", centre, "_", timestamp, ".docx"),
                  output_dir = here::here("inst/output_report/"),
                  params = list(centre = centre, author = author))

cli::cli_alert("DigiONE onboarding complete! Report and codelists have been generated in 'inst/output_report' folder. Thank you")
