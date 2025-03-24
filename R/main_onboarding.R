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


### system info

### get source, name, cdm info
cdm_details <- get_cdm_details(conn, db_name, omop_name)

### record counts per omop table 
cdm_record_counts <- get_cdm_counts(cdm)

cdm_snapshot_clinical <- OmopSketch::summariseClinicalRecords(cdm, c("condition_occurrence", "drug_exposure", "measurement")) |>
  tableClinicalRecords()

#for the above, where we find the source vocab - try to replicate this but find the date associated?
#add some instructions how to interpret the table (mainly variable section)

cdm_snapshot_obs <- OmopSketch::summariseObservationPeriod(cdm$observation_period) |>
  tableObservationPeriod()

### mapping completeness (vocab mappings)
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


########### MEDOC ##############################

##### check through which variables are present and where
results <- check_omop_variables(cdm, omop_variables_to_check, medoc_concepts)


## as with above, do a check for location of primary diagnosis (i.e. how we identify it) and if mets diagnoses are available

#1 search cdm for codes to find: which tables and variables are concept ids stored for: primary malig, metastasis/ secondary malig

cancer_codes <- read.csv(here::here('/inst/cancer_diag_codes.csv'))

primary_result <- check_cancer_codes(cancer_codes, "primary", cdm)

cancer <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("cancer", "primary malignancy", "neoplasm"),
  domains = "Condition",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")

mets <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("metastases"),
  domains = "Condition",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")

primary_snapshot <- OmopSketch::summariseConceptSetCounts(cdm, 
                          conceptSet = list("cancer" = cancer),
                          countBy = "person") %>%
 # plotConceptSetCounts()
  


####
metastasis_result <- check_cancer_codes(cancer_codes, "metastasis", cdm)
acetaminophen <- getCandidateCodes(
  cdm = cdm,
  keywords = "acetaminophen",
  domains = "Drug",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")



#tnm coding 
tnm_codes <- read.csv(here::here('/inst/tnm_codes.csv'))
tnm_result <- check_tnm(cdm, tnm_codes)

#3 what is the pattern of primary diagnosis - is it present throughout the treatment period, is it superceded by mets diagnosis 
#4 what is the frequency of diagnosis - is it only present at one time point per year, or at each visit 
diag_pattern_result <- check_diag_pattern(cdm, cancer_codes)
# this is not functional yet


### drug therapy - number and % with dosage info, & where date of death comes before end date of treatment 
drugs_file_path <- here::here('/inst/drug_list.csv')
drug_code_list <- read.csv(drugs_file_path)
summary_immuno_drugs <- execute_drug_checks('immunotherapy')
summary_chemo_drugs <- execute_drug_checks('chemotherapy')
summary_therapy_drugs <- execute_drug_checks('targeted therapy')

### radiotherapy - number and % with valid RT treatment
radiotherapy_codes_path <- here::here('/inst/radiotherapy_codes.csv')
summary_radiotherapy <- execute_rt_checks(cdm, radiotherapy_codes_path) 

#### radiotherapy dosage availability
radiotherapy_dose_result <- check_radiation_dose_info(cdm)

### patients with results of biomarker (use common cancer ones from MEDOC)


### genomic readiness? how to measure this 
timestamp <- Sys.Date()
rmarkdown::render("/inst/onboarding_report_template.Rmd", 
                  output_format = "html_document",
                  output_file = paste0("MEDOC_cdm_report_", centre, "_", timestamp, ".html"),
                  output_dir = here::here("/inst/output_report/"),
                  params = list(centre = centre, author = author))
