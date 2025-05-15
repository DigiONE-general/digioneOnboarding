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
cdm_details <- get_cdm_details(conn, db_name, omop_schema_name)
cdm_record_counts <- get_cdm_counts(cdm)
cdm_overall_snapshot <- OmopSketch::summariseOmopSnapshot(cdm) |>
  OmopSketch::tableOmopSnapshot()

cdm_snapshot_clinical <- OmopSketch::summariseClinicalRecords(cdm, c("condition_occurrence", "drug_exposure", 
                                                                     "measurement")) |>
  tableClinicalRecords()

#for the above, where we find the source vocab - try to replicate this but find the date associated?
#add some instructions how to interpret the table (mainly variable section)

cdm_snapshot_obs <- OmopSketch::summariseObservationPeriod(cdm$observation_period) |>
  tableObservationPeriod()

OmopSketch::summariseObservationPeriod(cdm$observation_period) %>% plotObservationPeriod(colour = "observation_period_ordinal")

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
medoc_variable_locations <- check_omop_variables(cdm, omop_variables_to_check, medoc_concepts)
#this is done BUT cdmconnector doesn't load in episode table
episode_table <- check_tables(conn, sql_dialect)

## as with above, do a check for location of primary diagnosis (i.e. how we identify it) and if mets diagnoses are available
#1 search cdm for codes to find: which tables and variables are concept ids stored for: primary malig, metastasis/ secondary malig

cancer_codelist <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("cancer", "primary malignancy", "neoplasm", "lymphoma", "carcinoma", "melanoma", "leukemia", "panmyelosis",
               "tumor", "adamantinoma", "adenocarcinoma", "sarcoma", "astrocytoma", "astroblastoma", "carcinofibroma", "chordoma",
               "malignant", "blastoma", "seminoma", "paraganglioma", "neoplasia", "glioma", "Dysgerminoma", "Ectomesenchymoma", "carcinoid", "Ependymoma", "hemangioendothelioma",
               "thrombocythemia", "paraganglioma", "tumour", "ganglioma", "seminoma", "germinona", "gastrioma", "gliomatosis", "Glucagonoma", "Hodgkin", "lymphoproliferative",
               "Insulinoma", "Langerhans", "Medulloepithelioma", "Mycosis fungoides", "Myelodysplastic", "neurocytoma", "Oligodendroglioma", "Paget", "Paraganglioma",
               "Pheochromocytoma", "myeloma", "Plasmacytoma", "Polyembryoma", "mesothelioma", "myelofibrosis", "oligodendroglioma", "Sezary syndrome", "Somatostatinoma",
               "Vipoma", "macroglobulinemia", "paraganglioma", "hemangioendothelioma", "thrombocythemia", "Gastrinoma", "heavy chain disease", "Medulloepithelioma"),
  domains = "Condition",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")



primary_snapshot <- OmopSketch::summariseConceptSetCounts(cdm, 
                          conceptSet = list("cancer" = cancer_codelist),
                          countBy = "person") 
 # plotConceptSetCounts()

mets <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("metastasis", "metastatic", "mets", "metastases"),
  domains = "Measurement",
  includeDescendants = TRUE
) |>
  dplyr::pull("concept_id")



mets_snapshot <- OmopSketch::summariseConceptSetCounts(cdm, 
                                                          conceptSet = list("mets" = mets),
                                                          countBy = "person") 


cancer_codes <- read.csv(here::here('inst/code_lists/cancer_diag_codes.csv'))
cancer_codes <- cancer_codes %>% filter(grepl('Ameloblast', name)) 

#primary_result <- check_cancer_codes(cancer_codes, "primary", cdm)  
#metastasis_result <- check_cancer_codes(cancer_codes, "metastasis", cdm)

primary_snap <- primary_snapshot %>% mutate(concept_name = sapply(strsplit(additional_level, " &&& "), function(x) x[1]),
                                            domain = sapply(strsplit(additional_level, " &&& "), function(x) x[5]),
                                            concept_id = sapply(strsplit(additional_level, " &&& "), function(x) x[2])) %>%
                                     group_by(concept_name, domain, concept_id) %>%
                                     summarise(total_patient_count = sum(as.numeric(estimate_value))) %>%
                                     filter(concept_name != 'overall') %>%
                                     arrange(desc(total_patient_count)) %>%
                                     mutate(total_patient_count = ifelse(total_patient_count < 5, '>5', as.character(total_patient_count)))
primary_snap_sliced <- head(primary_snap, 20)


mets_snap <- mets_snapshot %>% mutate(concept_name = sapply(strsplit(additional_level, " &&& "), function(x) x[1]),
                                      domain = sapply(strsplit(additional_level, " &&& "), function(x) x[5]),
                                      concept_id = sapply(strsplit(additional_level, " &&& "), function(x) x[2])) %>%
                               group_by(concept_name, domain, concept_id) %>%
                               summarise(total_patient_count = sum(as.numeric(estimate_value))) %>%
                               filter(concept_name != 'overall') %>%
                               arrange(desc(total_patient_count)) %>%
                               mutate(total_patient_count = ifelse(total_patient_count < 5, '>5', as.character(total_patient_count)))
mets_snap_sliced <- head(mets_snap, 20)

#tnm coding 
tnm_codes <- read.csv(tnm_codes_path)

tumour_stage_codes <- tnm_codes$measurement_concept_id

staging_stored_as_value <- cdm$measurement %>%
  summarise(present = any(measurement_concept_id == 4111627)) %>%
  pull(present)

value_as_concept_id_contains_tumour_stage <- cdm$measurement %>%
  filter(measurement_concept_id == 4111627) %>%
  summarise(present = any(value_as_concept_id %in% tumour_stage_codes)) %>%
  pull(present)

tumour_stage_stored_in_measurement_concept_id <- cdm$measurement %>%
  summarise(present = any(measurement_concept_id %in% tumour_stage_codes)) %>%
  pull(present)

tnm_result <- tibble(
  `staging stored as value` = staging_stored_as_value,
  `value_as_concept_id contains tumour stage` = value_as_concept_id_contains_tumour_stage,
  `tumour stage stored in measurement_concept_id` = tumour_stage_stored_in_measurement_concept_id
)

#3 what is the pattern of primary diagnosis - is it present throughout the treatment period, is it superceded by mets diagnosis 
#4 what is the frequency of diagnosis - is it only present at one time point per year, or at each visit 
#diag_pattern_result <- check_diag_pattern(cdm, cancer_codes)
# this is not functional yet


### drug therapy - number and % with dosage info, & where date of death comes before end date of treatment 
drugs_file_path <- here::here('inst/code_lists/drug_list.csv')
drug_code_list <- read.csv(drugs_file_path)
summary_immuno_drugs <- execute_drug_checks('immunotherapy')
summary_chemo_drugs <- execute_drug_checks('chemotherapy')
summary_therapy_drugs <- execute_drug_checks('targeted therapy')

### radiotherapy - number and % with valid RT treatment
radiotherapy_codes_path <- here::here('inst/code_lists/radiotherapy_codes.csv')
summary_radiotherapy <- execute_rt_checks(cdm, radiotherapy_codes_path) 

#### radiotherapy dosage availability
radiotherapy_dose_result <- check_radiation_dose_info(cdm)

### patients with results of biomarker (use common cancer ones from MEDOC)

genomic_codes <- CodelistGenerator::getCandidateCodes(
  cdm = cdm,
  keywords = c("mutation", "genetic", "gene", "deletion", "duplication"),
  domains = "Measurement",
  includeDescendants = FALSE
) |>
  dplyr::pull("concept_id")

genetic_snapshot <- OmopSketch::summariseConceptSetCounts(cdm, 
                                                       conceptSet = list("genomic_codes" = genomic_codes),
                                                       countBy = "person") 

gene_snap <- genetic_snapshot %>% mutate(concept_name = sapply(strsplit(additional_level, " &&& "), function(x) x[1]),
                                      domain = sapply(strsplit(additional_level, " &&& "), function(x) x[5]),
                                      concept_id = sapply(strsplit(additional_level, " &&& "), function(x) x[2])) %>%
  group_by(concept_name, domain, concept_id) %>%
  summarise(total_patient_count = sum(as.numeric(estimate_value))) %>%
  filter(concept_name != 'overall', !(grepl('pyogenes', concept_name)), !(grepl('general', concept_name))) %>%
  arrange(desc(total_patient_count)) %>%
  mutate(total_patient_count = ifelse(total_patient_count < 5, '>5', as.character(total_patient_count)))
gene_snap_sliced <- head(gene_snap, 20)

timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M")

rmarkdown::render("inst/onboarding_report_template.Rmd", 
                  output_format = "html_document",
                  output_file = paste0("MEDOC_cdm_report_", centre, "_", timestamp, ".html"),
                  output_dir = here::here("inst/output_report/"),
                  params = list(centre = centre, author = author))
