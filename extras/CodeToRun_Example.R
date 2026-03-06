
#install.packages('renv')
renv::restore()
renv::activate()

cdmSchema = c("EXT_OMOPV5_USA_ONCEMR", "FULL_M202112_OMOP_V5")
writeSchema = c("PA_USA_ONCEMR", "STUDY_REFERENCE")
tablePrefix <- "test_"
db_name <- "EXT_OMOPV5_USA_ONCEMR"
omop_schema_name <- "FULL_M202112_OMOP_V5"
centre <- 'Example Centre'
author <- 'Example Author'
sql_dialect <- "snowflake" #either: 'snowflake', 'mysql', 'postgresql', 'sqlite', 'sqlserver', 'redshift'


conn <- DBI::dbConnect(odbc::odbc(),
                       Driver = "SnowflakeDSIIDriver",
                       server = "iqviaidporg-omop_pa.snowflakecomputing.com",
                       port = 443,
                       Role = "OMOP_DATASCIENTIST_UK",
                       Database = cdmSchema,
                       Warehouse = 'PA_USA_ONCEMR_MEDIUM',
                       Uid = Sys.getenv("SNOWFLAKE_USER"),
                       Authenticator = "SNOWFLAKE_JWT",
                       PRIV_KEY_FILE = "/mnt/rstudio_data/INTERNAL.IMSGLOBAL.COM/u1191672/rsa_key.p8")


cdm <- CDMConnector::cdmFromCon(con = conn,
                                cdmSchema = cdmSchema,
                                writeSchema = writeSchema,
                                writePrefix  = tablePrefix)

source(here::here('R/helper_functions.R'))
source(here::here('inst/concepts_list.R'))
source(here::here('R/main_onboarding.R'))



resultsFolder <- here::here("inst/output_report/instance_codelists")
if (!dir.exists(resultsFolder)) {
  dir.create(resultsFolder, recursive = TRUE)
}

if (exists("primary_snapshot") &&
    is.data.frame(primary_snapshot) &&
    nrow(primary_snapshot) > 0) {
  write.csv(primary_snapshot,
            paste0(resultsFolder, "/primary_diagnosis_codes.csv"),
            row.names = FALSE)
}

if (exists("mets_snapshot_cond_raw") &&
    is.data.frame(mets_snapshot_cond_raw) &&
    nrow(mets_snapshot_cond_raw) > 0) {
  write.csv(mets_snapshot_cond_raw,
            paste0(resultsFolder, "/metastasis_diagnosis_codes_conditions.csv"),
            row.names = FALSE)
}

if (exists("mets_snapshot_meas_raw") &&
    is.data.frame(mets_snapshot_meas_raw) &&
    nrow(mets_snapshot_meas_raw) > 0) {
  write.csv(mets_snapshot_meas_raw,
            paste0(resultsFolder, "/metastasis_diagnosis_codes_measurement.csv"),
            row.names = FALSE)
}

if (exists("gene_snapshot") &&
    is.data.frame(gene_snapshot) &&
    nrow(gene_snapshot) > 0) {
  write.csv(gene_snapshot,
            paste0(resultsFolder, "/genetic_codes.csv"),
            row.names = FALSE)
}

if (exists("medoc_mvp_updated") &&
    is.data.frame(medoc_mvp_updated) &&
    nrow(medoc_mvp_updated) > 0) {
  write.csv(medoc_mvp_updated,
            paste0(resultsFolder, "/medoc_mvp.csv"),
            row.names = FALSE)
}

