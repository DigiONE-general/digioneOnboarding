
install.packages('renv')
renv::restore()

############ Complete the credentials - please provide a response for all #############
cdmSchema = "" # omop instance name
writeSchema = "" # schema to write to
tablePrefix <- "" # table prefix - will be written to write schema
db_name <- "" #provide name of the database
omop_schema_name <- "" # omop instance name
centre <- "example centre" #provide the name of your hospital or abbreviation
author <- "example author" #provide your name 
sql_dialect <- "postgresql" #either: 'snowflake', 'mysql', 'postgresql', 'sqlite', 'sqlserver', 'redshift'

# connection details - refer to this doc : https://dbi.r-dbi.org/reference/dbConnect.html
conn <- DBI::dbConnect(odbc::odbc(),
                       Driver = "",
                       server = "",
                       port = 443, #for postgres: 5432, for snowflake: 443, for  oracle: 1521, for sql server: 1433
                       Role = "",
                       Database = cdmSchema,
                       Warehouse = '',
                       Uid = Sys.getenv(""),
                       Authenticator = "")

############# no amendments below here required #########################################
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
