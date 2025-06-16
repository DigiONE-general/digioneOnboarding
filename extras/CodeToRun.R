
install.packages('renv')
renv::restore()

# Complete the credentials - please provide a response for all
cdmSchema = "" # omop instance name
writeSchema = "" # schema to write to
tablePrefix <- "" # table prefix - will be written to write schema
db_name <- "" # 
omop_schema_name <- ""
centre <- ""
author <- ""
sql_dialect <- "" #either: 'snowflake', 'mysql', 'postgresql', 'sqlite', 'sqlserver', 'redshift'

# connection details - refer to this doc : https://dbi.r-dbi.org/reference/dbConnect.html
conn <- DBI::dbConnect(odbc::odbc(),
                       Driver = "",
                       server = "",
                       port = 443,
                       Role = "",
                       Database = cdmSchema,
                       Warehouse = '',
                       Uid = Sys.getenv(""),
                       Authenticator = "")


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

write.csv(primary_snapshot, paste0(resultsFolder, "/primary_diagnosis_codes.csv"))
write.csv(mets_snapshot, paste0(resultsFolder, "/metastasis_diagnosis_codes_conditions.csv"))
write.csv(mets_snapshot_meas, paste0(resultsFolder, "/metastasis_diagnosis_codes_measurement.csv"))
write.csv(gene_snapshot, paste0(resultsFolder, "/genetic_codes.csv"))