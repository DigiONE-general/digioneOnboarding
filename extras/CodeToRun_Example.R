
#install.packages('renv')
renv::restore()

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

write.csv(primary_snap, paste0(resultsFolder, "/primary_diagnosis_codes.csv"))
write.csv(mets_snap, paste0(resultsFolder, "/metastasis_diagnosis_codes.csv"))
write.csv(gene_snap, paste0(resultsFolder, "/genetic_codes.csv"))
