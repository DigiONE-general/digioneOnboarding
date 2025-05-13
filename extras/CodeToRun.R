
install.packages('renv')
renv::restore()

# Complete the credentials - please provide a response for all
cdmSchema = "" # omop instance name
writeSchema = "" # schema to write to
tablePrefix <- "" # table prefix - will be written to write schema
db_name <- "" # 
omop_name <- ""
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

source(here::here('/main_onboarding.R'))