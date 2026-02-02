# DigiONE Onboarding

## Description

This repository contains scripts and resources for the checking an OMOP instances' compliance with the Minimal Essential Description of Cancer (MEDOC) from DigiONE

The goal of the code is to scan the OMOP instance and compile key information relating to the CDM and vocabulary versions, mapping completedness, as well as collating specific information on how MEDOC concepts are stored, available and represented in the database

## Contents

-   **R:** R scripts containing functions and code for running the onboarding tool
-   **Inst:** Cotains markdown template, list of concept ids specific to MEDOC for use in code and output results (output_report and code_lists)
-   **Extras:** Code to run

## How to run

1. Clone this repo to your R environment
2. Ensure you open using R projects (double clicking on .Rproj file or open using Rstudio or application menu)
3. The only script which requires interaction from the user is /extras/CodeToRun.R.
An example is provided which provides connection details for using dbConnect for your database ( see: https://dbi.r-dbi.org/reference/dbConnect.html )
Please complete the CodeToRun.R script according to your database server specifications.
4. Run the complete CodeToRun.R script (ensuring you have activated the environment, completed pacakge installations)
5. Once complete (estimated time between 30minutes to 1.5 hour depending on database size, disk space etc), find the outputs as detailed below


## Output

- **Output report:** Saved to /inst/output_report - provides a full summary of your databases compliance with MEDOC. Maintains patient number aggregation a '>5'. Contains no disclosive information
- **Code lists:** Saved to /inst/code_lists - provides a full list of all concept codes for: primary cancer diagnoses, metastases diagnoses, genomic biomarker codes 
