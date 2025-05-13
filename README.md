# DigiONE Onboarding

## Description

This repository contains scripts and resources for the checking an OMOP instances' compliance with the Minimal Essential Description of Cancer (MEDOC) from DigiONE

The goal of the code is to scan the OMOP instance and compile key information relating to the CDM and vocabulary versions, mapping completedness, as well as collating specific information on how MEDOC concepts are stored, available and represented in the database

## Contents

-   **R:** R scripts containing functions and code for running the onboarding tool
-   **Inst:** Cotains markdown template, list of concept ids specific to MEDOC for use in code and output results (output_report and code_lists)
-   **Extras:** Code to run

## How to run

The only script which requires interaction from the user is /extras/CodeToRun.R.
An example is provided which provides connection details for using dbConnect for your database ( see: https://dbi.r-dbi.org/reference/dbConnect.html )


## Output

- **Output report:** Saved to /inst/output_report - provides a full summary of your databases compliance with MEDOC. Maintains patient number aggregation a '>5'. Contains no disclosive information
- **Code lists:** Saved to /inst/code_lists - provides a full list of all concept codes for: primary cancer diagnoses, metastases diagnoses, genomic biomarker codes 
