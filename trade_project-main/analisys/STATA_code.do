****************************************************
* PPML PIPELINE FOR THESIS
* Topic: Analysis and forecasting of medical equipment
* imports to Russia under sanctions pressure
*
* This script:
* 1) loads the prepared country-month panel;
* 2) checks panel structure and data quality;
* 3) runs preliminary linear FE checks;
* 4) estimates the main PPML model;
* 5) runs robustness checks;
* 6) exports result tables.
****************************************************



****************************************************
* 0. PROJECT PATHS
* Set the main directories for data, logs, and outputs.
* Replace the global proj path with your own local path.
****************************************************
global proj "/Users/your_name/path_to_project"
global data "$proj/data_prepared_for_stata"
global out  "$proj/stata_output"
global logdir "$proj/logs"
clear all
set more off
set linesize 255
capture log close

global proj "C:\users\crossover\Desktop\My Mac Desktop\ДИПЛОМ\trade_project-main\analisys"
global data "$proj/data"
global out  "$proj/stata_output"
global logdir "$proj/logs"

cap mkdir "$out"
cap mkdir "$logdir"

log using "$logdir/ppml_pipeline.log", replace text

****************************************************
* 1. INSTALL REQUIRED PACKAGES
* ppmlhdfe  - main estimator for PPML with high-dimensional FE
* reghdfe   - useful for linear FE checks
* ftools    - dependency for reghdfe/ppmlhdfe
* estout    - export regression tables
* xtserial  - Wooldridge-type serial correlation test in panel
****************************************************
cap which ppmlhdfe
if _rc ssc install ppmlhdfe, replace

cap which reghdfe
if _rc ssc install reghdfe, replace

cap which ftools
if _rc ssc install ftools, replace

cap which estout
if _rc ssc install estout, replace

cap which outreg2
if _rc ssc install outreg2, replace

cap which coefplot
if _rc ssc install coefplot, replace

cap which xtserial
if _rc net install st0039, from(http://www.stata-journal.com/software/sj3-2/)

cap which xtcsd
if _rc ssc install xtcsd, replace

cap which require
if _rc ssc install require, replace

****************************************************
* 2. LOAD THE MAIN COUNTRY-MONTH PANEL
* This is the baseline panel for the core PPML model.
****************************************************
use "$data/country_month_panel.dta", clear

* Apply monthly date format for readability in Stata
format stata_mdate %tm

****************************************************
* 3. BASIC DATA INSPECTION
* Check variable structure, coding, and key identifiers.
****************************************************
describe
codebook country country_id rep_date stata_mdate

* Verify that the panel key is unique:
* one observation per country per month
isid country_id stata_mdate


****************************************************
* 4. DESCRIPTIVE CHECKS
* Summary stats for the main variables used in the model.
****************************************************
summ value sanctions_proxy sanctions_proxy_smooth ///
     cpi_yoy ip_yoy ex_yoy gscpi distw logistics_exposure_distw ///
     unfriendly brics cis post_sanctions ///
     unfriendly_post brics_post cis_post


****************************************************
* 5. MISSING VALUES CHECK
* Important before running FE or PPML models.
****************************************************
misstable summarize value sanctions_proxy sanctions_proxy_smooth ///
    cpi_yoy ip_yoy ex_yoy gscpi distw logistics_exposure_distw ///
    unfriendly brics cis post_sanctions ///
    unfriendly_post brics_post cis_post
	
	
****************************************************
* 6. ZERO FLOWS CHECK
* PPML is especially useful because trade data often contain zeros.
****************************************************
gen value_zero = (value==0) if !missing(value)
sum value_zero


****************************************************
* 7. DISTRIBUTION OF THE DEPENDENT VARIABLE
* These plots help justify why log-OLS is not the main estimator:
* - value is non-negative
* - many zeros may be present
* - positive values are often highly skewed
****************************************************
histogram value, fraction
histogram value if value>0, fraction

* Log of positive values only, just for diagnostics
gen ln_value_pos = ln(value) if value>0
histogram ln_value_pos, fraction


****************************************************
* 8. PANEL DECLARATION
* Country is the panel unit, monthly date is the time variable.
****************************************************
xtset country_id stata_mdate

* Show panel balance / structure
xtdescribe


****************************************************
* 9. SIMPLE CORRELATION CHECK
* This is not a formal multicollinearity solution,
* but it helps to see whether raw and smoothed sanctions proxies
* are very strongly related.
****************************************************
pwcorr sanctions_proxy sanctions_proxy_smooth ///
       cpi_yoy ip_yoy ex_yoy logistics_exposure_distw, sig star(0.05)
	   
	   
****************************************************
* 10. CHECK WHICH VARIABLES ARE ABSORBED BY FIXED EFFECTS
*
* With country FE and month FE:
* - post_sanctions is absorbed by month FE
* - gscpi is absorbed by month FE
* - distw is absorbed by country FE
* - unfriendly, brics, cis are absorbed by country FE
*
* These regressions are only technical checks to confirm the logic.
****************************************************
reghdfe value post_sanctions, absorb(country_id stata_mdate) vce(cluster country_id)
reghdfe value gscpi, absorb(country_id stata_mdate) vce(cluster country_id)
reghdfe value distw, absorb(country_id stata_mdate) vce(cluster country_id)
reghdfe value unfriendly brics cis, absorb(country_id stata_mdate) vce(cluster country_id)


****************************************************
* 11. PRELIMINARY LINEAR FE BLOCK
*
* These models are not the final models for the thesis.
* They are used only as auxiliary justification:
* - pooled OLS vs FE vs RE
* - Hausman test
* - checking likely heteroskedasticity and serial correlation
*
* The main model remains PPML.
****************************************************
preserve

* Keep only positive trade flows because log(value) is undefined at zero
keep if value>0

* Log-transform positive import values for linear comparison only
gen ln_value = ln(value)

****************************************************
* 11.1 POOLED OLS
****************************************************
reg ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw
est store pooled

****************************************************
* 11.2 FIXED EFFECTS MODEL
* Month fixed effects are included via i.stata_mdate.
****************************************************
xtreg ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw i.stata_mdate, fe vce(cluster country_id)
est store fe_model

****************************************************
* 11.3 RANDOM EFFECTS MODEL
****************************************************
xtreg ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw i.stata_mdate, re vce(cluster country_id)
est store re_model

****************************************************
* 11.4 HAUSMAN TEST
* For Hausman, it is safer to run non-robust FE and RE versions.
* This test is used only as an auxiliary FE vs RE argument.
****************************************************
xtreg ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw i.stata_mdate, fe
est store fe_nr

xtreg ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw i.stata_mdate, re
est store re_nr

hausman fe_nr re_nr, sigmamore

****************************************************
* 11.5 ERROR-STRUCTURE CHECKS IN THE LINEAR ANALOG
*
* xttest3  - modified Wald test for groupwise heteroskedasticity
* xtserial - serial correlation test in panel data
*
* These tests help explain why clustered SE are needed and why
* linear log models are not ideal as the main estimator.
****************************************************
xtreg ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw i.stata_mdate, fe

xttest3
xtserial ln_value sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
         cpi_yoy ip_yoy ex_yoy logistics_exposure_distw

restore

****************************************************
* 12. MAIN PPML MODEL
*
* This is the core model for the thesis report.
*
* Dependent variable:
*   value = import value
*
* Regressors:
*   sanctions_proxy_smooth       - main sanctions pressure proxy
*   unfriendly_post             - differential post-2022 effect for unfriendly states
*   brics_post                  - differential post-2022 effect for BRICS
*   cis_post                    - differential post-2022 effect for CIS
*   cpi_yoy, ip_yoy, ex_yoy     - macro controls
*   logistics_exposure_distw    - logistics shock proxy interacting time and geography
*
* Fixed effects:
*   country_id                  - controls for time-invariant country heterogeneity
*   stata_mdate                 - controls for common monthly shocks
*
* Standard errors:
*   clustered by country_id
****************************************************
ppmlhdfe value ///
    sanctions_proxy_smooth_l4 ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///

est store ppml_main


****************************************************
* 13. SEMI-ELASTICITY INTERPRETATION
* Convert selected PPML coefficients into percentage effects:
* exp(beta)-1
****************************************************
lincom exp(_b[sanctions_proxy_smooth]) - 1
lincom exp(_b[unfriendly_post]) - 1
lincom exp(_b[brics_post]) - 1
lincom exp(_b[cis_post]) - 1
