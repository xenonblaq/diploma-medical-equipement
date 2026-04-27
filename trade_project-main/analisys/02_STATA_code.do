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
clear all
set more off
set linesize 255
capture log close

global proj "C:\Users\Admin\Desktop\ДИПЛОМ. GIT\diploma-medical-equipement\trade_project-main\analisys"
global data "$proj/data"
global out  "$proj/stata_output"
global logdir "$proj/logs"

cap mkdir "$out"
cap mkdir "$logdir"

log using "$logdir/ppml_pipeline.log", replace text


****************************************************
* REQUIRED PACKAGES
****************************************************
cap which require
if _rc ssc install require, replace

cap which ftools
if _rc net install ftools, from("https://raw.githubusercontent.com/sergiocorreia/ftools/master/src/")

cap which reghdfe
if _rc net install reghdfe, from("https://raw.githubusercontent.com/sergiocorreia/reghdfe/master/src/")

cap which ppmlhdfe
if _rc ssc install ppmlhdfe, replace

cap which esttab
if _rc ssc install estout, replace

cap which winsor2
if _rc ssc install winsor2, replace


****************************************************
* 01. COUNTRY-MONTH PANEL: DATA CHECK
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm

* 1) Ключ панели должен быть уникален
isid country_id stata_mdate

* 2) Проверка баланса панели
xtset country_id stata_mdate
xtdescribe

* 3) Пропуски по ключевым переменным
misstable summarize value sanctions_proxy_smooth ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
    unfriendly_post brics_post cis_post

* 4) Доля нулей
gen value_zero = (value==0) if !missing(value)
sum value_zero

* 5) Распределение зависимой переменной
histogram value, fraction
histogram value if value>0, fraction
gen ln_value_pos = ln(value) if value>0
histogram ln_value_pos, fraction

****************************************************
* 02. AUXILIARY FE JUSTIFICATION ON POSITIVE FLOWS
****************************************************
preserve
keep if value>0
gen ln_value = ln(value)

* pooled OLS
reg ln_value sanctions_proxy_smooth ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw
est store pooled

* FE with month dummies
xtset country_id stata_mdate
xtreg ln_value sanctions_proxy_smooth ///
      unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
      i.stata_mdate, fe vce(cluster country_id)
est store fe_model

* RE
xtreg ln_value sanctions_proxy_smooth ///
      unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
      i.stata_mdate, re vce(cluster country_id)
est store re_model

* Formal FE vs RE check (non-robust version for Hausman)
xtreg ln_value sanctions_proxy_smooth ///
      unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
      i.stata_mdate, fe
est store fe_nr

xtreg ln_value sanctions_proxy_smooth ///
      unfriendly_post brics_post cis_post ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
      i.stata_mdate, re
est store re_nr

hausman fe_nr re_nr, sigmamore

restore


****************************************************
* TEST OF TIME FIXED EFFECTS IN FE MODEL
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm

keep if value > 0
gen ln_value = ln(value)

xtset country_id stata_mdate

* FE model with time dummies
xtreg ln_value ///
    sanctions_proxy_smooth ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
    i.stata_mdate, ///
    fe vce(cluster country_id)

est store fe_time

* Joint significance of all time fixed effects
testparm i.stata_mdate

****************************************************
* ERROR-STRUCTURE DIAGNOSTICS IN LINEAR FE ANALOG
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

preserve
keep if value>0
gen ln_value = ln(value)

* FE analogue
xtreg ln_value sanctions_proxy_smooth ///
      cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
      i.stata_mdate, fe

* Serial correlation diagnostic
capture noisily xtserial ln_value sanctions_proxy_smooth ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw

restore


****************************************************
* HETEROSKEDASTICITY IN LINEAR LOG-ANALOG
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm

preserve
keep if value>0
gen ln_value = ln(value)

reg ln_value sanctions_proxy_smooth ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
    i.country_id i.stata_mdate

estat hettest
restore

****************************************************
* 03. NON-NESTED IDENTIFICATION CHECK:
* WHY SANCTIONS AND BLOC MODELS SHOULD BE ESTIMATED SEPARATELY
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

* A. sanctions model only
ppmlhdfe value ///
    sanctions_proxy_smooth ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store cm_sanctions_only

* B. combined model
ppmlhdfe value ///
    sanctions_proxy_smooth ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store cm_combined

* C. bloc model only
ppmlhdfe value ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store cm_bloc_only

esttab cm_sanctions_only cm_combined cm_bloc_only ///
    using "$out\identification_check.rtf", replace ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    keep(sanctions_proxy_smooth unfriendly_post brics_post cis_post ///
         cpi_yoy ip_yoy ex_yoy logistics_exposure_distw) ///
    title("Identification check: sanctions proxy vs bloc interactions")
	
	
****************************************************
* M1. COUNTRY-MONTH SANCTIONS MODEL
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)

est store M1_cm_sanctions

* Semi-elasticity of sanctions proxy
nlcom exp(_b[sanctions_proxy_smooth_l1]) - 1


****************************************************
* M2. COUNTRY-MONTH BLOC MODEL
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

ppmlhdfe value ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)

est store M2_cm_blocs

* Joint significance of bloc interactions
test unfriendly_post brics_post cis_post

* Semi-elasticities
nlcom exp(_b[unfriendly_post]) - 1
nlcom exp(_b[brics_post]) - 1
nlcom exp(_b[cis_post]) - 1


****************************************************
* M3. COUNTRY-HS-MONTH SANCTIONS MODEL WITH HS HETEROGENEITY
****************************************************
use "$data\country_hs_month_panel.dta", clear
format stata_mdate %tm
xtset country_hs_id stata_mdate

* Main heterogeneous sanctions model
ppmlhdfe value ///
    c.sanctions_proxy_smooth_l1##i.hs_id ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_hs_id stata_mdate) ///
    vce(cluster country_id)

est store M3_hs_sanctions

* Joint test: does the sanctions slope differ across HS codes?
testparm i.hs_id#c.sanctions_proxy_smooth_l1

levelsof hs_id, local(hslist)

foreach h of local hslist {
    di "HS = `h'"
    lincom _b[c.sanctions_proxy_smooth_l1] + _b[`h'.hs_id#c.sanctions_proxy_smooth_l1]
}

tab hs_id hs


matrix b = e(b)
matrix V = e(V)

putexcel set "$data/m3_b.xlsx", replace
putexcel A1 = matrix(b), names

putexcel set "$data/m3_V.xlsx", replace
putexcel A1 = matrix(V), names

****************************************************
* M4. COUNTRY-HS-MONTH BLOC MODEL
****************************************************
use "$data\country_hs_month_panel.dta", clear
format stata_mdate %tm
xtset country_hs_id stata_mdate

ppmlhdfe value ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_hs_id stata_mdate) ///
    vce(cluster country_id)

est store M4_hs_blocs

* Joint significance of bloc interactions
test unfriendly_post brics_post cis_post


ppmlhdfe value ///
    i.hs_id#i.unfriendly_post ///
    i.hs_id#i.brics_post ///
    i.hs_id#i.cis_post ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_hs_id stata_mdate) ///
    vce(cluster country_id)
	
****************************************************
* EXPORT MAIN TABLES
****************************************************
esttab M1_cm_sanctions M2_cm_blocs M3_hs_sanctions M4_hs_blocs ///
    using "$out\main_4_models.rtf", replace ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N, fmt(0) labels("Observations")) ///
    title("Main PPML models for sanctions intensity, bloc reallocation, and HS heterogeneity")
	
	
****************************************************
* R1. ALTERNATIVE SANCTIONS PROXIES
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

* 2-month lag
ppmlhdfe value ///
    sanctions_proxy_smooth_l2 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_lag2

* 3-month lag
ppmlhdfe value ///
    sanctions_proxy_smooth_l3 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_lag3

* 4-month lag
ppmlhdfe value ///
    sanctions_proxy_smooth_l4 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_lag4


* 5-month lag
ppmlhdfe value ///
    sanctions_proxy_smooth_l5 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_lag5

* raw proxy
ppmlhdfe value ///
    sanctions_proxy_l1 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_raw_proxy

* raw proxy L2
ppmlhdfe value ///
    sanctions_proxy_l2 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_raw_proxy_l2

* raw proxy L3
ppmlhdfe value ///
    sanctions_proxy_l3 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_raw_proxy_l3

* raw proxy L4
ppmlhdfe value ///
    sanctions_proxy_l4 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_raw_proxy_l4

* raw proxy L5
ppmlhdfe value ///
    sanctions_proxy_l5 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_raw_proxy_l5

gen sanctions_proxy_roll6 = ///
    sanctions_proxy + ///
    L1.sanctions_proxy + ///
    L2.sanctions_proxy + ///
    L3.sanctions_proxy + ///
    L4.sanctions_proxy + ///
    L5.sanctions_proxy
	
* rolling-6 proxy
ppmlhdfe value ///
    sanctions_proxy_roll6 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R1_sanctions_proxy_roll6

****************************************************
* R2. TWO-WAY CLUSTERING ROBUSTNESS
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id stata_mdate)
est store R2_twcluster


****************************************************
* R3. WINSORIZED MACRO CONTROLS
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

winsor2 ip_yoy ex_yoy cpi_yoy, cuts(1 99) replace

ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R3_winsor


****************************************************
* R4. EXCLUDING TRANSITION MONTHS
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

drop if inrange(stata_mdate, tm(2022m2), tm(2022m4))

ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R4_notransition


****************************************************
* R5. COMBINED MODEL AS APPENDIX ONLY
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store R5_combined


****************************************************
* VCE COMPARISON FOR MAIN SANCTIONS MODEL
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

* default robust
ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate)
est store vce_robust

* cluster by country
ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)
est store vce_country

* two-way cluster
ppmlhdfe value ///
    sanctions_proxy_smooth_l1 ///
    cpi_yoy_l5 ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id stata_mdate)
est store vce_twoway

esttab vce_robust vce_country vce_twoway ///
    using "$out\vce_comparison.rtf", replace ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Variance estimator comparison for PPML sanctions model")
	
	
****************************************************
* COMPARE LOG-FE AND PPML-FE: BLOC MODEL
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

****************************************************
* A. LOG-LINEAR FE MODEL
****************************************************
preserve
keep if value > 0

gen ln_value = ln(value)

xtreg ln_value ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw ///
    i.stata_mdate, ///
    fe vce(cluster country_id)

est store LOG_BLOCS

restore

****************************************************
* B. PPML FE MODEL
****************************************************
ppmlhdfe value ///
    unfriendly_post brics_post cis_post ///
    cpi_yoy ip_yoy ex_yoy logistics_exposure_distw, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)

est store PPML_BLOCS

****************************************************
* C. EXPORT COMPARISON TABLE
****************************************************
esttab LOG_BLOCS PPML_BLOCS ///
    using "$out\compare_log_vs_ppml_blocs.rtf", replace ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N, fmt(0) labels("Observations")) ///
    title("Comparison of log-linear FE and PPML FE: bloc model") ///
    keep(unfriendly_post brics_post cis_post cpi_yoy ip_yoy ex_yoy logistics_exposure_distw)

	
	
****************************************************
* COMPARE LOG-FE AND PPML-FE: LAGGED SANCTIONS MODEL
****************************************************
use "$data\country_month_panel.dta", clear
format stata_mdate %tm
xtset country_id stata_mdate

****************************************************
* A. LOG-LINEAR FE MODEL
****************************************************
preserve
keep if value > 0

gen ln_value = ln(value)

xtreg ln_value ///
    sanctions_proxy_smooth_l2 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1 ///
    i.stata_mdate, ///
    fe vce(cluster country_id)

est store LOG_SANCTIONS_L2

restore

****************************************************
* B. PPML FE MODEL
****************************************************
ppmlhdfe value ///
    sanctions_proxy_smooth_l2 ///
    cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1, ///
    absorb(country_id stata_mdate) ///
    vce(cluster country_id)

est store PPML_SANCTIONS_L2

****************************************************
* C. EXPORT TABLE
****************************************************
esttab LOG_SANCTIONS_L2 PPML_SANCTIONS_L2 ///
    using "$out\compare_log_vs_ppml_sanctions_l2.rtf", replace ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N, fmt(0) labels("Observations")) ///
    title("Comparison of log-linear FE and PPML FE: lagged sanctions model") ///
    keep(sanctions_proxy_smooth_l2 cpi_yoy_l5 ip_yoy_l1 ex_yoy_l1 logistics_exposure_distw_l1)
