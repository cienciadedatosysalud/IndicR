
<!-- README.md is generated from README.Rmd. Please edit that file -->

# IndicR4Health 

<!-- badges: start -->

<!-- [![CRAN
status](https://www.r-pkg.org/badges/version/)](https://CRAN.R-project.org/package="package"/)-->
[![GitHub
version](https://img.shields.io/badge/GitHub-0.0.0-blue)](https://github.com/cienciadedatosysalud/IndicR4Health)
[![R-CMD-check](https://github.com/cienciadedatosysalud/IndicR4Health/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/cienciadedatosysalud/IndicR4Health/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/cienciadedatosysalud/IndicR4Health/graph/badge.svg)](https://app.codecov.io/gh/cienciadedatosysalud/IndicR4Health)
[![Lifecycle:
stable](https://lifecycle.r-lib.org/articles/figures/lifecycle-stable.svg)](https://lifecycle.r-lib.org/articles/stages.html#stable/)
<!-- badges: end -->

**IndicR4Health** is a Lightweight, Fast, and Intuitive Indicator Calculations Package for Health.

### Development Version

You can install the development version of **IndicR4Health** from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("cienciadedatosysalud/IndicR4Health")
```

## Example

``` r
library(IndicR4Health)

hosp_dataframe <- data.frame(
  episode_id = c(1, 2, 3),
  age = c(45, 60, 32),
  sex = c("M", "F", "M"),
  diagnosis1 = c("F10.10", "I20", "I60"),
  diagnosis2 = c("E11", "J45", "I25"),
  diagnosis3 = c("I60", "K35", "F10.120"),
  present_on_admission_d1 = c(TRUE,FALSE,FALSE),
  present_on_admission_d2 = c(FALSE,TRUE,FALSE),
  present_on_admission_d3 = c(FALSE,TRUE,TRUE)
)


reng <- IndicR4Health::RuleEngine(hosp_dataframe, "episode_id")

target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
definition_codes <- c('F10.10')
scenario1 <- IndicR4Health::MatchAny(reng, "scenario1", target_columns, definition_codes)

target_columns <- c('diagnosis1','diagnosis3')
definition_codes <- c('F10.10',"I60")
scenario2 <- IndicR4Health::MatchAll(reng, "scenario2", target_columns, definition_codes)


target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
filter_columns <- c('present_on_admission_d1','present_on_admission_d2','present_on_admission_d3')
lookup_values <- c('true')

definition_codes <- c('F10.10',"I60")
scenario3 <- IndicR4Health::MatchAnyWhere(reng, "scenario3", target_columns,
                           definition_codes,
                           filter_columns = filter_columns,
                           lookup_values = lookup_values )


target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
filter_columns <- c('present_on_admission_d1','present_on_admission_d2','present_on_admission_d3')
lookup_values <- c('true')

definition_codes <- c('F10.10',"I60")
scenario4 <- IndicR4Health::MatchAllWhere(reng, "scenario4", target_columns,
                           definition_codes,
                           filter_columns = filter_columns,
                           lookup_values = lookup_values )


list_scenarios = list(scenario1, scenario2, scenario3, scenario4)
result <- IndicR4Health::RunIndicators(reng,list_scenarios, append_results = FALSE)

```
