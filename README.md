
<!-- README.md is generated from README.Rmd. Please edit that file -->

# IndicR 

<!-- badges: start -->

<!-- [![CRAN
status](https://www.r-pkg.org/badges/version/)](https://CRAN.R-project.org/package="package"/)-->
[![GitHub
version](https://img.shields.io/badge/GitHub-0.1.0-blue)](https://github.com/cienciadedatosysalud/IndicR)
[![R-CMD-check](https://github.com/cienciadedatosysalud/IndicR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/cienciadedatosysalud/IndicR/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/cienciadedatosysalud/IndicR/graph/badge.svg)](https://app.codecov.io/gh/cienciadedatosysalud/IndicR)
[![Lifecycle:
stable](https://lifecycle.r-lib.org/articles/figures/lifecycle-stable.svg)](https://lifecycle.r-lib.org/articles/stages.html#stable/)
<!-- badges: end -->

**IndicR** is a Lightweight, Fast, and Intuitive Indicator Calculations R Package from Health data.

### Development Version

You can install the development version of **IndicR** from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("cienciadedatosysalud/IndicR")
```

## Example

``` r
library(IndicR)

hosp_dataframe <- data.frame(
  episode_id = c(1, 2, 3),
  age = c(45, 60, 32),
  sex = c("M", "F", "M"),
  diagnosis1 = c("F10.10", "I20", "I60"),
  diagnosis2 = c("E11", "J45", "I25"),
  diagnosis3 = c("I61", "K35", "F10.120"),
  present_on_admission_d1 = c(TRUE,FALSE,FALSE),
  present_on_admission_d2 = c(FALSE,TRUE,FALSE),
  present_on_admission_d3 = c(TRUE,TRUE,TRUE)
)


reng <- IndicR::RuleEngine(hosp_dataframe, "episode_id")

target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
definition_codes <- c('F10.10')
scenario1 <- IndicR::MatchAny(reng, "scenario1", target_columns, definition_codes)

target_columns <- c('diagnosis1','diagnosis3')
definition_codes <- c('I20',"K35")
scenario2 <- IndicR::MatchAll(reng, "scenario2", target_columns, definition_codes)


target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
filter_columns <- c('present_on_admission_d1','present_on_admission_d2','present_on_admission_d3')
lookup_values <- c('true')

definition_codes <- c('F10.1',"I60")
scenario3 <- IndicR::MatchAnyWhere(reng, "scenario3", target_columns,
                                   definition_codes,
                                   filter_columns = filter_columns,
                                   lookup_values = lookup_values,
                                   regex_prefix_search = TRUE)


target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
filter_columns <- c('present_on_admission_d1','present_on_admission_d2','present_on_admission_d3')
lookup_values <- c('true')

definition_codes <- c('F10.1',"I60")
scenario4 <- IndicR::MatchAllWhere(reng, "scenario4", target_columns,
                                   definition_codes,
                                   filter_columns = filter_columns,
                                   lookup_values = lookup_values,
                                   regex_prefix_search = TRUE)


list_scenarios = list(scenario1, scenario2, scenario3, scenario4)
result <- IndicR::RunIndicators(reng,list_scenarios, append_results = TRUE,only_true_indicators = TRUE)


```

Indicator Builder
=================

Effortlessly generate indicator calculation script templates using 'IndicatorBuilder'. This web tool reads a CSV file, where each column represents an indicator by including its respective definition codes, and outputs a script template ready to be copied and used in R or Python. You can streamline your data analysis workflow with 'IndicatorBuilder'.
 
The use of 'IndicatorBuilder' complements the Python library [IndicPy](https://cienciadedatosysalud.github.io/IndicPy/#) and the R package [IndicR](https://cienciadedatosysalud.github.io/IndicR/), providing a easy-to-use tool scripting the definition of any indicator within your preferred programming environment.


IndicR for Python
=============

IndicR is also available for **Python** under the name [IndicPy](https://cienciadedatosysalud.github.io/IndicPy/#), offering similar functionality for processing data.

You can find and use IndicPy in its official repository:

🚀 IndicPy on GitHub <https://github.com/cienciadedatosysalud/IndicPy>_



## 📜 Disclaimer

This software is provided "as is," without any warranties of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, and non-infringement.

In no event shall the authors, contributors, or maintainers be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including but not limited to loss of use, data, or profits), regardless of the cause and under any liability theory, whether in contract, strict liability, or tort (including negligence or any other cause), arising in any way from the use of this software, even if advised of the possibility of such damages.

The user assumes full responsibility for the use of this library, including evaluating its suitability and safety in the context of their application.

