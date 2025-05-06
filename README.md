
<!-- README.md is generated from README.Rmd. Please edit that file -->

# IndicR4Health 

<!-- badges: start -->

<!-- [![CRAN
status](https://www.r-pkg.org/badges/version/)](https://CRAN.R-project.org/package="package"/)-->
[![GitHub
version](https://img.shields.io/badge/GitHub-0.0.0-blue)](https://github.com/cienciadedatosysalud/IndicR4Health)
[![R-CMD-check](https://github.com/cienciadedatosysalud/IndicR4Health/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/cienciadedatosysalud/IndicR4Health/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/cienciadedatosysalud/IndicR4Health/graph/badge.svg)](https://app.codecov.io/gh/cienciadedatosysalud/IndicR)
[![Lifecycle:
stable](https://lifecycle.r-lib.org/articles/figures/lifecycle-stable.svg)](https://lifecycle.r-lib.org/articles/stages.html#stable/)
[![DOI](https://zenodo.org/badge/972118056.svg)](https://doi.org/10.5281/zenodo.15342970)
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
  diagnosis3 = c("I61", "K35", "F10.120"),
  present_on_admission_d1 = c(TRUE,FALSE,FALSE),
  present_on_admission_d2 = c(FALSE,TRUE,FALSE),
  present_on_admission_d3 = c(TRUE,TRUE,TRUE)
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

Indicator Builder
=================

Effortlessly generate indicator calculation script templates using 'IndicatorBuilder'. This web tool reads a CSV file, where each column represents an indicator by including its respective definition codes, and outputs a script template ready to be copied and used in R or Python. You can streamline your data analysis workflow with 'IndicatorBuilder'.
 
The use of 'IndicatorBuilder' complements the Python library [IndicPy4Health](https://cienciadedatosysalud.github.io/IndicPy4Health/#) and the R package [IndicR4Health](https://cienciadedatosysalud.github.io/IndicR4Health/), providing an easy-to-use tool scripting the definition of any indicator within your preferred programming environment.


IndicR4Health for Python
=============

IndicR4Health is also available for **Python** under the name [IndicPy4Health](https://cienciadedatosysalud.github.io/IndicPy4Health/#), offering similar functionality for processing data.

You can find and use IndicPy4Health in its official repository:

🚀 IndicPy4Health on GitHub <https://github.com/cienciadedatosysalud/IndicPy4Health>_



## 📜 Disclaimer

This software is provided "as is," without any warranties of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, and non-infringement.

In no event shall the authors, contributors, or maintainers be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including but not limited to loss of use, data, or profits), regardless of the cause and under any liability theory, whether in contract, strict liability, or tort (including negligence or any other cause), arising in any way from the use of this software, even if advised of the possibility of such damages.

The user assumes full responsibility for the use of this library, including evaluating its suitability and safety in the context of their application.

