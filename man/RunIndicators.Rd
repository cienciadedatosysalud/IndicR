% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ruleEngine.R
\name{RunIndicators}
\alias{RunIndicators}
\title{RunIndicators}
\usage{
RunIndicators(
  rule_engine,
  indicators_rules,
  only_true_indicators = TRUE,
  append_results = FALSE,
  to_csv = NULL,
  to_parquet = NULL
)
}
\arguments{
\item{rule_engine}{RuleEngine object. Used to apply the indicator rules
on the associated dataset.}

\item{indicators_rules}{List of objects of class `SqlRuleIndicator` (MatchAny, MatchAll, MatchAnyWhere, MatchAllWhere, CustomMatch). Each object
represents an indicator rule to be applied.}

\item{only_true_indicators}{Logical. If `TRUE`, the function returns only the records
that meet at least one of the indicators. Defaults to `TRUE`.}

\item{append_results}{Logical. If `TRUE`, the function returns the original dataset
along with the indicators. If `FALSE`, only the `unique_identifier_column` and the
indicator results are returned. Defaults to `FALSE`.}

\item{to_csv}{Character or `NULL`. Path to save the results as a CSV file. If `NULL`,
no CSV file is created. Defaults to `NULL`.}

\item{to_parquet}{Character or `NULL`. Path to save the results as a parquet file format
with gzip compression. Defaults to `NULL`.}
}
\value{
Depends on the parameter values:
- If `only_true_indicators = TRUE`, only the records matching at least one indicator
  are returned.
- If `append_results = TRUE`, the full dataset with appended indicators is returned.
- If `append_results = FALSE`, only the `unique_identifier_column` and the indicator
  results are returned.
- If `to_csv` or `to_parquet` is specified, the results are saved to the respective file format.
}
\description{
Executes the specified indicator rules using the given RuleEngine object
and provides options for output customization.
}
\details{
This function ensures that all `indicators_rules` are objects of class
`SqlRuleIndicator` (MatchAny, MatchAll, MatchAnyWhere, MatchAllWhere, CustomMatch). If any invalid object is passed, the function stops with an error.
}
\examples{
\dontrun{
# Example usage:

df <- read.csv("dataset.csv", sep = "|", header = TRUE)
rule_engine <- RuleEngine(df,"hospitalization_id")

target_columns <- c("diagnosis1")
definition_codes <- c("F10.10","F10.11","F10.120","F10.121")

alcohol_indicator <- IndicR4Health::MatchAny(
                rule_engine,
                "alcohol_i",
                target_columns,
                definition_codes
                )
indicators_rules <- list(alcohol_indicator)

# Option return data frame
result <- IndicR4Health::RunIndicators(
  rule_engine,
  indicators_rules,
  only_true_indicators = TRUE,
  append_results = FALSE
  )

# Option save to csv file
IndicR4Health::RunIndicators(
  rule_engine,
  indicators_rules,
  only_true_indicators = TRUE,
  append_results = FALSE,
  to_csv = "output.csv"
  )

# Option save to parquet file
IndicR4Health::RunIndicators(
  rule_engine,
  indicators_rules,
  only_true_indicators = TRUE,
  append_results = FALSE,
  to_csv = "output.parquet"
  )
}
}
