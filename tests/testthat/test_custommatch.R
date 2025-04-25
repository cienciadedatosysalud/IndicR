
library(IndicR)
library(testthat)

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


reng <- IndicR::RuleEngine(hosp_dataframe, "episode_id")


### MatchAny regex_prefix_search =  FALSE, inverse_match_result =  FALSE

target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
definition_codes <- c('I')
scenario1 <- MatchAny(reng, "scenario1", target_columns, definition_codes, regex_prefix_search = TRUE)
scenario2 <- CustomMatch("scenario2custom", "scenario1 and sex = 'M'")
scenario3 <- CustomMatch("scenario3custom", "scenario1 and sex = 'F'")

list_scenarios = list(scenario1, scenario2, scenario3)
result <- RunIndicators(reng,list_scenarios, append_results = FALSE)
result <- result[order(result$episode_id), ]
rownames(result) <- NULL

result_scenario1 <- result[c('episode_id', 'scenario1')]
result_scenario2 <- result[c('episode_id', 'scenario2custom')]
result_scenario3 <- result[c('episode_id', 'scenario3custom')]


expected_result1 <- data.frame(
  episode_id = c(1,2,3),
  scenario1 = c(TRUE,TRUE,TRUE)
)


expected_result2 <- data.frame(
  episode_id = c(1,2,3),
  scenario2custom = c(TRUE,FALSE, TRUE)
)

expected_result3 <- data.frame(
  episode_id = c(1,2,3),
  scenario3custom = c(FALSE,TRUE,FALSE)
)

expect_equal(all.equal(expected_result1, result_scenario1), TRUE)
expect_equal(all.equal(expected_result2, result_scenario2), TRUE)
expect_equal(all.equal(expected_result3, result_scenario3), TRUE)


