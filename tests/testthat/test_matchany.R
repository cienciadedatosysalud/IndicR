
library(IndicR)
library(testthat)

hosp_dataframe <- data.frame(
  episode_id = c(1, 2, 3),
  age = c(45, 60, 32),
  sex = c("M", "F", "M"),
  diagnosis1 = c("F10.10", "I20", "I60"),
  diagnosis2 = c("E11", "J45", "I25"),
  diagnosis3 = c("I60", "K35", "F10.120"),
  present_on_admission_d1 = c(FALSE,FALSE,FALSE),
  present_on_admission_d2 = c("No","Yes","No"),
  present_on_admission_d3 = c(FALSE,TRUE,TRUE)
)


reng <- IndicR::RuleEngine(hosp_dataframe, "episode_id")


### MatchAny regex_prefix_search =  FALSE, inverse_match_result =  FALSE

target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
definition_codes <- c('F10.10')
scenario1 <- MatchAny(reng, "scenario1", target_columns, definition_codes)

### MatchAny regex_prefix_search =  TRUE, inverse_match_result =  FALSE
definition_codes <- c('F10.1')
scenario2 <- MatchAny(reng, "scenario2", target_columns, definition_codes,regex_prefix_search =  TRUE)

### MatchAny regex_prefix_search = FALSE, inverse_match_result =  TRUE
definition_codes <- c('F10.10')
scenario3 <- MatchAny(reng, "scenario3", target_columns, definition_codes, inverse_match_result =  TRUE)

### MatchAny regex_prefix_search =  TRUE, inverse_match_result =  TRUE
definition_codes <- c('F10.1')
scenario4 <- MatchAny(reng, "scenario4", target_columns, definition_codes,regex_prefix_search =  TRUE, inverse_match_result =  TRUE)



list_scenarios = list(scenario1, scenario2, scenario3, scenario4)
result <- reng$run_indicators(list_scenarios, append_results = FALSE)

result_scenario1 <- result[c('episode_id', 'scenario1')]
result_scenario2 <- result[c('episode_id', 'scenario2')]
result_scenario3 <- result[c('episode_id', 'scenario3')]
result_scenario4 <- result[c('episode_id', 'scenario4')]

expected_result1 <- data.frame(
  episode_id = c(1,3),
  scenario1 = c(TRUE,FALSE)
)


expected_result2 <- data.frame(
  episode_id = c(1,3),
  scenario2 = c(TRUE, TRUE)
)

expected_result3 <- data.frame(
  episode_id = c(1,3),
  scenario3 = c(FALSE,FALSE)
)

expected_result4 <- data.frame(
  episode_id = c(1,3),
  scenario4 = c(FALSE,FALSE)
)

expect_equal(all.equal(expected_result1, result_scenario1), TRUE)
expect_equal(all.equal(expected_result2, result_scenario2), TRUE)
expect_equal(all.equal(expected_result3, result_scenario3), TRUE)
expect_equal(all.equal(expected_result4, result_scenario4), TRUE)



# reng$destroy()
