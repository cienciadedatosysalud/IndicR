
library(IndicR4Health)
library(testthat)

hosp_dataframe <- data.frame(
  episode_id = c(1, 2, 3),
  age = c(45, 60, 32),
  sex = c("M", "F", "M"),
  diagnosis1 = c("F10.10", "I20", "I60"),
  diagnosis2 = c("E11", "J45", "I25"),
  diagnosis3 = c("I60", "K35", "F10.120"),
  present_on_admission_d1 = c("Yes","No","No"),
  present_on_admission_d2 = c("No","Yes","No"),
  present_on_admission_d3 = c("No","Yes","Yes")
)


reng <- IndicR4Health::RuleEngine(hosp_dataframe, "episode_id")


### MatchAnyWhere regex_prefix_search =  FALSE

target_columns <- c('diagnosis1','diagnosis2','diagnosis3')
filter_columns <- c('present_on_admission_d1','present_on_admission_d2','present_on_admission_d3')
lookup_values <- c('Yes')

definition_codes <- c('F10.10',"I60")
scenario1 <- MatchAnyWhere(reng, "scenario1", target_columns,
                           definition_codes,
                           filter_columns = filter_columns,
                           lookup_values = lookup_values )

### MatchAnyWhere regex_prefix_search =  TRUE
definition_codes <- c('F10.1','I')
scenario2 <- MatchAnyWhere(reng, "scenario2", target_columns,
                           definition_codes,regex_prefix_search =  TRUE,
                           filter_columns = filter_columns,
                           lookup_values = lookup_values)



list_scenarios = list(scenario1, scenario2)
result <- IndicR4Health::RunIndicators(reng,list_scenarios, append_results = FALSE)

result_scenario1 <- result[c('episode_id', 'scenario1')]
result_scenario2 <- result[c('episode_id', 'scenario2')]

expected_result1 <- data.frame(
  episode_id = c(1,3),
  scenario1 = c(TRUE,FALSE)
)


expected_result2 <- data.frame(
  episode_id = c(1,3),
  scenario2 = c(TRUE, TRUE)
)


expect_equal(all.equal(expected_result1, result_scenario1), TRUE)
expect_equal(all.equal(expected_result2, result_scenario2), TRUE)

