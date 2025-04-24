# IndicR
#' Create Parent Class SqlRuleIndicator
#'
#' @param name Name of the indicator.
#' @param sql_rule Logic of the indicator.
#' @return An SqlRuleIndicator Object.
SqlRuleIndicator <- setRefClass(
  "SqlRuleIndicator",
  fields = list(
    name = "character",
    sql_rule = "character"
  ),
  methods = list(
    initialize = function(name, sql_rule) {
      name <<- name
      sql_rule <<- sql_rule
    },

    get_indicator_name = function() {
      return(name)
    },

    get_sql_rule = function() {
      return(sql_rule)
    }
  )
)


#' RuleEngine
#'
#' @param df Dataframe object in which you want to process the indicators.
#' @param unique_identifier_column Name of the column containing the unique identifiers for the provided dataframe (there can be no repeated values in this column).
#' @param database_path Path where you want to save the database needed to calculate the indicators (by default in memory).
#' @return A Rule Engine Object.
#' @examples
#' \dontrun{
#' df <- read.csv("dataset.csv", sep = "|", header = TRUE)
#' reng <- rule_engine(df,"hospitalization_id")
#'
#' df2 <- read.csv("dataset2.csv", sep = "|", header = TRUE)
#' reng2 <- rule_engine(df2,"episode_id","./indicators.duckdb")
#' }
#' @importFrom DBI dbConnect
#' @importFrom DBI dbWriteTable
#' @importFrom DBI dbExecute
#' @importFrom DBI dbGetQuery
#' @importFrom DBI dbDisconnect
#' @importFrom duckdb duckdb
#' @importFrom methods new
#' @export
#'
RuleEngine <- function(df,unique_identifier_column,database_path = ":memory:"){
  return(RuleEngineClass$new(df,unique_identifier_column,database_path))
}


#'
#'
RuleEngineClass <- setRefClass(
  "RuleEngine",
  fields = list(
    conn = "ANY",
    row_identifier = "character",
    columns = "character"
  ),
  methods = list(
    initialize = function(df,unique_identifier_column,database_path = ":memory:") {
      conn <<- DBI::dbConnect(duckdb::duckdb(),database_path)
      DBI::dbWriteTable(conn, "df_original", df, overwrite = TRUE)
      DBI::dbExecute(conn, "CREATE OR REPLACE TABLE dataframe_original AS SELECT row_number() OVER () as row_index_id,* FROM df_original")

      columns <<- DBI::dbGetQuery(conn, "SHOW dataframe_original")$column_name

      result <- DBI::dbGetQuery(conn, sprintf("select count(*) == count(distinct %s) as valid_identifier from main.dataframe_original", unique_identifier_column))
      if (!result$valid_identifier) {
        stop("La verificacion ha fallado: los identificadores no son validos. Se detiene la ejecucion.")
      }

      row_identifier <<- unique_identifier_column
    },

    finalize = function() {
      # Close the database connection
      if (!is.null(conn)) {
        DBI::dbDisconnect(conn, shutdown = TRUE)
        message("Database connection closed.")
      } else {
        message("No active database connection to close.")
      }
    },

    run_indicators = function(indicators_rules, only_true_indicators = TRUE, append_results = FALSE,
                                    csv_path = NULL, to_parquet = FALSE) {
      indicators_ok <- all(sapply(indicators_rules, function(rule) inherits(rule, "SqlRuleIndicator")))
      if (!indicators_ok) {
        stop("Solo se aceptan objetos de la clase SqlRuleIndicator.")
      }

      DBI::dbExecute(conn,sprintf("CREATE OR REPLACE TABLE results_ as (select row_index_id, %s  from dataframe_original)", row_identifier))
      DBI::dbExecute(conn, "
        CREATE OR REPLACE VIEW dataframe_ AS (
          SELECT a.row_index_id, * EXCLUDE(row_index_id)
          FROM dataframe_original a LEFT JOIN results_ b ON a.row_index_id = b.row_index_id
        )
      ")
      indicators_name <- sapply(indicators_rules, function(rule) rule$name)
      for (sql_rule in indicators_rules) {
        query <- sprintf(
          "CREATE OR REPLACE TABLE results_ AS (
            SELECT a.*, COALESCE(b.%s, FALSE) AS %s
            FROM results_ a LEFT JOIN (%s) b ON a.row_index_id = b.row_index_id
          )",
          sql_rule$name, sql_rule$name, sql_rule$sql_rule
        )
        DBI::dbExecute(conn, query)

        DBI::dbExecute(conn, "
          CREATE OR REPLACE VIEW dataframe_ AS (
            SELECT a.row_index_id, * EXCLUDE(row_index_id)
            FROM dataframe_original a LEFT JOIN results_ b ON a.row_index_id = b.row_index_id
          )
        ")
      }

      condition_true <- paste(indicators_name, collapse = " OR ")

      if (append_results && only_true_indicators) {
        query_get_data <- sprintf("SELECT * exclude(row_index_id) FROM dataframe_ WHERE row_index_id IN (SELECT row_index_id FROM results_ WHERE %s)", condition_true)
      } else if (append_results && !only_true_indicators) {
        query_get_data <- "SELECT * exclude(row_index_id) FROM dataframe_"
      } else {
        query_get_data <- sprintf("SELECT * exclude(row_index_id) FROM results_ WHERE %s", condition_true)
      }

      if (!is.null(csv_path)) {
        if (to_parquet) {
          query_save_csv <- sprintf("COPY (%s) TO '%s' WITH (FORMAT 'parquet', COMPRESSION 'gzip')", query_get_data, csv_path)
        } else {
          query_save_csv <- sprintf("COPY (%s) TO '%s' WITH (FORMAT CSV)", query_get_data, csv_path)
        }
        DBI::dbExecute(conn, query_save_csv)
      } else {
        return(DBI::dbGetQuery(conn, query_get_data))
      }
    }
  )
)


#' MatchAny
#'
#' @description
#' A class that creates the indicator in sql form which evaluates whether any
#' of the target columns match the specified definition codes, ensuring that
#' the indicator is TRUE if at least one match occurs.
#'
#' @param rule_engine The rule engine containing the dataset where indicators will be applied.
#' @param indicator_name A string representing the name of the indicator, assigned to the `name` field.
#' @param target_columns A vector specifying the column names where the match is evaluated.
#'                       Searches are performed across all target columns.
#' @param definition_codes A set of codes used to define the matching criteria for the target columns.
#' @param regex_prefix_search Logical value indicating whether to perform prefix-based regex matches (`TRUE`)
#'                            or exact matches (default) (`FALSE`).
#' @param inverse_match_result Logical value indicating whether to invert the match results (`TRUE`)
#'                             or retain them as is (`FALSE`).
#' @examples
#' \dontrun{
#' hosp_dataframe <- data.frame(
#'   episode_id = c(1, 2, 3),
#'   age = c(45, 60, 32),
#'   sex = c("M", "F", "M"),
#'   diagnosis1 = c("F10.10", "I20", "I60"),
#'   diagnosis2 = c("E11", "J45", "I25"),
#'   diagnosis3 = c("I60", "K35", "F10.120")
#' )
#'
#' target_columns <- c("diagnosis1")
#'
#' definition_codes <- c("F10.10","F10.11","F10.120","F10.121")
#'
#' alcohol_indicator <- IndicR::MatchAny(
#'                 reng,            # rule_engine
#'                 "alcohol_i",     # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 definition_codes # Codes to look for in the columns
#'                 )
#'}
#'@return Returns an instance of MatchAny with a generated SQL rule.
#'@export
#'
MatchAny <- function(rule_engine, indicator_name, target_columns,
                     definition_codes, regex_prefix_search = FALSE,
                     inverse_match_result = FALSE){
  return(MatchAnyClass$new(rule_engine, indicator_name, target_columns,
                    definition_codes, regex_prefix_search,
                    inverse_match_result))
}


#'
MatchAnyClass <- setRefClass(
  "MatchAny",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns,
                          definition_codes, regex_prefix_search = FALSE,
                          inverse_match_result = FALSE) {

      invalid_columns <- setdiff(target_columns, rule_engine$columns)
      if (length(invalid_columns) > 0) {
        stop("Solo se acepta ... (columnas invalidas): ", paste(invalid_columns, collapse = ", "))
      }

      name <<- indicator_name
      columns_part <- paste(target_columns, collapse = ",")
      if (regex_prefix_search) {

        codes_part <- paste(sprintf("'%s%%'", definition_codes), collapse = ",")

        sql_rule <<- sprintf(
          "WITH codes_to_compare AS (SELECT DISTINCT UNNEST([%s]) AS code_to_compare)
         SELECT DISTINCT a.row_index_id, %s TRUE AS %s
         FROM (SELECT * FROM (SELECT row_index_id, UNNEST([%s]) AS list_diag FROM dataframe_) a WHERE list_diag IS NOT NULL) a
         LEFT JOIN codes_to_compare b ON a.list_diag LIKE b.code_to_compare WHERE b.code_to_compare IS NOT NULL",
          codes_part,
          if (inverse_match_result) "NOT" else "",
          indicator_name,
          columns_part
        )
      } else {
        codes_part <- paste(sprintf("'%s'", definition_codes), collapse = ",")

        sql_rule <<- sprintf(
          "WITH codes_to_compare AS (SELECT DISTINCT UNNEST([%s]) AS code_to_compare)
         SELECT DISTINCT a.row_index_id, %s TRUE AS %s
         FROM (SELECT * FROM (SELECT row_index_id, UNNEST([%s]) AS list_diag FROM dataframe_) a WHERE list_diag IS NOT NULL) a
         LEFT JOIN codes_to_compare b ON a.list_diag = b.code_to_compare WHERE b.code_to_compare IS NOT NULL",
          codes_part,
          if (inverse_match_result) "NOT" else "",
          indicator_name,
          columns_part
        )
      }
    }
  )
)



#' MatchAnyWhere
#'
#' @description
#' A class that creates the indicator in SQL form which evaluates whether any of the target columns
#' match the specified definition codes under the conditions defined by the filter columns and lookup values.
#' The matching will only be applied to those target columns that are in the same order as the filter columns
#' and satisfy the conditions in lookup values, ensuring that the indicator is TRUE if at least one such target column satisfies the matching criteria.
#'
#'
#' @param rule_engine The rule engine containing the dataset where the indicators will be applied.
#' @param indicator_name A string representing the name of the indicator, assigned to the `name` property.
#' @param target_columns A vector specifying the column names where the values from `definition_codes` will be searched.
#'                       Searches will only be performed in the corresponding column of `target_columns` if the condition is met
#'                       in the column of the same index in `filter_columns`.
#' @param definition_codes A set of codes used to define the matching criteria applied to `target_columns`.
#' @param filter_columns A vector specifying the columns that define the conditions under which the `lookup_values` must hold.
#'                       These conditions are evaluated in order and are directly linked to the columns of `target_columns`.
#' @param lookup_values A list of values used to define logical conditions that must be satisfied in the order of `filter_columns`.
#' @param regex_prefix_search Logical value indicating whether to use regex-based prefix searches (`TRUE`) or exact matches (`FALSE`).
#' @param inverse_match_result Logical value indicating whether to invert the match results (`TRUE`) or keep them as is (`FALSE`).
#'
#' @return Returns an instance of MatchAnyWhere with a generated SQL rule.
#'
#' @examples
#' \dontrun{
#' hosp_dataframe <- data.frame(
#'   episode_id = c(1, 2, 3),
#'   age = c(45, 60, 32),
#'   sex = c("M", "F", "M"),
#'   diagnosis1 = c("F10.10", "I20", "I60"),
#'   diagnosis2 = c("E11", "J45", "I25"),
#'   diagnosis3 = c("I60", "K35", "F10.120"),
#'   present_on_admission_d1 = c(FALSE,FALSE,FALSE),
#'   present_on_admission_d2 = c("No","Yes","No"),
#'   present_on_admission_d3 = c(FALSE,TRUE,TRUE)
#' )
#'
#'
#' reng <- RuleEngine(hosp_dataframe,"episode_id")
#'
#' target_columns <- c("diagnosis2","diagnosis3")
#' definition_codes <- c("F10.10","F10.11","F10.120","F10.121")
#'
#' filter_columns <- c("present_on_admission_d2","present_on_admission_d3")
#' lookup_values <- c("Yes", "true")
#'
#' alcohol_indicator_poa <- IndicR::MatchAnyWhere(
#'                 reng,            # rule_engine
#'                 "alcohol_i_poa", # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 definition_codes # Codes to look for in the columns,
#'                 filter_columns,
#'                 lookup_values
#'                 )
#' alcohol_i_regex_poa <- IndicR::MatchAnyWhere(
#'                 reng,            # rule_engine
#'                 "alcohol_i_regex_poa", # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 c("F10") # Codes to look for in the columns (start with "F10"),
#'                 filter_columns,
#'                 lookup_values,
#'                 regex_prefix_search = TRUE
#'                 )
#'
#'
#' indicators_list <- list(alcohol_indicator_poa, alcohol_i_regex_poa)
#' reng$run_indicators(indicators_list, append_results = FALSE, csv_path="./results.csv")
#' }
#' @export
#'
MatchAnyWhere <- function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE,
                          inverse_match_result= FALSE){


  return(MatchAnyWhereClass$new(rule_engine, indicator_name, target_columns, definition_codes,
                                filter_columns, lookup_values, regex_prefix_search,
                                inverse_match_result))
}



#'
MatchAnyWhereClass <- setRefClass(
  "MatchAnyWhere",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE,
                          inverse_match_result= FALSE) {

      invalid_columns <- setdiff(target_columns, rule_engine$columns)
      if (length(invalid_columns) > 0) {
        stop("Solo se acepta ... (columnas invalidas): ", paste(invalid_columns, collapse = ", "))
      }

      invalid_columns <- setdiff(filter_columns, rule_engine$columns)
      if (length(invalid_columns) > 0) {
        stop("Solo se acepta ... (columnas invalidas): ", paste(invalid_columns, collapse = ", "))
      }


      if (length(target_columns) != length(filter_columns)) {
        stop("La longitud debe ser la misma")
      }

      # TODO check filter_columns same data type

      # select count(*) filter(data_type ='BOOLEAN') as columns_boolean, count(*) filter(data_type !='BOOLEAN') as columns_no_boolean
      # from information_schema.columns where table_name = 'dataframe_' and column_name in ('poad1','poad2','poad3')


      name <<- indicator_name

      when_str <- ""
      for (e in lookup_values) {
        when_str <- paste0(when_str, sprintf(" WHEN x == '%s' THEN TRUE", e))
      }

      case_when_part <- sprintf("CASE %s ELSE FALSE END", when_str)

      columns_part <- paste(target_columns, collapse = ",")
      filter_columns_part <- paste(filter_columns, collapse = ",")
      query_where <- sprintf("list_where([%s], list_transform([%s], x -> (%s)))",
                             columns_part, filter_columns_part, case_when_part)
      if (regex_prefix_search) {
        codes_part <- paste(sprintf("'%s%%'", definition_codes), collapse = ",")
        sql_rule <<- sprintf("WITH codes_to_compare AS (SELECT DISTINCT UNNEST([%s]) AS code_to_compare) SELECT DISTINCT row_index_id, %s TRUE AS %s FROM (
  SELECT row_index_id, UNNEST(listwhere_result) AS codes_where FROM (SELECT row_index_id, %s AS listwhere_result FROM main.dataframe_ WHERE len(listwhere_result) > 0)) a
  LEFT JOIN codes_to_compare b ON a.codes_where like b.code_to_compare WHERE b.code_to_compare IS NOT NULL",codes_part, if (inverse_match_result) "NOT" else "", indicator_name, query_where)

      } else {

        codes_part <- paste(sprintf("'%s'", definition_codes), collapse = ",")
        sql_rule <<- sprintf("WITH codes_to_compare AS (SELECT DISTINCT UNNEST([%s]) AS code_to_compare) SELECT DISTINCT row_index_id, %s TRUE AS %s FROM (SELECT row_index_id, UNNEST(listwhere_result) AS codes_where FROM (
    SELECT row_index_id, %s AS listwhere_result FROM main.dataframe_ WHERE len(listwhere_result) > 0 )) a LEFT JOIN codes_to_compare b ON a.codes_where = b.code_to_compare
WHERE b.code_to_compare IS NOT NULL", codes_part, if (inverse_match_result) "NOT" else "", indicator_name, query_where)
      }
    }
  )
)



#' MatchAll
#'
#' @description
#' A class that creates the indicator in SQL form which evaluates whether all
#' of the target columns match the specified definition codes, ensuring that
#' the indicator is TRUE only if every target column has a match.
#'
#' @param rule_engine The rule engine containing the dataset where indicators will be applied.
#' @param indicator_name A string representing the name of the indicator, assigned to the `name` field.
#' @param target_columns A vector specifying the column names where the match is evaluated.
#'                       Searches are performed across all target columns.
#' @param definition_codes A set of codes used to define the matching criteria for the target columns.
#' @param regex_prefix_search Logical value indicating whether to perform prefix-based regex matches (`TRUE`)
#'                            or exact matches (default) (`FALSE`).
#' @param inverse_match_result Logical value indicating whether to invert the match results (`TRUE`)
#'                             or retain them as is (`FALSE`).
#' @return Returns an instance of MatchAll with a generated SQL rule.
#' @examples
#' \dontrun{
#' hosp_dataframe <- data.frame(
#'   episode_id = c(1, 2, 3),
#'   age = c(45, 60, 32),
#'   sex = c("M", "F", "M"),
#'   diagnosis1 = c("F10.10", "I20", "I60"),
#'   diagnosis2 = c("E11", "J45", "I25"),
#'   diagnosis3 = c("I60", "K35", "F10.120")
#' )
#'
#' target_columns <- c("diagnosis1")
#'
#' definition_codes <- c("F10.10","F10.11","F10.120","F10.121")
#'
#' alcohol_indicator <- IndicR::MatchAll(
#'                 reng,            # rule_engine
#'                 "alcohol_i",     # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 definition_codes # Codes to look for in the columns
#'                 )
#' }
#' @export
#'
MatchAll <- function(rule_engine, indicator_name, target_columns,
                     definition_codes, regex_prefix_search = FALSE,
                     inverse_match_result = FALSE){
  return(MatchAllClass$new(rule_engine, indicator_name, target_columns,
                           definition_codes, regex_prefix_search,
                           inverse_match_result))
}


MatchAllClass <- setRefClass(
  "MatchAll",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns,
                          definition_codes, regex_prefix_search = FALSE,
                          inverse_match_result = FALSE) {

      invalid_columns <- setdiff(target_columns, rule_engine$columns)
      if (length(invalid_columns) > 0) {
        stop("Solo se acepta ... (columnas invalidas): ", paste(invalid_columns, collapse = ", "))
      }

      name <<- indicator_name
      columns_part <- paste(target_columns, collapse = ",")
      if (regex_prefix_search) {
        codes_part <- paste(sprintf("'%s%%'", definition_codes), collapse = ",")

        sql_rule <<- sprintf(
          "with codes_to_compare as (select unnest([%s]) as code_to_compare )
select distinct row_index_id, %s true as %s from (	select a.row_index_id,len(array_agg(list_diag_)) as n_diag_match, first(a.n_diag_no_null) as  n_diag_no_null  from (
select * from (select row_index_id, unnest(list_diag) as list_diag_, n_diag_no_null from (select row_index_id , [
%s] as list_diag,
length(array_filter(list_diag, x -> x IS NOT NULL)) AS n_diag_no_null  from dataframe_) a )where list_diag_ is not null) a
left join  codes_to_compare b
on a.list_diag_ like b.code_to_compare where b.code_to_compare is not null
group by a.row_index_id) where n_diag_match = n_diag_no_null",
          codes_part,
          if (inverse_match_result) "NOT" else "",
          indicator_name,
          columns_part
        )
      } else {
        codes_part <- paste(sprintf("'%s'", definition_codes), collapse = ",")

        sql_rule <<- sprintf(
          "with codes_to_compare as (select unnest([%s]) as code_to_compare )
select distinct row_index_id, %s true as %s from (	select a.row_index_id,len(array_agg(list_diag_)) as n_diag_match, first(a.n_diag_no_null) as  n_diag_no_null  from (
select * from (select row_index_id, unnest(list_diag) as list_diag_, n_diag_no_null from (select row_index_id , [
%s] as list_diag,
length(array_filter(list_diag, x -> x IS NOT NULL)) AS n_diag_no_null  from dataframe_) a )where list_diag_ is not null) a
left join  codes_to_compare b
on a.list_diag_ = b.code_to_compare where b.code_to_compare is not null
group by a.row_index_id) where n_diag_match = n_diag_no_null",
          codes_part,
          if (inverse_match_result) "NOT" else "",
          indicator_name,
          columns_part
        )
      }
    }
  )
)






#' MatchAllWhere indicator
#'
#' @description
#' A class that creates the indicator in SQL form which evaluates whether all of
#' the target columns match the specified definition codes under the conditions
#' defined by the filter columns and lookup values.
#' The matching will only be applied to those target columns that are in the
#' same order as the filter columns and satisfy the conditions in lookup values,
#' ensuring that the indicator is TRUE only if every such target column satisfies the matching criteria.
#'
#'
#' @param rule_engine The rule engine containing the dataset where the indicators will be applied.
#' @param indicator_name A string representing the name of the indicator, assigned to the `name` property.
#' @param target_columns A vector specifying the column names where the values from `definition_codes` will be searched.
#'                       Searches will only be performed in the corresponding column of `target_columns` if the condition is met
#'                       in the column of the same index in `filter_columns`.
#' @param definition_codes A set of codes used to define the matching criteria applied to `target_columns`.
#' @param filter_columns A vector specifying the columns that define the conditions under which the `lookup_values` must hold.
#'                       These conditions are evaluated in order and are directly linked to the columns of `target_columns`.
#' @param lookup_values A list of values used to define logical conditions that must be satisfied in the order of `filter_columns`.
#' @param regex_prefix_search Logical value indicating whether to use regex-based prefix searches (`TRUE`) or exact matches (`FALSE`).
#' @param inverse_match_result Logical value indicating whether to invert the match results (`TRUE`) or keep them as is (`FALSE`).
#'
#' @return Returns an instance of MatchAnyWhere with a generated SQL rule stored in the `sql_rule` field.
#'
#' @examples
#' \dontrun{
#' hosp_dataframe <- data.frame(
#'   episode_id = c(1, 2, 3),
#'   age = c(45, 60, 32),
#'   sex = c("M", "F", "M"),
#'   diagnosis1 = c("F10.10", "I20", "I60"),
#'   diagnosis2 = c("E11", "J45", "I25"),
#'   diagnosis3 = c("I60", "K35", "F10.120"),
#'   present_on_admission_d1 = c(FALSE,FALSE,FALSE),
#'   present_on_admission_d2 = c("No","Yes","No"),
#'   present_on_admission_d3 = c(FALSE,TRUE,TRUE)
#' )
#'
#'
#' reng <- RuleEngine(hosp_dataframe,"episode_id")
#'
#' target_columns <- c("diagnosis2","diagnosis3")
#' definition_codes <- c("F10.10","F10.11","F10.120","F10.121")
#'
#' filter_columns <- c("present_on_admission_d2","present_on_admission_d3")
#' lookup_values <- c("Yes", "true")
#'
#' alcohol_indicator_poa <- IndicR::MatchAllWhere(
#'                 reng,            # rule_engine
#'                 "alcohol_i_poa", # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 definition_codes # Codes to look for in the columns,
#'                 filter_columns,
#'                 lookup_values
#'                 )
#' alcohol_i_regex_poa <- IndicR::MatchAllWhere(
#'                 reng,            # rule_engine
#'                 "alcohol_i_regex_poa", # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 c("F10") # Codes to look for in the columns (start with "F10"),
#'                 filter_columns,
#'                 lookup_values,
#'                 regex_prefix_search = TRUE
#'                 )
#'
#'
#' indicators_list <- list(alcohol_indicator_poa, alcohol_i_regex_poa)
#' reng$run_indicators(indicators_list, append_results = FALSE, csv_path="./results.csv")
#' }
#' @export
#'
#'
MatchAllWhere <- function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE,
                          inverse_match_result= FALSE){
  MatchAllWhereClass$new(rule_engine, indicator_name, target_columns, definition_codes,
                         filter_columns, lookup_values, regex_prefix_search,
                         inverse_match_result)
}

#'
MatchAllWhereClass <- setRefClass(
  "MatchAllWhere",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE,
                          inverse_match_result= FALSE) {

      invalid_columns <- setdiff(target_columns, rule_engine$columns)
      if (length(invalid_columns) > 0) {
        stop("Solo se acepta ... (columnas invalidas): ", paste(invalid_columns, collapse = ", "))
      }

      invalid_columns <- setdiff(filter_columns, rule_engine$columns)
      if (length(invalid_columns) > 0) {
        stop("Solo se acepta ... (columnas invalidas): ", paste(invalid_columns, collapse = ", "))
      }


      if (length(target_columns) != length(filter_columns)) {
        stop("La longitud debe ser la misma")
      }

      name <<- indicator_name

      when_str <- ""
      for (e in lookup_values) {
        when_str <- paste0(when_str, sprintf(" WHEN x == '%s' THEN TRUE", e))
      }

      case_when_part <- sprintf("CASE %s ELSE FALSE END", when_str)

      columns_part <- paste(target_columns, collapse = ",")
      filter_columns_part <- paste(filter_columns, collapse = ",")
      query_where <- sprintf("list_where([%s], list_transform([%s], x -> (%s)))",
                             columns_part, filter_columns_part, case_when_part)
      if (regex_prefix_search) {
        codes_part <- paste(sprintf("'%s%%'", definition_codes), collapse = ",")
        sql_rule <<- sprintf("with codes_to_compare as (select distinct unnest([%s]) as code_to_compare )
select  distinct row_index_id, %s true as %s from (select a.row_index_id,len(array_agg(codes_where)) as n_diag_match, first(n_diag_no_null) as  n_diag_no_null
from (
	select row_index_id, unnest(listwhere_result) as codes_where,n_diag_no_null from (
	select row_index_id , %s as listwhere_result , length(listwhere_result) AS n_diag_no_null
	from main.dataframe_ where
	len(listwhere_result) > 0)) a
left join codes_to_compare b
on a.codes_where like b.code_to_compare where b.code_to_compare is not null
group by a.row_index_id) where n_diag_match = n_diag_no_null",
                             codes_part, if (inverse_match_result) "NOT" else "", indicator_name, query_where)

      } else {
        codes_part <- paste(sprintf("'%s'", definition_codes), collapse = ",")

        sql_rule <<- sprintf("with codes_to_compare as (select distinct unnest([%s]) as code_to_compare )
select  distinct row_index_id, %s true as %s from (select a.row_index_id,len(array_agg(codes_where)) as n_diag_match, first(n_diag_no_null) as  n_diag_no_null
from (
	select row_index_id, unnest(listwhere_result) as codes_where,n_diag_no_null from (
	select row_index_id , %s as listwhere_result , length(listwhere_result) AS n_diag_no_null
	from main.dataframe_ where
	len(listwhere_result) > 0)) a
left join codes_to_compare b
on a.codes_where = b.code_to_compare where b.code_to_compare is not null
group by a.row_index_id) where n_diag_match = n_diag_no_null",
                             codes_part, if (inverse_match_result) "NOT" else "", indicator_name, query_where)
      }
    }
  )
)


#' CustomMatch
#'
#' @description
#' A class that creates a custom SQL indicator based on user-defined logic, allowing
#' for flexible evaluation of conditions within a dataset.
#'
#'
#' @param indicator_name A string representing the name of the indicator.
#' @param sql_logic A string containing the custom SQL logic to be applied for evaluation.
#' @return Returns an instance of CustomMatch with the generated SQL query.
#'
#' @examples
#' \dontrun{
#' hosp_dataframe <- data.frame(
#'   episode_id = c(1, 2, 3),
#'   age = c(45, 60, 32),
#'   sex = c("M", "F", "M"),
#'   diagnosis1 = c("F10.10", "I20", "I60"),
#'   diagnosis2 = c("E11", "J45", "I25"),
#'   diagnosis3 = c("I60", "K35", "F10.120")
#' )
#'
#' reng <- RuleEngine(hosp_dataframe,"episode_id")
#'
#' target_columns <- c("diagnosis1")
#'
#' definition_codes <- c("F10.10","F10.11","F10.120","F10.121")
#'
#' alcohol_indicator <- IndicR::MatchAll(
#'                 reng,            # rule_engine
#'                 "alcohol_i",     # Indicator name
#'                 target_columns,  # Columns where the search is performed.
#'                 definition_codes # Codes to look for in the columns
#'                 )
#'
#'
#' custom_alcohol_indicator <- IndicR::CustomMatch(
#'                             "alcohol_i_plus40", # Name of the indicator
#'                             "alcohol_i AND age >= 40" # Logic of the indicator
#'                         )
#'
#' indicators_list <- list(alcohol_indicator,custom_alcohol_indicator)
#' reng$run_indicators(
#'   indicators_list,
#'   append_results = FALSE,
#'   csv_path="./results.csv"
#'   )
#' }
#' @references Explore all the logical operators you can use in DuckDB https://duckdb.org/docs/stable/sql/query_syntax/where
#' @export
#'
CustomMatch <- function(indicator_name, sql_logic){
  return(CustomMatchClass$new(indicator_name, sql_logic))
}


CustomMatchClass <- setRefClass(
  "CustomMatch",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(indicator_name, sql_logic) {
    name <<- indicator_name
    sql_rule <<- sprintf("select distinct row_index_id, true as %s  FROM main.dataframe_ where %s",
                         indicator_name, sql_logic)

    }
  )
)

