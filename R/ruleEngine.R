# IndicR


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


# Restricted words
restricted_words <- c('ABORT','ABS','ABSOLUTE','ACTION','ADD','ADMIN','AFTER','AGGREGATE','ALL','ALSO','ALTER','ALWAYS',
                      'ANALYSE','ANALYZE','AND','ANY','ARRAY','AS','ASC','ASSERTION','ASSIGNMENT','ASYMMETRIC','AT',
                      'ATTRIBUTE','AUTHORIZATION','BACKWARD','BEFORE','BEGIN','BETWEEN','BIGINT','BINARY','BIT',
                      'BOOLEAN','BOTH','BY','CACHE','CALL','CALLED','CASCADE','CASCADED','CASE','CAST','CATALOG',
                      'CHAIN','CHAR','CHARACTER','CHARACTERISTICS','CHECK','CHECKPOINT','CLASS','CLOSE','CLUSTER',
                      'COALESCE','COLLATE','COLLATION','COLUMN','COLUMNS','COMMENT','COMMENTS','COMMIT','COMMITTED',
                      'CONCURRENTLY','CONFIGURATION','CONFLICT','CONNECTION','CONSTRAINT','CONSTRAINTS','CONTENT',
                      'CONTINUE','CONVERSION','COPY','COST','CREATE','CROSS','CSV','CUBE','CURRENT','CURRENT_CATALOG',
                      'CURRENT_DATE','CURRENT_ROLE','CURRENT_SCHEMA','CURRENT_TIME','CURRENT_TIMESTAMP',
                      'CURRENT_USER','CURSOR','CYCLE','DATA','DATABASE','DAY','DEALLOCATE','DEC','DECIMAL','DECLARE',
                      'DEFAULT','DEFAULTS','DEFERRABLE','DEFERRED','DEFINED','DEFINER','DELETE','DELIMITER','DELIMITERS',
                      'DEPENDS','DESC','DETACH','DICTIONARY','DISABLE','DISCARD','DISTINCT','DO','DOCUMENT','DOMAIN',
                      'DOUBLE','DROP','EACH','ELSE','ENABLE','ENCODING','ENCRYPTED','END','ENUM','ESCAPE','EVENT',
                      'EXCEPT','EXCLUDE','EXCLUDING','EXCLUSIVE','EXECUTE','EXISTS','EXPLAIN','EXTENSION','EXTERNAL',
                      'EXTRACT','FALSE','FAMILY','FETCH','FILTER','FIRST','FLOAT','FOLLOWING','FOR','FORCE','FOREIGN',
                      'FORWARD','FREEZE','FROM','FULL','FUNCTION','FUNCTIONS','GENERATED','GLOBAL','GRANT','GRANTED',
                      'GREATEST','GROUP','GROUPING','GROUPS','HANDLER','HAVING','HEADER','HOLD','HOUR','IDENTITY','IF',
                      'ILIKE','IMMEDIATE','IMMUTABLE','IMPLICIT','IMPORT','IN','INCLUDE','INCLUDING','INCREMENT',
                      'INDEX','INDEXES','INHERIT','INHERITS','INITIALLY','INLINE','INNER','INOUT','INPUT','INSENSITIVE',
                      'INSERT','INSTEAD','INT','INTEGER','INTERSECT','INTERVAL','INTO','INVOKER','IS','ISNULL',
                      'ISOLATION','JOIN','KEY','LABEL','LANGUAGE','LARGE','LAST','LATERAL','LEADING','LEAKPROOF',
                      'LEAST','LEFT','LEVEL','LIKE','LIMIT','LISTEN','LOAD','LOCAL','LOCALTIME','LOCALTIMESTAMP',
                      'LOCATION','LOCK','LOCKED','LOGGED','MAPPING','MATCH','MATERIALIZED','MAXVALUE','METHOD','MINUTE',
                      'MINVALUE','MODE','MONTH','MOVE','NAME','NAMES','NATIONAL','NATURAL','NCHAR','NEW','NEXT','NO',
                      'NONE','NOT','NOTHING','NOTIFY','NOTNULL','NOWAIT','NULL','NULLIF','NULLS','NUMERIC','OBJECT',
                      'OF','OFF','OFFSET','OIDS','OLD','ON','ONLY','OPERATOR','OPTION','OPTIONS','OR','ORDER',
                      'ORDINALITY','OTHERS','OUT','OUTER','OVER','OVERLAPS','OVERLAY','OVERRIDING','OWNED','OWNER',
                      'PARALLEL','PARSER','PARTIAL','PARTITION','PASSING','PASSWORD','PLACING','PLANS','POLICY',
                      'POSITION','PRECEDING','PRECISION','PREPARE','PREPARED','PRESERVE','PRIMARY','PRIOR',
                      'PRIVILEGES','PROCEDURAL','PROCEDURE','PROCEDURES','PROGRAM','PUBLICATION','QUOTE','RANGE','READ',
                      'REAL','REASSIGN','RECHECK','RECURSIVE','REF','REFERENCES','REFERENCING','REFRESH','REINDEX',
                      'RELATIVE','RELEASE','RENAME','REPEATABLE','REPLACE','REPLICA','REQUIRING','RESET','RESTART',
                      'RESTRICT','RETURNING','RETURNS','REVOKE','RIGHT','ROLE','ROLLBACK','ROLLUP','ROUTINE','ROUTINES',
                      'ROW','ROWS','RULE','SAVEPOINT','SCHEMA','SCHEMAS','SCROLL','SEARCH','SECOND','SECURITY','SELECT',
                      'SEQUENCE','SEQUENCES','SERIALIZABLE','SERVER','SESSION','SESSION_USER','SET','SETOF','SETS',
                      'SHARE','SHOW','SIMILAR','SIMPLE','SKIP','SMALLINT','SNAPSHOT','SOME','SQL','STABLE','STANDALONE',
                      'START','STATEMENT','STATISTICS','STDIN','STDOUT','STORAGE','STORED','STRICT','STRIP',
                      'SUBSCRIPTION','SUBSTRING','SYMMETRIC','SYSID','SYSTEM','TABLE','TABLES','TABLESAMPLE',
                      'TABLESPACE','TEMP','TEMPLATE','TEMPORARY','TEXT','THEN','TIES','TIME','TIMESTAMP','TO',
                      'TRAILING','TRANSACTION','TRANSFORM','TREAT','TRIGGER','TRIM','TRUE','TRUNCATE','TRUSTED',
                      'TYPE','TYPES','UNBOUNDED','UNCOMMITTED','UNENCRYPTED','UNION','UNIQUE','UNKNOWN','UNLISTEN',
                      'UNLOGGED','UNTIL','UPDATE','USER','USING','VACUUM','VALID','VALIDATE','VALIDATOR','VALUE',
                      'VALUES','VARCHAR','VARIADIC','VARYING','VERBOSE','VERSION','VIEW','VIEWS','VOLATILE','WHEN',
                      'WHERE','WHITESPACE','WINDOW','WITH','WITHIN','WITHOUT','WORK','WRAPPER','WRITE','XML',
                      'XMLATTRIBUTES','XMLCONCAT','XMLELEMENT','XMLEXISTS','XMLFOREST','XMLNAMESPACES','XMLPARSE',
                      'XMLPI','XMLROOT','XMLSERIALIZE','XMLTABLE','YEAR','YES','ZONE','ROW_INDEX_ID')



check_params_simple <- function(rule_engine,indicator_name,target_columns,definition_codes,regex_prefix_search){
  if (!inherits(rule_engine, "RuleEngine")) {
    stop("The 'rule_engine' argument must be of class RuleEngine")
  }
  if (!is.character(indicator_name)) {
    stop("The 'indicator_name' argument must be of type character")
  }
  if (length(setdiff(toupper(indicator_name), restricted_words)) == 0) {
    stop("The 'indicator_name' argument cannot match any restricted word.")
  }
  if (!is.vector(target_columns) || !all(sapply(target_columns, is.character))) {
    stop("The 'target_columns' argument must be a list of characters")
  }
  invalid_columns <- setdiff(target_columns, rule_engine$columns)
  if (length(invalid_columns) > 0) {
    stop("Some columns in 'target_columns' are not defined in the original data frame: ", paste(invalid_columns, collapse = ", "))
  }
  if (!is.vector(definition_codes) || !all(sapply(definition_codes, is.character))) {
    stop("The 'definition_codes' argument must be a list of characters")
  }
  if (!is.logical(regex_prefix_search)) {
    stop("The 'regex_prefix_search' argument must be a logical value (boolean)")
  }

  target_columns_part <- paste(sprintf("'%s'", target_columns), collapse = ",")
  query <- sprintf("select count(distinct data_type) > 1 as n_data_type
from information_schema.columns where table_name = 'dataframe_original' and column_name in (%s)",target_columns_part)
  result <- DBI::dbGetQuery(rule_engine$conn,query )
  if (result$n_data_type) {
    warning("The columns defined in 'target_columns' contain different data types")
  }
}


check_params_where <- function(rule_engine,indicator_name,target_columns,definition_codes,filter_columns,regex_prefix_search){
  if (!inherits(rule_engine, "RuleEngine")) {
    stop("The 'rule_engine' argument must be of class RuleEngine")
  }
  if (!is.character(indicator_name)) {
    stop("The 'indicator_name' argument must be of type character")
  }
  if (length(setdiff(toupper(indicator_name), restricted_words)) == 0) {
    stop("The 'indicator_name' argument cannot match any restricted word.")
  }
  if (!is.vector(target_columns) || !all(sapply(target_columns, is.character))) {
    stop("The 'target_columns' argument must be a list of characters")
  }
  invalid_columns <- setdiff(target_columns, rule_engine$columns)
  if (length(invalid_columns) > 0) {
    stop("Some columns in 'target_columns' are not defined in the original data frame: ", paste(invalid_columns, collapse = ", "))
  }
  if (!is.vector(definition_codes)  || !all(sapply(definition_codes, is.character))) {
    stop("The 'definition_codes' argument must be a list of characters")
  }
  if (!is.logical(regex_prefix_search)) {
    stop("The 'regex_prefix_search' argument must be a logical value (boolean)")
  }
  target_columns_part <- paste(sprintf("'%s'", target_columns), collapse = ",")
  query <- sprintf("select count(distinct data_type) > 1 as n_data_type
from information_schema.columns where table_name = 'dataframe_original' and column_name in (%s)",target_columns_part)
  result <- DBI::dbGetQuery(rule_engine$conn,query )
  if (result$n_data_type) {
    warning("The columns defined in 'target_columns' contain different data types")
  }

  invalid_columns <- setdiff(filter_columns, rule_engine$columns)
  if (length(invalid_columns) > 0) {
    stop("Some columns in 'filter_columns' are not defined in the original data frame: ", paste(invalid_columns, collapse = ", "))
  }


  if (length(target_columns) != length(filter_columns)) {
    stop("The length of 'target_columns' must be equal to the length of 'filter_columns'.")
  }
}

#' RuleEngine
#'
#' @description This class facilitates the processing and evaluation of indicators on a dataset by leveraging a database engine.
#' It is designed to work with data frames and ensures efficient handling of operations, including validation of unique identifiers
#' and saving results.
#'
#' @param df Data frame. Data frame object in which you want to process the indicators.
#' @param unique_identifier_column Character. Name of the column containing the unique identifiers for the provided dataframe (there can be no repeated values in this column).
#' @param database_path Character. Path where you want to save the database needed to calculate the indicators. By default ":memory:" (in memory).
#' @return A RuleEngine Object.
#' @examples
#' \dontrun{
#' df <- read.csv("dataset.csv", sep = "|", header = TRUE)
#' reng <- RuleEngine(df,"hospitalization_id")
#'
#' df2 <- read.csv("dataset2.csv", sep = "|", header = TRUE)
#' reng2 <- RuleEngine(df2,"episode_id","./indicators.duckdb")
#' }
#' @importFrom DBI dbConnect
#' @importFrom DBI dbWriteTable
#' @importFrom DBI dbExecute
#' @importFrom DBI dbGetQuery
#' @importFrom DBI dbDisconnect
#' @importFrom duckdb duckdb
#' @importFrom methods new
#' @export
RuleEngine <- function(df,
                       unique_identifier_column,
                       database_path = ":memory:"){
  return(RuleEngineClass$new(df,unique_identifier_column,database_path))
}


#' @title RunIndicators
#' @description Executes the specified indicator rules using the given RuleEngine object
#' and provides options for output customization.
#'
#' @param rule_engine RuleEngine object. Used to apply the indicator rules
#' on the associated dataset.
#' @param indicators_rules List of objects of class `SqlRuleIndicator` (MatchAny, MatchAll, MatchAnyWhere, MatchAllWhere, CustomMatch). Each object
#' represents an indicator rule to be applied.
#' @param only_true_indicators Logical. If `TRUE`, the function returns only the records
#' that meet at least one of the indicators. Defaults to `TRUE`.
#' @param append_results Logical. If `TRUE`, the function returns the original dataset
#' along with the indicators. If `FALSE`, only the `unique_identifier_column` and the
#' indicator results are returned. Defaults to `FALSE`.
#' @param to_csv Character or `NULL`. Path to save the results as a CSV file. If `NULL`,
#' no CSV file is created. Defaults to `NULL`.
#' @param to_parquet  Character or `NULL`. Path to save the results as a parquet file format
#' with gzip compression. Defaults to `NULL`.
#' @return Depends on the parameter values:
#' - If `only_true_indicators = TRUE`, only the records matching at least one indicator
#'   are returned.
#' - If `append_results = TRUE`, the full dataset with appended indicators is returned.
#' - If `append_results = FALSE`, only the `unique_identifier_column` and the indicator
#'   results are returned.
#' - If `to_csv` or `to_parquet` is specified, the results are saved to the respective file format.
#'
#' @details This function ensures that all `indicators_rules` are objects of class
#' `SqlRuleIndicator` (MatchAny, MatchAll, MatchAnyWhere, MatchAllWhere, CustomMatch). If any invalid object is passed, the function stops with an error.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#'
#' df <- read.csv("dataset.csv", sep = "|", header = TRUE)
#' rule_engine <- RuleEngine(df,"hospitalization_id")
#'
#' target_columns <- c("diagnosis1")
#' definition_codes <- c("F10.10","F10.11","F10.120","F10.121")
#'
#' alcohol_indicator <- IndicR::MatchAny(
#'                 rule_engine,
#'                 "alcohol_i",
#'                 target_columns,
#'                 definition_codes
#'                 )
#' indicators_rules <- list(alcohol_indicator)
#'
#' # Option return data frame
#' result <- IndicR::RunIndicators(
#'   rule_engine,
#'   indicators_rules,
#'   only_true_indicators = TRUE,
#'   append_results = FALSE
#'   )
#'
#' # Option save to csv file
#' IndicR::RunIndicators(
#'   rule_engine,
#'   indicators_rules,
#'   only_true_indicators = TRUE,
#'   append_results = FALSE,
#'   to_csv = "output.csv"
#'   )
#'
#' # Option save to parquet file
#' IndicR::RunIndicators(
#'   rule_engine,
#'   indicators_rules,
#'   only_true_indicators = TRUE,
#'   append_results = FALSE,
#'   to_csv = "output.parquet"
#'   )
#' }
#' @export
RunIndicators <- function(rule_engine, indicators_rules, only_true_indicators = TRUE, append_results = FALSE,
                                   to_csv = NULL, to_parquet = FALSE){

  indicators_ok <- all(sapply(indicators_rules, function(rule) inherits(rule, "SqlRuleIndicator")))
  if (!indicators_ok) {
    stop("Solo se aceptan objetos de la clase SqlRuleIndicator.")
  }

  rule_engine$run_indicators(indicators_rules, only_true_indicators, append_results,
                             to_csv, to_parquet)
}


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
#' @param rule_engine Object of class RuleEngine.The rule engine containing the dataset where indicators will be applied.
#' @param indicator_name Character. A string representing the name of the indicator, assigned to the `name` field.
#' @param target_columns List of characters.  Column names where the match is evaluated.
#'                       Searches are performed across all target columns.
#' @param definition_codes List of characters. A set of codes used to define the matching criteria for the target columns.
#' @param regex_prefix_search Logical. Logical value indicating whether to perform prefix-based regex matches (`TRUE`)
#'                            or exact matches (default) (`FALSE`).
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
#'                 reng,
#'                 "alcohol_i",
#'                 target_columns,
#'                 definition_codes
#'                 )
#'}
#'@return Returns an instance of MatchAny with a generated SQL rule.
#'@export
MatchAny <- function(rule_engine, indicator_name, target_columns,
                     definition_codes, regex_prefix_search = FALSE){

  definition_codes <- definition_codes[definition_codes != "" & !is.na(definition_codes) & !is.null(definition_codes)]
  if (length(definition_codes) < 1) {
    stop("Error: 'definition_codes' must contain at least one non-empty, non-null element.")
  }

  check_params_simple(rule_engine,indicator_name,target_columns,definition_codes,regex_prefix_search)

  return(MatchAnyClass$new(rule_engine, indicator_name, target_columns,
                    definition_codes, regex_prefix_search,
                    ))
}


#'
MatchAnyClass <- setRefClass(
  "MatchAny",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns,
                          definition_codes, regex_prefix_search = FALSE,
                          inverse_match_result = FALSE) {

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
#' @param rule_engine Object of class RuleEngine.The rule engine containing the dataset where the indicators will be applied.
#' @param indicator_name Character. A string representing the name of the indicator, assigned to the `name` property.
#' @param target_columns List of characters.  Column names where the values from `definition_codes` will be searched.
#'                       Searches will only be performed in the corresponding column of `target_columns` if the condition is met
#'                       in the column of the same index in `filter_columns`.
#' @param definition_codes List of characters. A set of codes used to define the matching criteria applied to `target_columns`.
#' @param filter_columns List of characters. Column names that define the conditions under which the `lookup_values` must hold.
#'                       These conditions are evaluated in order and are directly linked to the columns of `target_columns`.
#' @param lookup_values List of characters.A list of values used to define logical conditions that must be satisfied in the order of `filter_columns`.
#' @param regex_prefix_search Logical. Logical value indicating whether to use regex-based prefix searches (`TRUE`) or exact matches (`FALSE`).
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
#'                 reng,
#'                 "alcohol_i_poa",
#'                 target_columns,
#'                 definition_codes
#'                 filter_columns,
#'                 lookup_values
#'               )
#'
#' # Codes to look for in the columns (start with "F10")
#' alcohol_i_regex_poa <- IndicR::MatchAnyWhere(
#'                 reng,
#'                 "alcohol_i_regex_poa",
#'                 target_columns,
#'                 c("F10")
#'                 filter_columns,
#'                 lookup_values,
#'                 regex_prefix_search = TRUE
#'                 )
#'
#'
#' indicators_list <- list(alcohol_indicator_poa, alcohol_i_regex_poa)
#' IndicR::RunIndicators(reng,indicators_list, append_results = FALSE, csv_path="./results.csv")
#' }
#' @export
MatchAnyWhere <- function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE){

  definition_codes <- definition_codes[definition_codes != "" & !is.na(definition_codes) & !is.null(definition_codes)]
  if (length(definition_codes) < 1) {
    stop("Error: 'definition_codes' must contain at least one non-empty, non-null element.")
  }

  lookup_values <- lookup_values[lookup_values != "" & !is.na(lookup_values) & !is.null(lookup_values)]
  if (length(lookup_values) < 1) {
    stop("Error: 'lookup_values' must contain at least one non-empty, non-null element.")
  }

  check_params_where(rule_engine, indicator_name, target_columns, definition_codes,
                     filter_columns, regex_prefix_search)
  return(MatchAnyWhereClass$new(rule_engine, indicator_name, target_columns, definition_codes,
                                filter_columns, lookup_values, regex_prefix_search))
}


MatchAnyWhereClass <- setRefClass(
  "MatchAnyWhere",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE,
                          inverse_match_result= FALSE) {


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
#' @param rule_engine Object of class RuleEngine.The rule engine containing the dataset where indicators will be applied.
#' @param indicator_name Character. A string representing the name of the indicator, assigned to the `name` field.
#' @param target_columns List of characters.  Column names where the match is evaluated.
#'                       Searches are performed across all target columns.
#' @param definition_codes List of characters. A set of codes used to define the matching criteria for the target columns.
#' @param regex_prefix_search Logical. Logical value indicating whether to perform prefix-based regex matches (`TRUE`)
#'                            or exact matches (default) (`FALSE`).
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
#'                 reng,
#'                 "alcohol_i",
#'                 target_columns,
#'                 definition_codes
#'                 )
#' }
#' @export
MatchAll <- function(rule_engine, indicator_name, target_columns,
                     definition_codes, regex_prefix_search = FALSE){

  definition_codes <- definition_codes[definition_codes != "" & !is.na(definition_codes) & !is.null(definition_codes)]
  if (length(definition_codes) < 1) {
    stop("Error: 'definition_codes' must contain at least one non-empty, non-null element.")
  }

  check_params_simple(rule_engine,indicator_name,target_columns,definition_codes,regex_prefix_search)
  return(MatchAllClass$new(rule_engine, indicator_name, target_columns,
                           definition_codes, regex_prefix_search))
}


MatchAllClass <- setRefClass(
  "MatchAll",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns,
                          definition_codes, regex_prefix_search = FALSE,
                          inverse_match_result = FALSE) {

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
#' @param rule_engine Object of class RuleEngine.The rule engine containing the dataset where the indicators will be applied.
#' @param indicator_name Character. A string representing the name of the indicator, assigned to the `name` property.
#' @param target_columns List of characters.  Column names where the values from `definition_codes` will be searched.
#'                       Searches will only be performed in the corresponding column of `target_columns` if the condition is met
#'                       in the column of the same index in `filter_columns`.
#' @param definition_codes List of characters. A set of codes used to define the matching criteria applied to `target_columns`.
#' @param filter_columns List of characters. Column names that define the conditions under which the `lookup_values` must hold.
#'                       These conditions are evaluated in order and are directly linked to the columns of `target_columns`.
#' @param lookup_values List of characters.A list of values used to define logical conditions that must be satisfied in the order of `filter_columns`.
#' @param regex_prefix_search Logical. Logical value indicating whether to use regex-based prefix searches (`TRUE`) or exact matches (`FALSE`).
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
#'                 reng,
#'                 "alcohol_i_poa",
#'                 target_columns,
#'                 definition_codes ,
#'                 filter_columns,
#'                 lookup_values
#'                 )
#'
#' # Codes to look for in target_columns (start with "F10")
#' alcohol_i_regex_poa <- IndicR::MatchAllWhere(
#'                 reng,
#'                 "alcohol_i_regex_poa",
#'                 target_columns,
#'                 c("F10") ,
#'                 filter_columns,
#'                 lookup_values,
#'                 regex_prefix_search = TRUE
#'                 )
#'
#'
#' indicators_list <- list(alcohol_indicator_poa, alcohol_i_regex_poa)
#' IndicR::RunIndicators(reng,indicators_list, append_results = FALSE, csv_path="./results.csv")
#' }
#' @export
MatchAllWhere <- function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE){

  definition_codes <- definition_codes[definition_codes != "" & !is.na(definition_codes) & !is.null(definition_codes)]
  if (length(definition_codes) < 1) {
    stop("Error: 'definition_codes' must contain at least one non-empty, non-null element.")
  }

  lookup_values <- lookup_values[lookup_values != "" & !is.na(lookup_values) & !is.null(lookup_values)]
  if (length(lookup_values) < 1) {
    stop("Error: 'lookup_values' must contain at least one non-empty, non-null element.")
  }

  check_params_where(rule_engine,indicator_name,target_columns,definition_codes,filter_columns,regex_prefix_search)

  MatchAllWhereClass$new(rule_engine, indicator_name, target_columns, definition_codes,
                         filter_columns, lookup_values, regex_prefix_search)
}

MatchAllWhereClass <- setRefClass(
  "MatchAllWhere",
  contains = "SqlRuleIndicator",
  methods = list(
    initialize = function(rule_engine, indicator_name, target_columns, definition_codes,
                          filter_columns, lookup_values, regex_prefix_search= FALSE,
                          inverse_match_result= FALSE) {
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
#' @param indicator_name Character. A string representing the name of the indicator.
#' @param sql_logic  Character. A string containing the custom SQL logic to be applied for evaluation.
#' @return Returns an instance of CustomMatch with the generated SQL query.
#' @details
#' When a `CustomMatch` indicator depends on another previously calculated indicator,
#' the required indicator must appear before the `CustomMatch` in the list of
#' indicators provided to the `RuleEngine`.
#' Additionally, the user must ensure that all variables referenced in the `CustomMatch`
#' are present in the data frame.
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
#'                 reng,
#'                 "alcohol_i",
#'                 target_columns,
#'                 definition_codes
#'                 )
#'
#'
#' custom_alcohol_indicator <- IndicR::CustomMatch(
#'                             "alcohol_i_plus40", # Name of the indicator
#'                             "alcohol_i AND age >= 40" # Logic of the indicator
#'                         )
#'
#' indicators_list <- list(alcohol_indicator,custom_alcohol_indicator)
#' IndicR::RunIndicators(reng,
#'   indicators_list,
#'   append_results = FALSE,
#'   csv_path="./results.csv"
#'   )
#' }
#' @references Explore all the logical operators you can use in DuckDB https://duckdb.org/docs/stable/sql/query_syntax/where
#' @export
CustomMatch <- function(indicator_name, sql_logic){
  if (!is.character(indicator_name)) {
    stop("The 'indicator_name' argument must be of type character")
  }
  if (!is.character(sql_logic)) {
    stop("The 'target_columns' argument must be a list of characters")
  }
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

