package porcupine

import (
	"sort"
	"strings"
)

var duckLakeFunctions = strings.Fields(`
	count count_star count_if sum avg min max first last any_value arg_min arg_max
	bool_and bool_or list array_agg string_agg median quantile_cont quantile_disc quantile
	approx_count_distinct approx_quantile stddev_pop stddev_samp var_pop var_samp
	abs ceil ceiling floor round trunc sqrt cbrt pow power exp exp2 ln log log2 log10
	greatest least if ifnull nullif coalesce isnan isfinite isinf
	length char_length lower upper trim ltrim rtrim concat concat_ws substring substr
	left right starts_with ends_with contains strpos position replace reverse
	string_split split_part regexp_matches regexp_full_match regexp_extract regexp_replace
	json_extract json_extract_string json_value json_type json_valid json_array_length
	json_keys json_contains json_exists json_array json_object to_json
	date_trunc date_part extract date_diff date_sub date_add strftime strptime try_strptime
	timezone epoch epoch_ms epoch_us epoch_ns make_date make_timestamp make_timestamptz
	year quarter month week day dayofweek dayofyear hour minute second
	current_date current_timestamp now today age last_day time_bucket
	row_number rank dense_rank percent_rank cume_dist ntile lag lead first_value last_value nth_value
	list_count list_sum list_avg list_min list_max list_distinct list_contains list_extract
	array_length array_contains struct_extract
	+ - * / // % ** -> ->> ~~ !~~ ~~* !~~* ~ !~ || ~~_escape !~~_escape
`)

func duckLakeSQLFunctions() []string {
	names := append([]string(nil), duckLakeFunctions...)
	sort.Strings(names)
	return names
}
