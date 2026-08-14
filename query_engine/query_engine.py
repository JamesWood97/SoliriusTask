import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from stage2.stage2 import load_film_df
try:
    from .filter import Filter
except ImportError:
    from filter import Filter
from datetime import datetime

def filter_data(query, df):
    if query.operation == "between":
        condition = between_filter(query)
    elif query.operation == "in":
        condition = in_filter(query, query.values)
    elif query.operation == "not in":
        condition = not_in_filter(query, query.values)
    else:
        condition = F.lit(False)
        for value in query.values:
            condition = condition | get_filter_function(query)(query, value)
    return df.filter(condition)

def get_filter_function(query):
    if query.operation == "==":
        return equals_filter
    elif query.operation == "!=":
        return not_equal_filter
    elif query.operation == ">":
        return greater_than_filter
    elif query.operation == "<":
        return less_than_filter
    elif query.operation == ">=":
        return greater_than_or_equal_filter
    elif query.operation == "<=":
        return less_than_or_equal_filter
    elif query.operation == "in":
        return in_filter
    elif query.operation == "not in":
        return not_in_filter
    elif query.operation == "contains":
        return contains_filter
    elif query.operation == "between":
        return lambda query, values: greater_than_filter(query, values[0]).union(equals_filter(query, values[1]))
    else:
        raise Exception(f"Operation {query.operation} not supported")

def equals_filter(query, value):
    return F.col(query.column) == value

def not_equal_filter(query, value):
    return F.col(query.column) != value

def greater_than_filter(query, value):
    return F.col(query.column) > value

def less_than_filter(query, value):
    return F.col(query.column) < value

def greater_than_or_equal_filter(query, value):
    return F.col(query.column) >= value

def less_than_or_equal_filter(query, value):
    return F.col(query.column) <= value

def in_filter(query, array):
    return F.col(query.column).isin(array)

def not_in_filter(query, array):
    return ~F.col(query.column).isin(array)

def contains_filter(query, value):
    return F.col(query.column).contains(F.lit(value))

def between_filter(query):
    return F.col(query.column).between(query.values[0], query.values[1])

def cast_query_values_to_type(query, df):
    column_type = dict(df.dtypes)[query.column]
    if column_type == "date":
        query.values = [datetime.strptime(v, "%Y-%m-%d").date() for v in query.values]
    elif column_type == "int":
        query.values = [int(v) for v in query.values]
    elif column_type == "float":
        query.values = [float(v) for v in query.values]
    elif column_type == "string":
        query.values = [str(v) for v in query.values]
    elif column_type == "boolean":
        query.values = [v.lower() in ("true", "1") for v in query.values]
    else:
        raise Exception(f"Column type {column_type} not supported")

def execute_query(filter_obj, df):
    if isinstance(filter_obj, list) or isinstance(filter_obj, tuple):
        for filter_instance in filter_obj:
            df = execute_query(filter_instance, df)
    elif isinstance(filter_obj, Filter):
        cast_query_values_to_type(filter_obj, df)
        df = filter_data(filter_obj, df)
    else:
        raise Exception(f"Filter {filter_obj} not supported")
    return df
