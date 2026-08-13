import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from stage2.stage2 import load_film_df
import filter
from datetime import datetime

def equals_filter(query, df):
    return df.filter(F.col(query.column) == query.values)

def not_equal_filter(query, df):
    return df.filter(F.col(query.column) != query.values)

def greater_than_filter(query, df):
    return df.filter(F.col(query.column) > query.values)

def less_than_filter(query, df):
    return df.filter(F.col(query.column) < query.values)

def in_filter(query, df):
    return df.filter(F.col(query.column).isin(query.values))

def not_in_filter(query, df):
    return df.filter(~F.col(query.column).isin(query.values))

def contains_filter(query, df):
    condition = F.lit(False)
    for value in query.values:
        condition = condition | F.col(query.column).contains(F.lit(value))
    return df.filter(condition)

def between_filter(query, df):
    return df.filter(F.col(query.column).between(query.values[0], query.values[1]))

def cast_to_type(query, df):
    column_type = dict(df.dtypes)[query.column]
    if column_type == "date":
        query.values = [datetime.strptime(v, "%Y-%m-%d").date() for v in query.values]
    elif column_type == "int":
        query.values = [int(v) for v in query.values]
    elif column_type == "float":
        query.values = [float(v) for v in query.values]
    elif column_type == "string":
        query.values = [str(v) for v in query.values]
    else:
        raise Exception(f"Column type {column_type} not supported")

def execute_query(query, df):
    cast_to_type(query, df)
    if query.operation == "==":
        return equals_filter(query, df)
    elif query.operation == "!=":
        return not_equal_filter(query, df)
    elif query.operation == ">":
        return greater_than_filter(query, df)
    elif query.operation == "<":
        return less_than_filter(query, df)
    elif query.operation == ">=":
        return greater_than_filter(query, df).union(equals_filter(query, df))
    elif query.operation == "<=":
        return less_than_filter(query, df).union(equals_filter(query, df))
    elif query.operation == "in":
        return in_filter(query, df)
    elif query.operation == "not in":
        return not_in_filter(query, df)
    elif query.operation == "contains":
        return contains_filter(query, df)
    elif query.operation == "between":
        return between_filter(query, df)
    else:
        raise Exception(f"Operation {query.operation} not supported")

df = load_film_df(SparkSession.builder.appName("Query Engine").getOrCreate())
filter = filter.Filter("release_year", "between", ("1979-01-01", "2000-01-01"))
df = execute_query(filter, df)
df.show(10000)