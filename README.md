# Solirius Data Engineering Coding Test
PySpark implementation of the coding task for Solirius

## Pipeline

The pipeline is spit into two stages:
### Stage 1
 * Loads the CSV dataset
 * Transforms and casts the columns using the JSON schema
 * Writes the complete film dataset to output/films.parquet
 * 
### Stage 2
 * Reads the Stage 1 output.
 * Splits films containing multiple genres into their individual genres
 *Writes a separate Parquet dataset for each genre under output/genres

Luigi ensures that Stage 2 only runs after Stage 1 completes successfully. The pipeline can be restarted and previous successful stages skipped if a run fails

## Film similarity

film_similarity/film_similarity.py compares a selected film with the rest of the dataset

The similarity score uses:

* Title word overlap
* Genre similarity
* Director similarity
* Cast similarity
* Adult rating status

These measures are combined into a weighted similarity percentage. The caller can provide a minimum similarity threshold

## Query engine

The query engine accepts either one Filter object or a collection of filters. Multiple filters are applied sequentially

Supported operations include:

* == and !=
* <, >, <= and >=
* in and not in
* contains
* between

Also bundled is an automated unit test suite for the query engine and its filters
