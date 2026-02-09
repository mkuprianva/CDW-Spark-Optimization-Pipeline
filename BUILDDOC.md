# CDW Pipe
- the purpose of this pipeline is to quickly perform joins and filters
- Although databricks has some functionality like this such as liquid clustering this is not highly optimized and with shuffles we can have significant IO throughput and delay


## Premise
- Shuffles is going to be the biggest issue and the current sql paradim does not check or filter prior to join leading to large datasets being joined together
- Spark and delta tables are fundamentaly different that SQL where the former is optimized for columnar format and the latter is row based with B or B+ trees.
- Each join consists of several parts
    - Filtering and creation of a shared index that will depend on the join type
    - Searching along the unified index to find matches in the table
    - Zip of the two dataframes along the index


## Data structures
- Index
 - Index dataframe (keep as array)