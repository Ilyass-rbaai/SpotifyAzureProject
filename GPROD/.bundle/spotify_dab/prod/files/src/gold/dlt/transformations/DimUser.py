import dlt
@dlt.table
def fact_stream_stg():
    df=spark.readStream.table('databricksazurespotify.silver.fact_stream')
    return df
dlt.create_streaming_table('databricksazurespotify.gold.fact_stream')
dlt.create_auto_cdc_flow(
  target = "databricksazurespotify.gold.fact_stream",
  source = "fact_stream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = "2", 
  track_history_except_column_list = None, # optional
  name = None, # optional
  once = False # optional
)   