from delta.tables import DeltaTable

def merge_delta_tables(spark, source_df, target_path, join_expr, update_expr=None):
    """Performs robust upserts into Target Delta table from Source DataFrame"""
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table = DeltaTable.forPath(spark, target_path)
        
        builder = target_table.alias("t").merge(
            source_df.alias("s"),
            join_expr
        )
        
        if update_expr:
            builder = builder.whenMatchedUpdate(set=update_expr)
        else:
            builder = builder.whenMatchedUpdateAll()
            
        builder.whenNotMatchedInsertAll().execute()
    else:
        # Table doesn't exist, execute initial write
        source_df.write.format("delta").mode("overwrite").save(target_path)
