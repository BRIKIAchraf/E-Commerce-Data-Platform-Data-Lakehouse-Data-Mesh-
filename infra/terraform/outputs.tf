output "s3_raw_bucket_arn" {
  value = aws_s3_bucket.raw_bucket.arn
}

output "s3_curated_bucket_arn" {
  value = aws_s3_bucket.curated_bucket.arn
}

output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.kafka_cluster.bootstrap_brokers_tls
}

output "snowflake_db_name" {
  value = snowflake_database.db.name
}
