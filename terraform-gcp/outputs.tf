output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "gcs_bucket_name" {
  value = google_storage_bucket.tfl_accidents_data_lake.name
}

output "gcs_readers" {
  value = var.gcs_readers
}

output "gcs_writers" {
  value = var.gcs_writers
}

output "gcs_admins" {
  value = var.gcs_admins
}

output "cloud_sql_instance" {
  value = google_sql_database_instance.tfl_accidents_db.name
}

output "cloud_sql_db" {
  value = google_sql_database.tfl_accidents_db.name
}

output "cloud_sql_user" {
  value = google_sql_user.tfl_accidents_user.name
}

output "dlt_service_account_email" {
  value = google_service_account.dlt_service_account.email
}

output "dbt_service_account_email" {
  value = google_service_account.dbt_service_account.email
}

output "cloud_sql_admins" {
  value = var.cloud_sql_admins
}

output "cloud_sql_clients" {
  value = var.cloud_sql_clients
}