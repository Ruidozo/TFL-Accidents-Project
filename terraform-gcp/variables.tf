variable "project_id" {
  description = "Name of the project on GCP"
  type        = string
}

variable "region" {
  description = "The default region for resources"
  type        = string
  default     = "us-central1"
}

variable "gcs_readers" {
  description = "List of users/emails/service accounts that can read from GCS"
  type        = list(string)
  default     = []
}

variable "gcs_writers" {
  description = "List of users/emails/service accounts that can write to GCS"
  type        = list(string)
  default     = []
}

variable "gcs_admins" {
  description = "List of users/emails/service accounts that have full control over GCS"
  type        = list(string)
}

variable "db_instance_name" {
  description = "Cloud SQL instance name"
  type        = string
}

variable "db_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "POSTGRES_14"
}

variable "db_tier" {
  description = "Machine type for Cloud SQL"
  type        = string
  default     = "db-f1-micro" # Small free-tier eligible instance
}

variable "db_disk_size" {
  description = "Disk size in GB"
  type        = number
  default     = 10
}

variable "db_user" {
  description = "Database admin username"
  type        = string
}

variable "db_password" {
  description = "Database admin password (change before production use)"
  type        = string
}

variable "cloud_sql_admins" {
  description = "Users or service accounts with full Cloud SQL admin access"
  type        = list(string)
  default     = []
}

variable "cloud_sql_clients" {
  description = "Users or service accounts that can connect to Cloud SQL"
  type        = list(string)
  default     = []
}

variable "notification_email" {
  description = "The email address to receive monitoring alerts"
  type        = string
}