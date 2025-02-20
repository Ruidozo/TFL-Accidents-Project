resource "google_storage_bucket" "tfl_accidents_data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true # Deletes objects when destroying the bucket

  uniform_bucket_level_access = true # Enforces IAM at bucket level

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_binding" "gcs_readers" {
  bucket = google_storage_bucket.tfl_accidents_data_lake.name
  role   = "roles/storage.objectViewer"

  members = [for user in var.gcs_readers : "user:${user}"]
}

resource "google_storage_bucket_iam_binding" "gcs_writers" {
  bucket = google_storage_bucket.tfl_accidents_data_lake.name
  role   = "roles/storage.objectCreator"

  members = [for user in var.gcs_writers : "user:${user}"]
}

resource "google_storage_bucket_iam_binding" "gcs_admins" {
  bucket = google_storage_bucket.tfl_accidents_data_lake.name
  role   = "roles/storage.admin"

  members = [for user in var.gcs_admins : "user:${user}"]
}

resource "google_sql_database_instance" "tfl_accidents_db" {
  depends_on = [google_service_networking_connection.private_vpc_connection]

  name             = var.db_instance_name
  database_version = var.db_version
  region           = var.region

  settings {
    tier              = var.db_tier
    disk_size         = var.db_disk_size
    disk_autoresize   = true
    availability_type = "ZONAL" # Change to "REGIONAL" for high availability

    ip_configuration {
      ipv4_enabled    = false # Disable public IP
      private_network = google_compute_network.tfl_accidents_vpc.id
    }

    backup_configuration {
      enabled                        = true
      start_time                     = "02:00"
      location                       = var.region
      point_in_time_recovery_enabled = true
    }
  }

  deletion_protection = false
}

resource "google_sql_database" "tfl_accidents_db" {
  name     = "tfl_accidents"
  instance = google_sql_database_instance.tfl_accidents_db.name
}

resource "google_sql_user" "tfl_accidents_user" {
  name     = var.db_user
  instance = google_sql_database_instance.tfl_accidents_db.name
  password = random_password.db_password.result # Replace with generated password
}

resource "random_password" "db_password" {
  length  = 16
  special = false
}

resource "google_project_iam_binding" "cloud_sql_admins" {
  project = var.project_id
  role    = "roles/cloudsql.admin"

  members = [for user in var.cloud_sql_admins : "user:${user}"]
}

resource "google_project_iam_binding" "cloud_sql_clients" {
  project = var.project_id
  role    = "roles/cloudsql.client"

  members = [for user in var.cloud_sql_clients : "user:${user}"]
}

# Create a dedicated VPC for Cloud SQL
resource "google_compute_network" "tfl_accidents_vpc" {
  name                    = "tfl-accidents-vpc"
  auto_create_subnetworks = false
}

# Create a subnet inside the VPC
resource "google_compute_subnetwork" "tfl_accidents_subnet" {
  name          = "tfl-accidents-subnet"
  region        = var.region
  network       = google_compute_network.tfl_accidents_vpc.id
  ip_cidr_range = "10.10.0.0/24"
}

# Enable Private Services Access for Cloud SQL
resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.tfl_accidents_vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

resource "google_compute_global_address" "private_ip_address" {
  name          = "tfl-accidents-private-ip-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 24
  network       = google_compute_network.tfl_accidents_vpc.id
}

resource "google_service_account" "dlt_service_account" {
  account_id   = "dlt-sql-sa"
  display_name = "DLT Cloud SQL Service Account"
}

resource "google_service_account" "dbt_service_account" {
  account_id   = "dbt-sql-sa"
  display_name = "dbt Cloud SQL Service Account"
}

resource "google_project_iam_binding" "dlt_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"

  members = ["serviceAccount:${google_service_account.dlt_service_account.email}"]
}

resource "google_project_iam_binding" "dbt_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"

  members = ["serviceAccount:${google_service_account.dbt_service_account.email}"]
}

resource "google_project_iam_binding" "dbt_bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"

  members = ["serviceAccount:${google_service_account.dbt_service_account.email}"]
}

resource "google_secret_manager_secret" "cloud_sql_password" {
  secret_id = "cloud-sql-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "cloud_sql_private_ip" {
  secret_id = "cloud-sql-private-ip"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "cloud_sql_password_value" {
  secret      = google_secret_manager_secret.cloud_sql_password.id
  secret_data = random_password.db_password.result
}

resource "google_secret_manager_secret_version" "cloud_sql_private_ip_value" {
  secret      = google_secret_manager_secret.cloud_sql_private_ip.id
  secret_data = google_sql_database_instance.tfl_accidents_db.private_ip_address
}

resource "google_secret_manager_secret_iam_binding" "cloud_sql_password_access" {
  secret_id = google_secret_manager_secret.cloud_sql_password.id
  role      = "roles/secretmanager.secretAccessor"

  members = [
    "serviceAccount:${google_service_account.dlt_service_account.email}",
    "serviceAccount:${google_service_account.dbt_service_account.email}"
  ]
}

resource "google_secret_manager_secret_iam_binding" "cloud_sql_private_ip_access" {
  secret_id = google_secret_manager_secret.cloud_sql_private_ip.id
  role      = "roles/secretmanager.secretAccessor"

  members = [
    "serviceAccount:${google_service_account.dlt_service_account.email}",
    "serviceAccount:${google_service_account.dbt_service_account.email}"
  ]
}

resource "google_logging_project_sink" "cloud_sql_audit_logs" {
  name        = "cloud-sql-audit-logs"
  destination = "storage.googleapis.com/${google_storage_bucket.tfl_accidents_data_lake.name}"
  filter      = "resource.type=\"cloudsql_database\""
  unique_writer_identity = true
}

resource "google_logging_project_sink" "gcs_audit_logs" {
  name        = "gcs-audit-logs"
  destination = "storage.googleapis.com/${google_storage_bucket.tfl_accidents_data_lake.name}"
  filter      = "resource.type=\"gcs_bucket\""
  unique_writer_identity = true
}

resource "google_monitoring_alert_policy" "cloud_sql_high_cpu" {
  display_name = "Cloud SQL High CPU Usage Alert"
  combiner     = "OR"
  conditions {
    display_name = "CPU usage > 80%"
    condition_threshold {
      filter          = "metric.type=\"cloudsql.googleapis.com/database/cpu/utilization\" resource.type=\"cloudsql_database\""
      threshold_value = 0.8
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.email.id]
}

resource "google_logging_project_sink" "iam_audit_logs" {
  name        = "iam-audit-logs"
  destination = "storage.googleapis.com/${google_storage_bucket.tfl_accidents_data_lake.name}"
  filter      = "resource.type=\"iam_role\""
  unique_writer_identity = true
}

resource "google_monitoring_notification_channel" "email" {
  display_name = "Email Notifications"
  type         = "email"
  labels = {
    email_address = var.notification_email
  }
}