#!/bin/bash

# Change to the terraform-gcp directory
cd terraform-gcp || exit

# Load environment variables from .env file in the root directory
if [ -f ../.env ]; then
  export $(grep -v '^#' ../.env | xargs -d '\n')
else
  echo ".env file not found in the root directory!"
  exit 1
fi

echo "Initializing Terraform..."
terraform init

echo "Applying Terraform configuration..."
terraform apply -auto-approve \
  -var="cloud_sql_admins=${CLOUD_SQL_ADMINS}" \
  -var="db_instance_name=${DB_INSTANCE_NAME}" \
  -var="db_password=${DB_PASSWORD}" \
  -var="db_user=${DB_USER}" \
  -var="gcs_admins=${GCS_ADMINS}" \
  -var="notification_email=${NOTIFICATION_EMAIL}" \
  -var="project_id=${PROJECT_ID}" \
  -var="vm_service_account=${VM_SERVICE_ACCOUNT}"

echo "Terraform deployment complete. Infrastructure details:"
terraform output