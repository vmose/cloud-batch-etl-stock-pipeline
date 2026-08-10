variable "gcp_project_id" {
  description = "GCP project ID to provision resources in"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for GCS bucket and BigQuery datasets"
  type        = string
  default     = "US"
}
