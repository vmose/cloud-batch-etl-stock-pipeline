terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

resource "google_storage_bucket" "raw_landing" {
  name                        = "${var.gcp_project_id}-stock-ticks-raw"
  location                    = var.gcp_region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 365 # raw files older than a year move to cheaper storage, not deleted
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.gcp_region
  description = "Untransformed data as loaded from GCS. Source of truth for what was received."
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id  = "analytics"
  location    = var.gcp_region
  description = "dbt-modeled, analysis-ready tables."
}
