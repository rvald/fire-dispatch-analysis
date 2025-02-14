terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.20.0"
    }
  }
}

provider "google" {
  # Configuration options
  project     = "fdny-data-analysis"
  region      = "us-central1"
}