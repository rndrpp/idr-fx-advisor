variable "credentials" {
  description = "Path to GCP service account key file"
  default     = "../gcp-key.json"
}

variable "project" {
  description = "project"
  default     = "idr-fx-advisor"
}

variable "region" {
  description = "region"
  default     = "asia-southeast2"
}