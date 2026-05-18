resource "google_bigquery_dataset" "fx_data" {
  dataset_id    = "fx_data"
  location      = var.region
  friendly_name = "fx"
  description   = "daily fx rate"
}

resource "google_bigquery_table" "daily_rates" {
  dataset_id          = google_bigquery_dataset.fx_data.dataset_id
  table_id            = "daily_rates"
  deletion_protection = false

schema = jsonencode([
  { name = "date",  type = "DATE"   },
  { name = "base",  type = "STRING" },
  { name = "quote", type = "STRING" },
  { name = "rate",  type = "FLOAT"  }
])
}