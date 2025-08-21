#!/bin/bash

# Deploy DAG to Google Cloud Composer
# Usage: ./deploy.sh <environment-name> <location> <project-id>

set -e

COMPOSER_ENV=${1:-"dwh-composer-env"}
LOCATION=${2:-"europe-southwest1"}
PROJECT_ID=${3:-"dwh-building"}

echo "🚀 Deploying to Composer Environment: $COMPOSER_ENV in $LOCATION"

# Validate inputs
if [ -z "$COMPOSER_ENV" ] || [ -z "$LOCATION" ] || [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: Missing required parameters"
    echo "Usage: ./deploy.sh <environment-name> <location> <project-id>"
    exit 1
fi

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "❌ Error: Please authenticate with gcloud first"
    echo "Run: gcloud auth login"
    exit 1
fi

# Set project
gcloud config set project $PROJECT_ID

echo "📁 Uploading DAG files..."
# Upload main DAG
gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV \
    --location $LOCATION \
    --source dags/dwh_pipeline_dag.py

# Upload utilities
gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV \
    --location $LOCATION \
    --source dags/dwh_pipeline_utils.py

echo "📦 Uploading batch_only modules..."
# Upload batch extraction modules
gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV \
    --location $LOCATION \
    --source ../batch_only/batch_extractor.py \
    --destination batch_only/

gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV \
    --location $LOCATION \
    --source ../batch_only/gcs_to_bq/main.py \
    --destination batch_only/

echo "⚙️ Uploading configuration..."
# Upload configuration files to data folder
gcloud composer environments storage data import \
    --environment $COMPOSER_ENV \
    --location $LOCATION \
    --source config/composer_config.json \
    --destination config.json

# Upload batch_only config (if different from composer config)
gcloud composer environments storage data import \
    --environment $COMPOSER_ENV \
    --location $LOCATION \
    --source ../batch_only/config/config.json \
    --destination batch_config.json

echo "🔧 Setting Airflow Variables..."
# Set required variables
gcloud composer environments run $COMPOSER_ENV \
    --location $LOCATION \
    variables set -- \
    GCP_PROJECT_ID $PROJECT_ID

gcloud composer environments run $COMPOSER_ENV \
    --location $LOCATION \
    variables set -- \
    GCS_BUCKET_NAME "valsurtruck-dwh-bronze"

gcloud composer environments run $COMPOSER_ENV \
    --location $LOCATION \
    variables set -- \
    MYSQL_DATABASE "pk_gest_xer"

gcloud composer environments run $COMPOSER_ENV \
    --location $LOCATION \
    variables set -- \
    BIGQUERY_LOCATION "US"

echo "✅ Deployment completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Upload your secrets.json and service-account.json to the Composer data folder"
echo "2. Access Airflow UI: https://console.cloud.google.com/composer/environments"
echo "3. Enable the 'dwh_pipeline' DAG"
echo "4. Monitor the first run in the Airflow UI"
echo ""
echo "🔒 Security reminder:"
echo "- Upload secrets.json: gcloud composer environments storage data import --environment $COMPOSER_ENV --location $LOCATION --source ../batch_only/config/secrets.json --destination secrets.json"
echo "- Upload service account: gcloud composer environments storage data import --environment $COMPOSER_ENV --location $LOCATION --source ../batch_only/.keys/service-account.json --destination service-account.json"
