#!/bin/bash

# Quick setup script for Composer environment
# Run this first to create the Composer environment and install dependencies

set -e

PROJECT_ID=${1:-"dwh-building"}
COMPOSER_ENV=${2:-"dwh-composer-env"}
LOCATION=${3:-"europe-southwest1"}

echo "🔧 Setting up Composer environment for Data Warehouse Pipeline"
echo "Project: $PROJECT_ID"
echo "Environment: $COMPOSER_ENV"
echo "Location: $LOCATION"
echo ""

# Set project
gcloud config set project $PROJECT_ID

echo "📦 Creating Composer environment (this may take 15-20 minutes)..."
gcloud composer environments create $COMPOSER_ENV \
    --location $LOCATION \
    --python-version 3 \
    --node-count 3 \
    --disk-size 30GB \
    --machine-type n1-standard-1 \
    --labels env=dev,team=data

echo "📋 Installing additional Python packages..."
gcloud composer environments update $COMPOSER_ENV \
    --location $LOCATION \
    --update-pypi-packages-from-file requirements.txt

echo "✅ Composer environment setup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Run ./deploy.sh to deploy the DAG"
echo "2. Upload your secrets and service account files"
echo "3. Access Airflow UI to enable and monitor the DAG"
echo ""
echo "🌐 Airflow UI: https://console.cloud.google.com/composer/environments"
