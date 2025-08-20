# Google Cloud Storage Setup Guide

This guide will help you set up secure access to Google Cloud Storage for the MySQL to GCP Data Pipeline.

## Step 1: Create GCP Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Note down your **Project ID**

## Step 2: Enable Cloud Storage API

1. In the GCP Console, go to **APIs & Services > Library**
2. Search for "Cloud Storage API"
3. Click **Enable**

## Step 3: Create Storage Bucket

1. Go to **Cloud Storage > Buckets**
2. Click **Create Bucket**
3. Choose:
   - **Name**: `your-company-dwh-bronze` (must be globally unique)
   - **Location**: Choose your preferred region
   - **Storage class**: Standard
   - **Access control**: Uniform
4. Click **Create**

## Step 4: Create Service Account

1. Go to **IAM & Admin > Service Accounts**
2. Click **Create Service Account**
3. Fill in:
   - **Name**: `mysql-to-gcs-extractor`
   - **Description**: `Service account for MySQL data extraction to GCS`
4. Click **Create and Continue**
5. Add role: **Storage Admin**
6. Click **Continue** and **Done**

## Step 5: Create and Download Service Account Key

1. In the Service Accounts list, click on your new service account
2. Go to the **Keys** tab
3. Click **Add Key > Create new key**
4. Choose **JSON** format
5. Click **Create**
6. Save the downloaded JSON file securely (e.g., `gcp-service-account.json`)

## Step 6: Update Configuration

1. Update your `secrets.json` file:
   ```json
   {
     "database": {
       "username": "root",
       "password": "your_password"
     },
     "gcp": {
       "project_id": "your-actual-project-id",
       "bucket_name": "your-company-dwh-bronze",
       "service_account_key_path": "C:/path/to/gcp-service-account.json"
     }
   }
   ```

## Step 7: Test Connection

Run the extractor to test GCS connection:
```bash
python mysql_to_gcs_extractor.py
```

## Alternative: Use gcloud CLI

Instead of service account key, you can use gcloud CLI:

1. Install [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
2. Run: `gcloud auth application-default login`
3. Set your project: `gcloud config set project YOUR_PROJECT_ID`
4. In `secrets.json`, set `service_account_key_path` to `"path/to/service-account.json"` (leave as template value)

## Security Best Practices

1. **Never commit** service account keys to version control
2. **Restrict service account permissions** to only what's needed
3. **Rotate keys** regularly
4. **Use IAM conditions** for additional security
5. **Enable audit logging** for storage access

## Troubleshooting

### "Credentials not found" error:
- Check service account key path in `secrets.json`
- Verify the JSON key file exists and is readable
- Try using gcloud CLI authentication

### "Access denied" error:
- Verify service account has Storage Admin role
- Check bucket name is correct
- Ensure bucket exists and is accessible

### "Bucket not found" error:
- Verify bucket name in `secrets.json`
- Check if bucket exists in the correct project
- Ensure service account has access to the bucket

## Data Organization in GCS

The extractor organizes data with this structure:
```
your-bucket/
└── bronze/
    └── pk_gest_xer/
        └── {table_name}/
            └── date=2025/08/18/
                └── {table_name}_20250818_104637.parquet
```

This partitioned structure enables:
- Efficient querying by date
- Easy data lifecycle management
- Optimized BigQuery loading
- Clear data lineage tracking
