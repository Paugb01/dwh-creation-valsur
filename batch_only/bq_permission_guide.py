#!/usr/bin/env python3
"""
BigQuery Permission Analysis and Solution Guide
"""

print("🔍 BIGQUERY PERMISSION ANALYSIS")
print("=" * 80)

print("\n📧 CURRENT SERVICE ACCOUNT:")
print("   mysql-to-gcs-extractor@dwh-building.iam.gserviceaccount.com")

print("\n❌ CURRENT ISSUE:")
print("   HTTP 403 - Permission bigquery.datasets.get denied")
print("   Cannot read, write, or manage BigQuery datasets")

print("\n🎯 REQUIRED IAM ROLES:")
print("   1. BigQuery Data Editor (roles/bigquery.dataEditor)")
print("      - Allows: Create/read/update/delete tables and data")
print("      - Permissions: bigquery.datasets.get, bigquery.tables.*")
print("   ")
print("   2. BigQuery Job User (roles/bigquery.jobUser)") 
print("      - Allows: Run BigQuery jobs (queries, loads, exports)")
print("      - Permissions: bigquery.jobs.create")
print("   ")
print("   3. Storage Object Viewer (roles/storage.objectViewer)")
print("      - Allows: Read objects from GCS buckets")
print("      - Permissions: storage.objects.get, storage.objects.list")

print("\n🛠️  SOLUTION STEPS:")
print("   1. Go to Google Cloud Console:")
print("      https://console.cloud.google.com/iam-admin/iam?project=dwh-building")
print("   ")
print("   2. Find the service account:")
print("      mysql-to-gcs-extractor@dwh-building.iam.gserviceaccount.com")
print("   ")
print("   3. Add the following roles:")
print("      ✅ BigQuery Data Editor")
print("      ✅ BigQuery Job User") 
print("      ✅ Storage Object Viewer")

print("\n💡 ALTERNATIVE: Grant dataset-specific permissions:")
print("   If you want more granular control, you can grant permissions")
print("   directly on the BigQuery datasets (bronze1, silver1, gold1)")
print("   instead of project-level roles.")

print("\n🚀 GCLOUD COMMAND (if you have gcloud CLI access):")
print("""
   # Grant BigQuery Data Editor role
   gcloud projects add-iam-policy-binding dwh-building \\
       --member="serviceAccount:mysql-to-gcs-extractor@dwh-building.iam.gserviceaccount.com" \\
       --role="roles/bigquery.dataEditor"
   
   # Grant BigQuery Job User role
   gcloud projects add-iam-policy-binding dwh-building \\
       --member="serviceAccount:mysql-to-gcs-extractor@dwh-building.iam.gserviceaccount.com" \\
       --role="roles/bigquery.jobUser"
   
   # Grant Storage Object Viewer role (for GCS access)
   gcloud projects add-iam-policy-binding dwh-building \\
       --member="serviceAccount:mysql-to-gcs-extractor@dwh-building.iam.gserviceaccount.com" \\
       --role="roles/storage.objectViewer"
""")

print("\n📋 VERIFICATION:")
print("   After adding the roles, run this script again:")
print("   python check_bq_permissions.py")
print("   ")
print("   Expected result:")
print("   ✅ Can list datasets")
print("   ✅ Can read dataset bronze1, silver1, gold1")
print("   ✅ Can create/insert/delete test tables")

print("\n" + "=" * 80)
