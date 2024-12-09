# take-home-test
Steps for deployment from terminal

STEP 1
git clone repo locally

STEP 2
gcloud auth login

STEP 3
gcloud config set project devoteam-interview-pav-dhillon

STEP 4
bq mk test_3

STEP 5
gcloud functions deploy process_gcs_file \
    --runtime python310 \
    --trigger-resource test-3-pd-cf \
    --trigger-event google.storage.object.finalize \
    --region us-central1 \
    --entry-point process_gcs_file

STEP 6
Upload file to GCS bucket to test
