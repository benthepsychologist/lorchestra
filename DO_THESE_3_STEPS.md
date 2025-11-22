# Do These 3 Manual Steps (5 minutes)

I can't access GCP Console for you, but these 3 steps are super quick:

## Step 1: Create Project (2 min)

**Click this link:** https://console.cloud.google.com/projectcreate

Fill in:
- Project name: `lorchestra`
- Project ID: `local-orchestration`

Click **CREATE**

Wait for "Project created" notification.

---

## Step 2: Create Service Account (2 min)

**Click this link:** https://console.cloud.google.com/iam-admin/serviceaccounts?project=local-orchestration

1. Click **+ CREATE SERVICE ACCOUNT**

2. Fill in:
   - Service account name: `lorchestra-events`
   - Click **CREATE AND CONTINUE**

3. Add these 2 roles:
   - Select: `BigQuery Data Editor`
   - Click **+ ADD ANOTHER ROLE**
   - Select: `BigQuery Job User`
   - Click **CONTINUE** → **DONE**

---

## Step 3: Download Key (1 min)

In the service accounts list:

1. Find `lorchestra-events@local-orchestration.iam.gserviceaccount.com`
2. Click the **⋮** (three dots)
3. Click **Manage keys**
4. Click **ADD KEY** → **Create new key**
5. Select **JSON**
6. Click **CREATE**

**Save the downloaded file to:**
```
/workspace/lorchestra/credentials/lorchestra-events-key.json
```

(Or anywhere you like - you'll tell me the path)

---

## ✅ Done?

Once you have the JSON key file saved, tell me the path and I'll:
- ✅ Enable BigQuery API
- ✅ Create the dataset
- ✅ Create both tables (event_log, raw_objects)
- ✅ Configure everything
- ✅ Verify it all works

Just say: "Key saved at /path/to/key.json"
