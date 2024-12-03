# aws-eks-spark-examples
Contains the Code for Granica AI's Data Engineer Take Home Assessment

## Local Setup
1. [OPTIONAL] Create a virtual environment and activate.
```bash
python3 -m venv myenv && source myenv/bin/activate
```

2. Install all required dependencies.

```bash
pip install -r requirements.txt
```

3. Complete AWS CLI setup and export following variables.
```bash
export AWS_ACCESS_KEY_ID='your_access_key_id' \
export AWS_SECRET_ACCESS_KEY='your_secret_access_key'
```

4. Run the scripts/etl.py file to load the data. This creates a local Iceberg Table in warehouse/logs folder in s3 bucket
```bash
spark-submit --properties-file spark-app.conf scripts/etl.py sparksubmit s3a://granica-demo-logs-data/really_large_access.log
```

5. Run the scripts/analytics.py file to get top5 query output in s3 bucket.
```bash
spark-submit --properties-file spark-app.conf scripts/analytics.py sparksubmit s3a://granica-demo-logs-data/output
```