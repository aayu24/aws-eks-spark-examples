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

3. Unzip the data.zip file
```bash
python3 -m zipfile -e data.zip .
```

4. Run the scripts/etl.py file to load the data. This creates a local Iceberg Table in warehouse/demo/logs folder.
```bash
spark-submit --properties-file spark-app.conf --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0 --py-files scripts/log_parser_utils.py scripts/etl.py sparksubmit really_large_access.log
```

5. Run the scripts/analytics.py file to get top5 query output.
```bash
spark-submit --properties-file spark-app.conf --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0 scripts/analytics.py sparksubmit
```
