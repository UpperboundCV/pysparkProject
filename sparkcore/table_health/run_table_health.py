import os
import sys
import argparse

os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark/"
os.environ["PYTHONPATH"] = "/opt/cloudera/parcels/CDH/lib/spark/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/"
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")
sys.path.append('/nfs/msa/dapscripts/ka/pln/dev/tfm/pys/collection/tableHealth/sparkcore')
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--env", required=True, help="environment: local, dev, or prod")
    ap.add_argument("-e", "--entity", required=True, help="entity: it can be only ka or ay (lower case)")
    ap.add_argument("-p", "--process_date", required=True, help="data date to process")
    ap.add_argument("-s", "--schema", required=True, help="schema name")
    ap.add_argument("-t", "--table_name", required=True, help="table name")
