import os
import shutil
import subprocess
import datetime
from cassandra.cluster import Cluster

# Initialize Cassandra session
def init_cassandra():
    cluster = Cluster(['127.0.0.1'])  # Change as per your Cassandra cluster
    session = cluster.connect()
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS mediastore
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    """)
    session.set_keyspace('mediastore')
    session.execute("""
    CREATE TABLE IF NOT EXISTS dataset_versions (
        dataset_name text,
        version text,
        dvc_hash text,
        timestamp timestamp,
        PRIMARY KEY (dataset_name, version)
    );
    """)
    return session

# Create a new dataset
def create_dataset(dataset_name):
    os.makedirs(f'datasets/{dataset_name}', exist_ok=True)
    print(f"Dataset {dataset_name} created.")

# Create a new version for a dataset
def create_version(dataset_name, version):
    version_path = f'datasets/{dataset_name}/{version}'
    os.makedirs(version_path, exist_ok=True)
    print(f"Version {version} for dataset {dataset_name} created.")

# Add images to a dataset version
def add_images(dataset_name, version, image_paths):
    version_path = f'datasets/{dataset_name}/{version}'
    for image_path in image_paths:
        shutil.copy(image_path, version_path)
    subprocess.run(['dvc', 'add', version_path])
    subprocess.run(['git', 'add', '.'])
    subprocess.run(['git', 'commit', '-m', f'Add images to {dataset_name} version {version}'])
    subprocess.run(['git', 'push'])
    dvc_hash = get_dvc_hash(version_path)
    session = init_cassandra()
    session.execute("""
    INSERT INTO dataset_versions (dataset_name, version, dvc_hash, timestamp)
    VALUES (%s, %s, %s, %s)
    """, (dataset_name, version, dvc_hash, datetime.datetime.now()))

# Helper function to get DVC hash
def get_dvc_hash(path):
    result = subprocess.run(['dvc', 'status', path], capture_output=True, text=True)
    for line in result.stdout.split('\n'):
        if 'new' in line:
            return line.split()[0]
    return None

# Retrieve a specific version of a dataset
def retrieve_version(dataset_name, version):
    session = init_cassandra()
    result = session.execute("""
    SELECT dvc_hash FROM dataset_versions
    WHERE dataset_name=%s AND version=%s
    """, (dataset_name, version)).one()
    if result:
        dvc_hash = result.dvc_hash
        subprocess.run(['dvc', 'checkout', dvc_hash])
        print(f"Version {version} of dataset {dataset_name} retrieved.")
    else:
        print(f"Version {version} of dataset {dataset_name} not found.")

# Example usage
if __name__ == "__main__":
    create_dataset('dataset1')
    create_version('dataset1', 'version1')
    add_images('dataset1', 'version1', ['image1.jpeg', 'image2.jpeg'])
    retrieve_version('dataset1', 'version1')
