# rpay-data-challenge

## Local Setup & Jupyter Notebook

### Clone the repo
```bash
git clone git@github.com:nsoria1/rpay-data-challenge.git
cd rpay-data-challenge
```

### Requirements
This project has been developed using:
- bash
- docker
- docker compose
- python 3.13

### Exploratory data analysis
This repository contains a folder to quickly view how is the shape of the data coming from the parquet files. In order to reproduce it you can do the following:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Then you can open the jupyter notebook or vs code and navigate to the `exploratory_analysis` folder.

## CDI Bonus Data Product

### Prerequisites
- Docker and Docker Compose installed.
- Parquet files in `data/input/` (CDC data).

### Setup and Run
0. **Optional: For fresh start**:
   - Run the following command for a fresh start:
    ```bash
    docker-compose up --build
    ```
1. **Create Minio data folders**:
   - Create the required folders 
    ```bash
    mkdir -p minio-data/input minio-data/output
    ```
1. **Prepare Parquet Files**:
   - Place your CDC Parquet files in `data/input/`.
2. **Run pipeline**:
    ```bash
    docker-compose up --build
    ```
3. **Checking outputs**:
    - Logs: logs/pipeline.log.
    - Results: MinIO at http://localhost:9001 (user: admin, password: password
        - Wallet history: s3://output/wallet_history.
        - Transactions: s3://output/transactions.
    - Spark UI: http://localhost:4040.
4. **Stop application**:
    ```bash
    docker-compose down
    ```
    or reset minio data
    ```bash
    docker-compose down -v
    ```