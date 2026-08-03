#!/usr/bin/env bash
# ============================================================
#  data/download_datasets.sh
#  Instructions and helper commands for downloading all
#  required datasets. Run on Cluster C master node.
# ============================================================

set -euo pipefail
source "$(dirname "$0")/../config/cluster.env"

echo "============================================================"
echo "  Smart Grid Analytics Platform — Dataset Setup"
echo "============================================================"
echo ""

mkdir -p "${LOCAL_DATA_DIR}"/{london,pjm,uci}

echo "---------------------------------------------------------------"
echo "DATASET 1: London Smart Meters (Kaggle)"
echo "---------------------------------------------------------------"
echo "  Size    : ~10 GB"
echo "  Records : 167 million rows"
echo ""
echo "  Option A — Kaggle CLI (recommended):"
echo "    pip install kaggle"
echo "    kaggle datasets download -d jeanmidev/smart-meters-in-london \\"
echo "      -p ${LOCAL_DATA_DIR}/london --unzip"
echo ""
echo "  Option B — Manual download:"
echo "    Visit: https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london"
echo "    Download and extract to: ${LOCAL_DATA_DIR}/london/"
echo ""
echo "  Expected files after extraction:"
echo "    ${LOCAL_DATA_DIR}/london/halfhourly_dataset/   (main folder, ~5500 CSV files)"
echo "    ${LOCAL_DATA_DIR}/london/informations_households.csv"
echo "    ${LOCAL_DATA_DIR}/london/weather_hourly_darksky.csv"
echo "    ${LOCAL_DATA_DIR}/london/acorn_details.csv"
echo ""

echo "---------------------------------------------------------------"
echo "DATASET 2: PJM Hourly Energy Demand (Kaggle)"
echo "---------------------------------------------------------------"
echo "  Size    : ~1 GB"
echo "  Records : ~145,000 rows (multi-zone)"
echo ""
echo "  Option A — Kaggle CLI:"
echo "    kaggle datasets download -d robikscube/hourly-energy-consumption \\"
echo "      -p ${LOCAL_DATA_DIR}/pjm --unzip"
echo ""
echo "  Option B — Manual:"
echo "    Visit: https://www.kaggle.com/datasets/robikscube/hourly-energy-consumption"
echo "    Extract to: ${LOCAL_DATA_DIR}/pjm/"
echo ""
echo "  Expected files:"
echo "    ${LOCAL_DATA_DIR}/pjm/AEP_hourly.csv"
echo "    ${LOCAL_DATA_DIR}/pjm/DOM_hourly.csv"
echo "    (and ~10 other zone files)"
echo ""

echo "---------------------------------------------------------------"
echo "DATASET 3: UCI Individual Household Power Consumption"
echo "---------------------------------------------------------------"
echo "  Size    : ~20 MB"
echo "  Records : ~2 million rows"
echo ""
echo "  Download automatically:"
if command -v wget &>/dev/null; then
    echo "  Downloading UCI dataset..."
    wget -q -P "${LOCAL_DATA_DIR}/uci" \
      "https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip"
    unzip -q "${LOCAL_DATA_DIR}/uci/household_power_consumption.zip" \
      -d "${LOCAL_DATA_DIR}/uci/"
    echo "  [OK] UCI dataset downloaded to ${LOCAL_DATA_DIR}/uci/"
else
    echo "  wget not available. Visit:"
    echo "  https://archive.ics.uci.edu/dataset/235"
    echo "  Extract to: ${LOCAL_DATA_DIR}/uci/"
fi

echo ""
echo "---------------------------------------------------------------"
echo "Uploading datasets to HDFS"
echo "---------------------------------------------------------------"

# ── Create HDFS directory structure ──────────────────────────
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /smart_grid/raw/london
hdfs dfs -mkdir -p /smart_grid/raw/pjm
hdfs dfs -mkdir -p /smart_grid/raw/uci
hdfs dfs -mkdir -p /smart_grid/clean/meter_readings
hdfs dfs -mkdir -p /smart_grid/models
hdfs dfs -mkdir -p /smart_grid/forecasts
hdfs dfs -mkdir -p /smart_grid/reports
echo "  [OK] HDFS directory structure created"

# ── Upload London dataset (household metadata only at this stage) ─
echo "Uploading household metadata..."
hdfs dfs -put -f \
  "${LOCAL_DATA_DIR}/london/informations_households.csv" \
  /smart_grid/raw/london/

# ── Upload PJM data ───────────────────────────────────────────
echo "Uploading PJM demand data..."
hdfs dfs -put -f "${LOCAL_DATA_DIR}/pjm/" /smart_grid/raw/
echo "  [OK] PJM data uploaded"

echo ""
echo "NOTE: London halfhourly_dataset will be uploaded by Pig ETL"
echo "      (too large to upload directly - Pig processes it in chunks)"
echo ""
echo "All done. Run 'pig -f pig/etl_clean.pig' next."
