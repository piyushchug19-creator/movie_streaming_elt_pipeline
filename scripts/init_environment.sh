#!/usr/bin/env bash
set -e

# ==============================================================================
# Movie Streaming ELT Pipeline - Environment Initialization Script
# ==============================================================================

echo "================================================================="
echo "🎬 Initializing Movie Streaming ELT Pipeline Environment"
echo "================================================================="

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# 1. Check or initialize .env file
if [ ! -f ".env" ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo "Created .env. Please update it with your GCP and API credentials."
else
    echo "Found existing .env file."
fi

# 2. Ensure Airflow directories exist with appropriate permissions
echo "Creating Airflow logs and plugins directories..."
mkdir -p airflow/logs airflow/plugins

# 3. Detect Airflow UID for Linux Docker permission handling
AIRFLOW_UID=$(id -u 2>/dev/null || echo 50000)
if ! grep -q "^AIRFLOW_UID=" .env; then
    echo "AIRFLOW_UID=${AIRFLOW_UID}" >> .env
fi

# 4. Check for Google Service Account Key
if [ -f "service_account.json" ]; then
    echo "Found service_account.json in project root."
else
    echo "Notice: service_account.json not found in project root."
    echo "Ensure your service account key is saved as service_account.json or set GOOGLE_APPLICATION_CREDENTIALS in .env."
fi

# 5. Check prerequisites
echo "Checking local toolchains..."
command -v docker >/dev/null 2>&1 && echo " Docker is installed: $(docker --version)" || echo "⚠️ Docker not found"
command -v docker-compose >/dev/null 2>&1 || docker compose version >/dev/null 2>&1 && echo " Docker Compose is available" || echo "⚠️ Docker Compose not found"
command -v python3 >/dev/null 2>&1 && echo " Python 3 is installed: $(python3 --version)" || echo "⚠️ Python 3 not found"

echo ""
echo "================================================================="
echo "Initialization Complete!"
echo "Next steps:"
echo " 1. Configure .env with your GCP project ID and API keys."
echo " 2. (Optional) Run python3 scripts/load_sample_data.py to seed BigQuery."
echo " 3. Start Airflow: docker compose up -d"
echo " 4. Launch Conversational BI: streamlit run bi_app/app.py"
echo "================================================================="

