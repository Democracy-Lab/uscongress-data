#!/bin/bash
#SBATCH --job-name=CRECB_scraper
#SBATCH -c 16                      # for parallel processing         
#SBATCH --mem-per-cpu=2G           # for parallel processing

# Configuration (for parallel processing)
WORKERS="${SLURM_CPUS_PER_TASK}"
export OMP_NUM_THREADS=1 # Prevent Python parallel libraries from oversubscribing

# Comma-separated list of one or more GovInfo API keys (register here: www.govinfo.gov/api-signup)
API_KEYS="DEMO_KEY1,DEMO_KEY2"

# CRECB_scraper.py will start at 1873 and scrape all years by default
python CRECB_scraper.py "CRECB_output_folder" \
  --workers "$WORKERS" \
  --api-keys "$API_KEYS" \
  --parallel
