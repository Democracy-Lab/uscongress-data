#!/bin/bash
#SBATCH --job-name=CREC_scraper
#SBATCH -c 16                      # for parallel processing         
#SBATCH --mem-per-cpu=2G           # for parallel processing

# Configuration (for parallel processing)
WORKERS="${SLURM_CPUS_PER_TASK}"
export OMP_NUM_THREADS=1 # Prevent Python parallel libraries from oversubscribing

# Comma-separated list of one or more GovInfo API keys (register here: www.govinfo.gov/api-signup)
API_KEYS="DEMO_KEY1,DEMO_KEY2"

 # 35 years will safely capture all content, going back from the present
python CREC_scraper.py "CREC_output_folder" \
  --years 35 \
  --workers "$WORKERS" \
  --api-keys "$API_KEYS" \
  --parallel
