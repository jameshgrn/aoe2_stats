#!/bin/bash

#SBATCH -J aoe2_data_processing
#SBATCH -p general
#SBATCH -o aoe2_data_processing_%j.txt
#SBATCH -e aoe2_data_processing_%j.err
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jhgearon@iu.edu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=01-02:00:00
#SBATCH --mem=2G
#SBATCH -A r00268


cd aoe2_stats/aoe2_stats
#Load any modules that your program needs
module load python

pip -U install duckdb pandas 
#Run your program
python eda.py
