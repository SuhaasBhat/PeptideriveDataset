#!/bin/bash
#SBATCH -o ../slurm_out/slurm_%A_run1_numchains2.out
#SBATCH -e ../slurm_out/slurm_%A_run1_numchains2.err
#SBATCH -c 4
#SBATCH --mem=40G

source /hpc/group/chatterjee/suhaas/miniconda3/etc/profile.d/conda.sh
conda activate pdb-api
#python -u get_assembly_ids.py
#python -u assembly_ids_to_interface_ids.py
#python -u interface_ids_to_interface_dicts.py
#python -u filter_to_dataframe.py --output_dir run_1/clean_interim_2
python -u supplementary_data.py --output_dir run_1/canonical_acids
#python -u get_num_chains_per_assembly.py --output_dir run_1
