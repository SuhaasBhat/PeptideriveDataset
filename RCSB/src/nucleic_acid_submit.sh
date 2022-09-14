#!/bin/bash
#SBATCH -o ../slurm_out/slurm_%A_NARUN_supp.out
#SBATCH -e ../slurm_out/slurm_%A_NARUN_supp.err
#SBATCH -c 4
#SBATCH --mem=40G

source /hpc/group/chatterjee/suhaas/miniconda3/etc/profile.d/conda.sh
conda activate pdb-api
#python -u get_assembly_ids.py
#python -u assembly_ids_to_interface_ids.py
#python -u NUCLEIC_ACID_interface_ids_to_interface_dicts.py --output_dir dna_binding_run_1
#python -u filter_to_dataframe.py --output_dir dna_binding_run_1 --binding_area_cluster_radius 1 #basically not clustering here
python -u supplementary_data.py --output_dir dna_binding_run_1
