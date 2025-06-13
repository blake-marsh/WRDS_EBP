#!/bin/bash
#$ -cwd

#$ -N merton
#$ -pe onenode 8
#$ -l m_mem_free=6G
source ~/virtualenv/base_python/bin/activate

python3 merton_data_wrds_two.py
deactivate

