#!/bin/bash
#$ -cwd

#$ -N wrds_ebp
#$ -pe onenode 6
#$ -l m_mem_free=6G

python3 wrds_ebp.py
