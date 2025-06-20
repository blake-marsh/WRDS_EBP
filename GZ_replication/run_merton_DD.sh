#!/bin/bash
#$ -cwd

#$ -N run_merton_DD
#$ -pe onenode 8
#$ -l m_mem_free=6G

sas run_merton_DD.sas
python3 compare.py
