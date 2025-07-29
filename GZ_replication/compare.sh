#!/bin/bash
#$ -cwd

#$ -N compare
#$ -pe onenode 2
#$ -l m_mem_free=2G

YEAR=1988
MONTH=4

sas -set year "$YEAR" -set month "$MONTH" run_merton_DD.sas
python3 compare.py "$YEAR" "$MONTH"
