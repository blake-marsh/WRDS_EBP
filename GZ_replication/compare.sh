#!/bin/bash
#$ -cwd

#$ -N compare
#$ -pe onenode 8
#$ -l m_mem_free=6G

YEAR=1988
MONTH=04

sas -set year "$YEAR" -set month "$MONTH" run_merton_DD.sas
python3 compare.py "$YEAR" "$MONTH"
