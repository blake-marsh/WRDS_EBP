#!/bin/bash
#$ -cwd

#$ -N compare
#$ -pe onenode 8
#$ -l m_mem_free=6G

YEAR=1988
MONTH=4

python3 wrds_ebp.py "$YEAR" "$MONTH"
