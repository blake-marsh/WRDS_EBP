#!/bin/bash
#$ -cwd

#$ -N compare
#$ -pe onenode 8
#$ -l m_mem_free=6G

python3 compare.py
