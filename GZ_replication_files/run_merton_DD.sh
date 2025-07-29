#!/bin/bash
#$ -cwd

#$ -N run_merton_DD
#$ -pe onenode 8
#$ -l m_mem_free=6G

sas -set year 1988 -set month 04 run_merton_DD.sas 
