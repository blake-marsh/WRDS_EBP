#!/bin/bash
#$ -cwd

#$ -N ebp
#$ -pe onenode 8
#$ -l m_mem_free=6G

sas run_merton_DD.sas
