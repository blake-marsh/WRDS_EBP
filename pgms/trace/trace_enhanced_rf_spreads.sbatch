#!/bin/bash

# Submit this to the scheduler by running this command below:
# sbatch trace_enhanced_rf_spreads.sbatch

##SBATCH -p compute  ## Select run partition (queue)

#SBATCH -t 10:00:00 ## Job maximum time limit

#SBATCH -N 1 ## Select number of nodes

#SBATCH -n 64 ## Select number of processors

#SBATCH --mem 500GB ## Select high memory node

#SBATCH --job-name="trace" ## give job a name when listed in squeue
#SBATCH --output=/home/kc/j1wbm01//WRDS_EBP/logfiles/trace.log ## SBATCH logfile name

##SBATCH --mail-user=blake.marsh@kc.frb.org ## user's email
##SBATCH --mail-type=END

#-------------
# system info
#-------------

## print the start time
echo -e "Job started at: $(date '+%A %m-%d-%Y %X') \n"

## load the environment variables
module load gcc
module load miniconda3

cd ~/WRDS_EBP/

#---------------------------------------------------------------------
# download Treasury rates from the Federal Reserve's H.15 release
# proxy server isn't activated on qsub jobs so run from command line
# before submission
#---------------------------------------------------------------------
source activate WRDS_EBP
python ./pgms/trace/download_h15.py
conda deactivate

source activate r_base

#-----------------------
# process treasury data
#-----------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/format_h15.R ./logfiles/format_h15.Rout

#-----------------------------
# get trace data and process
#------------------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_query.R ./logfiles/trace_enhanced_query.Rout
#R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_debt_types.R ./logfiles/trace_enhanced_debt_types.Rout

#----------------------------------------
# bond characteristics from FISD mergent
#----------------------------------------
#R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_with_fisd_characteristics.R ./logfiles/trace_enhanced_with_fisd_characteristics.Rout

#----------------------------
# determine the trace sample
#----------------------------
#R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_sample.R ./logfiles/trace_enhanced_sample.Rout

#----------------------------------------------------------
# calculate the risk-free spreads off synthetic Treasuries
#----------------------------------------------------------
#R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_rf_spreads.R ./logfiles/trace_enhanced_rf_spreads.Rout

deactivate


