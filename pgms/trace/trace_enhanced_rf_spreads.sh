#!/bin/bash

#$ -cwd

#$ -N trace_enhanced
#$ -pe onenode 1
#$ -l m_mem_free=48G
#$ -o $HOME/WRDS_EBP/logfiles/trace_enhanced.o.log
#$ -e $HOME/WRDS_EBP/logfiles/trace_enhanced.e.log


cd ~/WRDS_EBP/

#---------------------------------------------------------------------
# download Treasury rates from the Federal Reserve's H.15 release
# proxy server isn't activated on qsub jobs so run from command line
# before submission
#---------------------------------------------------------------------
#source ~/virtualenvs/WRDS_EBP/bin/activate
#python ./pgms/trace/download_h15.py
#deactivate

#-----------------------
# process treasury data
#------------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/format_h15.R ./logfiles/format_h15.Rout

#-----------------------------
# get trace data and process
#------------------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_query.R ./logfiles/trace_enhanced_query.Rout
R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_debt_types.R ./logfiles/trace_enhanced_debt_types.Rout

#----------------------------------------
# bond characteristics from FISD mergent
#----------------------------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_with_fisd_characteristics.R ./logfiles/trace_enhanced_with_fisd_characteristics.Rout

#----------------------------
# determine the trace sample
#----------------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_sample.R ./logfiles/trace_enhanced_sample.Rout

#----------------------------------------------------------
# calculate the risk-free spreads off synthetic Treasuries
#----------------------------------------------------------
R CMD BATCH --no-save --no-restore ./pgms/trace/trace_enhanced_rf_spreads.R ./logfiles/trace_enhanced_rf_spreads.Rout




