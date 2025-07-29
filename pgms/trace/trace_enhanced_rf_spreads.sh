#!/bin/bash

#$ -cwd

#$ -N trace_enhanced
#$ -pe onenode 8
#$ -l m_mem_free=6G
#$ -o $HOME/WRDS_EBP/logfiles/trace_enhanced.o.log
#$ -e $HOME/WRDS_EBP/logfiles/trace_enhanced.e.log


cd ~/WRDS_EBP/pgms/trace/

# download Treasury rates from the Federal Reserve's H.15 release
source ~/virtualenv/base_python/bin/activatesource ~/virtualenv/base_python/bin/activate
python ./pgms/download_h15.py
R CMD BATCH --no-save --no-restore ./pgms/format_h15.R ./logfiles/format_h15.Rout
deactivate

## get trace data and process
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_query.R ./logfiles/trace_enhanced_query.Rout
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_debt_types.R ./logfiles/trace_enhanced_debt_types.Rout

## bond characteristics from FISD mergent
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_with_fisd_characteristics.R ./logfiles/trace_enhanced_with_fisd_characteristics.Rout

## determine the trace sample
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_sample.R ./logfiles/trace_enhanced_sample.Rout

## calculate the risk-free spreads off synthetic Treasuries
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_rf_spreads.R ./logfiles/trace_enhanced_rf_spreads.Rout




