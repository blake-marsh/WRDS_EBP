/* This program estimates the option-adjusted excess bond premium (EBP)
   using SPR measure of corporate credit spreads for the nonfinancial
   firms. The EBP in period t is based on predicted spreads from the
   following reduced-form bond pricing model:
      log(SPR[k,t]) = (1 + CALL[k])*[a0 + a1*DD[i,t] + 
      		      	             a2*log(DUR[k,t]) + a3*log(PAR[k,t]) +
                                     a4*log(COUP[k]) + a5*log(AGE[k,t])] + 
                      CALL[k]*[b1*LEV[t] + b2*SLP[t] + b3*CRV[t] +
                               b4*VOL[t]] + 
                               c4*RTG[k,t] + c5*IND[i] + u[k,t],
   where
       SPR[k,t] = credit spread on bond k in month t (issued by firm i) 
        CALL[k] = callable indicator for bond k:
                     0 = non-callable 
                     1 = callable (i.e., redeemable)
        DD[i,t] = distance-to-default for firm i in month t, computed from
                  the Merton model
         LEV[t] = level factor of the Treasury yield curve in month t
         SLP[t] = slope factor of the Treasury yield curve in month t
         CRV[t] = curvature factor of the Treasury yield curve in month t
         VOL[t] = a measure of interest rate volatility in month t
       DUR[k,t] = duration of bond k in month t
       PAR[k,t] = (nominal) par value of the bond issue k in month t
        COUP[k] = (fixed) coupon rate of the bond issue k
       AGE[k,t] = age (in days) of the bond issue k in month t
       RTG[k,t] = credit rating fixed effect of the bond issue k in month t
         IND[i] = firm i's fixed industry (INDCODE) effect
   NOTE:
   (1) Estimation method: OLS
   (2) Sample period: Jan1973--Sep2010.
*/


   /* Sample period: */
   %let sdate = '01jan1973'd;
   %let edate = '01sep2010'd;

   /* Interest rate volatility factor: */
   %let vol_fct = vol_fct;

   /* Data setup:
      NOTE: The BONDYLD_M data set is proprietary. */
   data bondyld_m;
   set ebp.bondyld_m;
       lspr = log(spr);
       lspr_oa = log(spr_oa);
       lduration = log(duration);
       lparvalue = log(parvalue);
       lcoupon = log(coupon);
       lage = log(age);
       dd_merton_2 = dd_merton**2;
       if &sdate <= date <= &edate;
   run;


   /* Merge-in the estimated Treasury yield curve factors and
      realized volatilility of the 10-year Treasury yield: */
   data opt_fct_m;
   merge ebp.lsc_fct_m_eop(in=aa)
         ebp.vol_fct_m(in=bb);
   by date;
   if aa=1 and bb=1;
      _mrg = 1;
   run;
      
   proc sort data=bondyld_m;
   by date;
   run;

   data bondyld_m;
   merge bondyld_m(in=aa) opt_fct_m(in=bb);
   by date;
   if aa=1 and bb=1;
   run;

   proc sort data=bondyld_m;
   by cusip date;
   run;

   data bondyld_m;
   set bondyld_m;
       if call = 0 then
          do;
          call_dd_merton = 0;
          call_dd_merton_2 = 0;
          call_lduration = 0;
          call_lparvalue = 0;
          call_lcoupon = 0;
          call_lage = 0;
          call_lev_fct = 0;
          call_slp_fct = 0;
          call_crv_fct = 0;
          call_vol_fct = 0;
          end;
       if call = 1 then
          do;
          call_dd_merton = dd_merton;
          call_dd_merton_2 = dd_merton_2;
          call_lduration = lduration;
          call_lparvalue = lparvalue;
          call_lcoupon = lcoupon;
          call_lage = lage;
          call_lev_fct = lev_fct;
          call_slp_fct = slp_fct;
          call_crv_fct = crv_fct;
          call_vol_fct = &vol_fct;
          end;
   run;


   /* Estimation: */

   %let call_opt = call call_dd_merton
                   call_lduration call_lparvalue call_lcoupon call_lage
                   call_lev_fct call_slp_fct call_crv_fct call_vol_fct;
   
   proc glm data=bondyld_m;
   class sp_rating indcode;
   model lspr = dd_merton lduration lparvalue lcoupon lage
                &call_opt sp_rating indcode / solution;
   output out=_rds p=lspr_p r=lspr_r;
   ods output ParameterEstimates=ParameterEstimates;
   ods output FitStatistics=FitStatistics;
   run;
   quit;

   data b_call(keep=_mrg Estimate rename=(Estimate=b_call))
        b_call_dd(keep=_mrg Estimate rename=(Estimate=b_call_dd))
        b_call_ldur(keep=_mrg Estimate rename=(Estimate=b_call_ldur))
        b_call_lpar(keep=_mrg Estimate rename=(Estimate=b_call_lpar))
        b_call_lcoup(keep=_mrg Estimate rename=(Estimate=b_call_lcoup))
        b_call_lage(keep=_mrg Estimate rename=(Estimate=b_call_lage))
        b_call_lev(keep=_mrg Estimate rename=(Estimate=b_call_lev))
        b_call_slp(keep=_mrg Estimate rename=(Estimate=b_call_slp))
        b_call_crv(keep=_mrg Estimate rename=(Estimate=b_call_crv))
        b_call_vol(keep=_mrg Estimate rename=(Estimate=b_call_vol));
   set ParameterEstimates;
       _mrg = 1;
       if Parameter = "call" then output b_call;
       if Parameter = "call_dd_merton" then output b_call_dd;
       if Parameter = "call_lduration" then output b_call_ldur;
       if Parameter = "call_lparvalue" then output b_call_lpar;
       if Parameter = "call_lcoupon" then output b_call_lcoup;
       if Parameter = "call_lage" then output b_call_lage;
       if Parameter = "call_lev_fct" then output b_call_lev;
       if Parameter = "call_slp_fct" then output b_call_slp;
       if Parameter = "call_crv_fct" then output b_call_crv;
       if Parameter = "call_vol_fct" then output b_call_vol;
   run;

   data ParameterEstimates;
   merge b_call(in=a)
         b_call_dd(in=b)
         b_call_ldur(in=c)
         b_call_lpar(in=d)
         b_call_lcoup(in=e)
         b_call_lage(in=f)
         b_call_lev(in=g)
         b_call_slp(in=h)
         b_call_crv(in=i)
         b_call_vol(in=j);
   by _mrg;
   if a=1 and b=1 and c=1 and d=1 and e=1 and
      f=1 and g=1 and h=1 and i=1 and j=1;
   run;

   data _rds;
   merge _rds(in=aa) ParameterEstimates(in=bb);
   by _mrg;
   if aa=1 and bb=1;
      if call = 0 then
         do;
         call_adj = 0;
         spr_oa = spr;
         lspr_oa_p = lspr_p;
         end;
      if call = 1 then
         do;
         call_adj = b_call + b_call_dd*dd_merton + 
                           + b_call_ldur*lduration + b_call_lpar*lparvalue
                           + b_call_lcoup*lcoupon + b_call_lage*lage
                           + b_call_lev*lev_fct + b_call_slp*slp_fct
                           + b_call_crv*crv_fct + b_call_vol*&vol_fct;
         spr_oa = exp(lspr - call_adj);
         lspr_oa_p = lspr_p - call_adj;
         end;
   run;

   data _rds;
   set _rds;
       if lspr_r = . or lspr_p = . then delete;
   run;

   
   /* Compute the predicted level of credit spreads: */
   data sigmads(keep=_mrg sig_2);
   set FitStatistics;
       _mrg = 1;
       sig_2 = (RootMSE)**2;
   run;

   data _rds;
   merge _rds(in=aa) sigmads(in=bb);
   by _mrg;
   if aa=1 and bb=1;
      /* Predicted level of raw credit spreads: */
      spr_p = exp(lspr_p + 0.5*sig_2);
      /* Residual level of raw credit spreads: */
      spr_r = spr - spr_p;
   run; 

   proc sort data=_rds;
   by date;
   run;

   
   /* Compute the necessary cross-sectional moments: */

   %let csvars = spr spr_p spr_r spr_oa lspr lspr_p lspr_r lspr_oa lspr_oa_p;
   
   proc univariate data=_rds noprint;
   by date;
   var &csvars;
   output out=_ebp_m mean=&csvars
                     std=%varsfx(&csvars, _std)
                     nobs=nbonds;
   run;

   
   %let mvars = spr spr_p spr_oa spr_oa_p ebp_oa
                lspr lspr_p lspr_oa lspr_oa_p lebp_oa
                spr_std lspr_std spr_r_std lspr_r_std;
   
   data _ebp_m(keep=date year qtr nbonds &mvars);
   format date monyy7.;
   format nbonds 5.;
   format &mvars 8.4;
   set _ebp_m;
       /* Calculate the EBP: */
       ebp_oa = spr - spr_p;
       lebp_oa = lspr - lspr_p;
       spr_oa_p = spr_oa - ebp_oa;
       year = year(date);
       qtr = qtr(date);
   run;
    
   
   /* Monthly frequency: */       
   data ebp.ebp_oa_m(keep=date &mvars);
   format date monyy7.;
   format &mvars 8.4;
   set _ebp_m;
   run;

   
   /* Quarterly (average) frequency: */
   %let qvars_avg = %varsfx(&mvars, _avg);
       
   proc univariate data=_ebp_m noprint;
   by year qtr;
   var &mvars;
   output out=_ebp_q_avg mean=&qvars_avg;
   run;


   data _ebp_q_avg(keep=date &qvars_avg);
   format date yyq6.;
   format &qvars_avg 8.4;
   set _ebp_q_avg;
       date = yyq(year, qtr);
   run;

   
   /* Quarterly (end-of period) frequency: */
   %let qvars_eop = %varsfx(&mvars, _eop);
       
   data _ebp_q_eop(keep=date &qvars_eop);
   format date yyq6.;
   format &qvars_eop 8.4;
   set _ebp_m(rename=(%renamer1(&mvars, &qvars_eop)));
       if month(date) in (3, 6, 9, 12);
       date = yyq(year, qtr);
   run;

   
   /* Combine quarterly estimates of the EBP: */
   data ebp.ebp_oa_q;
   merge _ebp_q_avg(in=a) _ebp_q_eop(in=b);
   by date;
   if a=1 and b=1;
   run;   
