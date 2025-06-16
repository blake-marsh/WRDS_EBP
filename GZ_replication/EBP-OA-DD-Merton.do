/* This program computes the parameter estimates underlying the construction
   of the option-adjusted external finance premium (EBP) that uses monthly data 
   on credit spreads and the distance-to-default (DD) based on the Merton model.
   NOTE:
   (1) Estimation method: OLS
   (2) Two-way clustered robust standard errors (GVKEY/DATE dimensions)
   (3) Sample period: Jan1973--Sep2010
 */



	/* Set options: */
        #delimit;
        set memory 750m;
        set matsize 10000;
        set maxvar 10000;
	set more off;
	set linesize 255;
	capture log close;

        /* Output file: */
	capture log using "~m1exz00/stata/ebp/progs/Full-Sample/EBP-OA-DD-Merton.log", replace;

	/* Read-in the data:
           NOTE: The BONDYLD_M data set is proprietary. */
	use "~m1exz00/stata/ebp/data/bondyld_m.dta", clear;

        mata: mata set matafavor speed;


	/* Summary statistics: */
        qui: regress lspr dd_merton if date >= ym(1973, 1);
        sum spr dd_merton if e(sample) == 1;


	/* Specification 1:
           - credit ratings fixed effects
	   - industry fixed effects */
        qui xi: reg lspr i.call*dd_merton
                    i.call*lduration i.call*lparvalue i.call*lcoupon
                    i.call*lage i.sp_rating i.indcode if date >= ym(1973, 1);

        /* Fit statistics: */
        fitstat;
  
        xi: cgmreg lspr i.call*dd_merton
                   i.call*lduration i.call*lparvalue i.call*lcoupon
                   i.call*lage i.sp_rating i.indcode if date >= ym(1973, 1),
                   cluster(gvkey date);

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to DD: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
	/* Non-callable bonds: */
        lincom(_b[dd_merton]*$spr_avg);
	/* Callable bonds: */
        lincom((_b[dd_merton] + _b[_IcalXdd_me_1])*$spr_avg);

        /* Test the joint significance of credit rating fixed effects: */
        testparm _Isp_*;

        /* Test the joint significance of industry fixed effects: */
        testparm _Iind*;



	/* Specification 2:
           - interest rate term structure terms 
           - credit ratings fixed effects
	   - industry fixed effects */
        qui xi: reg lspr i.call*dd_merton 
                    call_lev_fct call_slp_fct call_crv_fct call_vol_fct
                    i.call*lduration i.call*lparvalue i.call*lcoupon
                    i.call*lage i.sp_rating i.indcode if date >= ym(1973, 1);

        /* Fit statistics: */
        fitstat;
  
        xi: cgmreg lspr i.call*dd_merton
                   call_lev_fct call_slp_fct call_crv_fct call_vol_fct
                   i.call*lduration i.call*lparvalue i.call*lcoupon
                   i.call*lage i.sp_rating i.indcode if date >= ym(1973, 1),
                   cluster(gvkey date);

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to DD: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
	/* Non-callable bonds: */
        lincom(_b[dd_merton]*$spr_avg);
	/* Callable bonds: */
        lincom((_b[dd_merton] + _b[_IcalXdd_me_1])*$spr_avg);

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to LEV_FCT: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        /* Callable bonds: */
        nlcom(_b[call_lev_fct]*$spr_avg); 

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to SLP_FCT: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        /* Callable bonds: */
        nlcom(_b[call_slp_fct]*$spr_avg); 

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to CRV_FCT: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        /* Callable bonds: */
        nlcom(_b[call_crv_fct]*$spr_avg); 

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to VOL_FCT: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        /* Callable bonds: */
        nlcom(_b[call_vol_fct]*$spr_avg); 

        /* Test the joint significance of credit rating fixed effects: */
        testparm _Isp_*;

        /* Test the joint significance of industry fixed effects: */
        testparm _Iind*;
