/* This program computes the parameter estimates underlying the construction
   of the excess bond premium (EBP) that uses monthly data on credit spreads
   and the distance-to-default (DD) based on the Merton model.
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
	capture log using "~m1exz00/stata/ebp/progs/Full-Sample/EBP-DD-Merton.log", replace;

	/* Read-in the data:
           NOTE: The BONDYLD_M data set is proprietary. */
	use "~m1exz00/stata/ebp/data/bondyld_m.dta", clear;

        mata: mata set matafavor speed;


	/* Summary statistics: */
        qui: regress lspr dd_merton if date >= ym(1973, 1);
        sum spr dd_merton if e(sample) == 1;



	/* Specification 1:
	   - no fixed effects */
        qui xi: reg lspr dd_merton lduration lparvalue lcoupon lage
                    i.call if date >= ym(1973, 1);

        /* Fit statistics: */
        fitstat;
  
        xi: cgmreg lspr dd_merton lduration lparvalue lcoupon lage
                   i.call if date >= ym(1973, 1), cluster(gvkey date);


        /* Compute the semi-elasticity (dy/dx) of spreads with respect to DD: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        nlcom(_b[dd_merton]*$spr_avg); 



	/* Specification 2:
           - credit ratings fixed effects */
        qui xi: reg lspr dd_merton lduration lparvalue lcoupon lage
                    i.call i.sp_rating if date >= ym(1973, 1);

        /* Fit statistics: */
        fitstat;
  
        xi: cgmreg lspr dd_merton lduration lparvalue lcoupon lage
                   i.call i.sp_rating if date >= ym(1973, 1),
                   cluster(gvkey date);

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to DD: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        nlcom(_b[dd_merton]*$spr_avg); 

        /* Test the joint significance of credit rating fixed effects: */
        testparm _Isp*;



	/* Specification 3:
           - credit ratings fixed effects
	   - industry fixed effects */
        qui xi: reg lspr dd_merton lduration lparvalue lcoupon lage
                    i.call i.sp_rating i.indcode if date >= ym(1973, 1);

        /* Fit statistics: */
        fitstat;
 
        xi: cgmreg lspr dd_merton lduration lparvalue lcoupon lage
                   i.call i.sp_rating i.indcode if date >= ym(1973, 1),
                   cluster(gvkey date);

        /* Compute the semi-elasticity (dy/dx) of spreads with respect to DD: */
        quietly sum spr if e(sample) == 1, meanonly;
        global spr_avg = r(mean);
        nlcom(_b[dd_merton]*$spr_avg); 

        /* Test the joint significance of credit rating fixed effects: */
        testparm _Isp_*;

        /* Test the joint significance of industry fixed effects: */
        testparm _Iind*;



