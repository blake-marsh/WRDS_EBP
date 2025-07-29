/* This program computes summary statistics for the selected variables
   in the BONDYLD_M data set.
   NOTE:
   (1) The BONDYLD_M data set is proprietary.     
   (2) Summary statistics are computed on trimmed data.
*/

   /* Set options: */
   options nodate nonumber;
   title1;
   title2;
   title3;


   data bondyld_m;
   set ebp.bondyld_m;
       if date >= '01jan1973'd;
   run;


   /* Summary statistics: */
   data bondyld_m;
   set ebp.bondyld_m;
       mty_issue = round(mty_issue);
   run;

   /* Create a numeric bond (CUSIP) identifier: */
   proc sort data=bondyld_m(keep=cusip) out=cusip nodupkey;
   by cusip;
   run;

   data cusip;
   set cusip;
       bond_id = _N_;
   run;

   proc sort data=bondyld_m;
   by cusip;
   run;

   data bondyld_m;
   merge cusip(in=aa) bondyld_m(in=bb);
   by cusip;
   if aa=1 and bb=1;
   run;
   
   proc sort data=bondyld_m;
   by cusip gvkey date;
   run;

   
   /* Compute the number of bonds outstanding per firm: */
   proc sort data=bondyld_m(keep=gvkey cusip) out=nbonds nodupkey;
   by gvkey cusip;
   run;

   data nbonds(keep=gvkey nbonds);
   set nbonds;
       by gvkey;
	  do;
	  if first.gvkey = 1 then nbonds = 1;
	  else nbonds = nbonds + 1;
	  if last.gvkey = 1;
	  end;
       retain nbonds;
   run;
   
   proc univariate data=nbonds noprint;
   var nbonds;
   output out=nbonds mean=mean
                     std=std
		     min=min
		     p10=p10
		     q1=p25
		     median=med
		     q3=p75
		     p90=p90
		     max=max;
   run;

   proc print data=nbonds noobs split="+";
   var mean std min p10 p25 med p75 p90 max;
   label mean="Mean";
   label std="StdDev";
   label min="Minimum";
   label p10="Pctl-10";
   label p25="Pctl-25";
   label med="Median";
   label p75="Pctl-75";
   label p90="Pctl-90";
   label max="Maximum";
   format mean std 6.2;
   format min p10 p25 med p75 p90 max 3.;
   title1 'Outstanding Bond Issues Across Firms';
   run;
   title1;


   /* Compute the number of bonds outstanding per firm/month: */
   proc sort data=bondyld_m(keep=gvkey date bond_id) out=nbonds nodupkey;
   by gvkey date bond_id;
   run;

   proc univariate data=nbonds noprint;
   by gvkey date;
   var bond_id;
   output out=tmp n=nbonds;
   run;

   proc univariate data=tmp noprint;
   var nbonds;
   output out=nbonds mean=mean
                     std=std
		     min=min
		     p10=p10
		     q1=p25
		     median=med
		     q3=p75
		     p90=p90
		     max=max;
   run;

   proc print data=nbonds noobs split="+";
   var mean std min p10 p25 med p75 p90 max;
   label mean="Mean";
   label std="StdDev";
   label min="Minimum";
   label p10="Pctl-10";
   label p25="Pctl-25";
   label med="Median";
   label p75="Pctl-75";
   label p90="Pctl-90";
   label max="Maximum";
   format mean std 6.2;
   format min p10 p25 med p75 p90 max 3.;
   title1 'Outstanding Bond Issues Per Firm/Month';
   run;
   title1;


   /* Compute the distribution maturities: */
   proc sort data=bondyld_m(keep=cusip mty_issue) out=mty_issue nodupkey;
   by cusip mty_issue;
   run;

   proc univariate data=mty_issue noprint;
   var mty_issue;
   output out=mty_issue mean=mean
                        std=std
		        min=min
		        p10=p10
		        q1=p25
		        median=med
		        q3=p75
		        p90=p90
		        max=max;
   run;

   proc print data=mty_issue noobs split="+";
   var mean std min p10 p25 med p75 p90 max;
   label mean="Mean";
   label std="StdDev";
   label min="Minimum";
   label p10="Pctl-10";
   label p25="Pctl-25";
   label med="Median";
   label p75="Pctl-75";
   label p90="Pctl-90";
   label max="Maximum";
   format mean std 6.2;
   format min p10 p25 med p75 p90 max 3.;
   title1 'Maturity of Outstanding Bond Issues';
   title2 '(years)';
   run;
   title1;
   title2;


   /* Compute the distribution coupon rates: */
   proc sort data=bondyld_m(keep=cusip coupon) out=coupon nodupkey;
   by cusip coupon;
   run;

   proc univariate data=coupon noprint;
   var coupon;
   output out=coupon mean=mean
                     std=std       
		     min=min
		     p10=p10
		     q1=p25
		     median=med
		     q3=p75
		     p90=p90
		     max=max;
   run;

   proc print data=coupon noobs split="+";
   var mean std min p10 p25 med p75 p90 max;
   label mean="Mean";
   label std="StdDev";
   label min="Minimum";
   label p10="Pctl-10";
   label p25="Pctl-25";
   label med="Median";
   label p75="Pctl-75";
   label p90="Pctl-90";
   label max="Maximum";
   format mean std min p10 p25 med p75 p90 max 6.2;
   title1 'Nominal Coupon Rates on Outstanding Bond Issues';
   title2 '(percent)';
   run;
   title1;
   title2;


   /* Compute the distribution of credit ratings: */
   proc univariate data=bondyld_m noprint;
   var sp_rating_num;
   output out=rating min=min
		     p10=p10
		     q1=p25
		     median=med
		     q3=p75
		     p90=p90
		     max=max;
   run;

   proc transpose data=rating out=rating;
   run;

   data rating(keep=statistic sp_rating_num);
   set rating;
       rename _NAME_ = statistic;
       rename Col1 = sp_rating_num;
   run;

   proc sort data=bondyld_m(keep=sp_rating_num sp_rating) out=sp_rating nodupkey;
   by sp_rating_num;
   run;

   proc sort data=rating;
   by sp_rating_num;
   run;

   data rating(keep=statistic sp_rating);
   merge rating(in=aa) sp_rating(in=bb);
   by sp_rating_num;
   if aa=1 and bb=1;
   run;
   
   proc transpose data=rating out=rating;
   var sp_rating;
   id statistic;
   run;

   proc print data=rating noobs split="+";
   var min p10 p25 med p75 p90 max;
   label min="Minimum";
   label p10="Pctl-10";
   label p25="Pctl-25";
   label med="Median";
   label p75="Pctl-75";
   label p90="Pctl-90";
   label max="Maximum";
   title1 'Composite Credit Ratings on Outstanding Bond Issues';
   title2 '(S&P scale)';
   run;
   title1;
   title2;


   /* Compute summary statistics for the key remaining variables: */
   %let sumvars = effyld spr spr_oa parvalue mktvalue_r mty duration convexity;
   
   proc univariate data=bondyld_m noprint;
   var &sumvars;
   output out=_sumds mean=%varsfx(&sumvars, _avg)
                     std=%varsfx(&sumvars, _std)
		     cv=%varsfx(&sumvars, _cv)
		     skewness=%varsfx(&sumvars, _skw)
		     kurtosis=%varsfx(&sumvars, _kurtosis)
		     min=%varsfx(&sumvars, _min)
		     p10=%varsfx(&sumvars, _p10)
		     q1=%varsfx(&sumvars, _p25)
		     median=%varsfx(&sumvars, _med)
		     q3=%varsfx(&sumvars, _p75)
		     p90=%varsfx(&sumvars, _p90)
		     max=%varsfx(&sumvars, _max);
   run;

   data _sumds;
   format %varsfx(&sumvars, _avg) 8.4;
   format %varsfx(&sumvars, _std) 8.4;
   format %varsfx(&sumvars, _cv) 8.4;
   format %varsfx(&sumvars, _skw) 8.4;
   format %varsfx(&sumvars, _kurtosis) 8.4;
   format %varsfx(&sumvars, _max) 8.4;
   format %varsfx(&sumvars, _p90) 8.4;
   format %varsfx(&sumvars, _p75) 8.4;
   format %varsfx(&sumvars, _med) 8.4;
   format %varsfx(&sumvars, _p25) 8.4;
   format %varsfx(&sumvars, _p10) 8.4;
   format %varsfx(&sumvars, _min) 8.4;
   set _sumds;
   run;
   
   proc iml;
   use _sumds;
   read all var _num_ into S;
   close _sumds;
   Smat = t(shape(S, 12, %nvar(&sumvars)));
   mean = Smat[ ,1];
   std = Smat[ ,2];
   cv = Smat[ ,3];
   skw = Smat[ ,4];
   kurtosis = Smat[ ,5];
   max = Smat[ ,6];
   p90 = Smat[ ,7];
   p75 = Smat[ ,8];
   med = Smat[ ,9];
   p25 = Smat[ ,10];
   p10 = Smat[ ,11];
   min = Smat[ ,12];
   varname = {%upcase(&sumvars)};
   create _sumds var{varname mean std cv skw kurtosis min p10 p25 med p75 p90 max};
   append;
   quit;  

   proc print data=_sumds noobs split="+";
   var Varname mean std cv skw kurtosis;
   label varname="Variable";
   label mean="Mean";
   label std="Standard+Deviation";
   label cv="CV";
   label skw="Skewness";
   label kurtosis="Excess+Kurtosis";
   format mean std cv skw kurtosis 8.4;
   title1 'Summary Statistics for Key Variables';
   title2 '(trimmed data)';
   run;
   title1;
   title2;

   proc print data=_sumds noobs split="+";
   var Varname min p10 p25 med p75 p90 max;
   label varname="Variable";
   label min="Minimum";
   label p10="Pctl-10";
   label p25="Pctl-25";
   label med="Median";
   label p75="Pctl-75";
   label p90="Pctl-90";
   label max="Maximum";
   format min p10 p25 med p75 p90 max 8.4;
   title1 'Distribution of Key Variables';
   title2 '(trimmed data)';
   run;
   title1;
   title2;
   

   /* Set options: */
   options date number;
   title1;
   title2;
   title3;
   
