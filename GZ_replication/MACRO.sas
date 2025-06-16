/**************************************************************************/
/* Purpose: General utility macros.                                       */
/*                                                                        */
/**************************************************************************/

   
***************************************************************************;
* Purpose: To append a suffix to a list of variables;

%macro varsfx(vars, sfx);

	%local ii;

	%if &sfx="" %then %str(&vars;);
	%else
	    %do ii=1 %to %nvar(&vars);
		%scan(&vars, &ii)&sfx
	    %end;
	  
%mend varsfx;


***************************************************************************;
/* Purpose: To compute the number of observations in a SAS data set,
	    specified by the argument inds. The number of observations 
	    is assigned to a global macro variable nobs. */

%macro numobs(inds);

	%global nobs;

	proc sql;
	create view _nobs as 
	select count(*) as count from &inds;
        
	data _null_;
	set _nobs;
	call symput('nobs', left(put(count,8.)));
	run;

	%dsrm(ds=_nobs, mt=view);

%mend numobs;


***************************************************************************;
/* Purpose: To create a macro variable of the form PREnum1 PRE(num1+1) ... 
	 PREnum2. */

%macro dset(pre, num1, num2);

	%local ii;

	%do ii=%eval(&num1) %to %eval(&num2);
	    %do;
	    %str(&pre.&ii)
	    %end;
	%end;

%mend dset;


***************************************************************************;
/* Purpose: To create a macro variable of the form b1*VAR1 + ... + bk*VARk.
	    Used in proc MODEL to create the RHS of a linear equation. */ 

%macro rhseq(vars, coeff=b);

	%local ii _var _parm;

	%do ii=1 %to %nvar(&vars);
	    %do;
	    %let _var=%scan(&vars, &ii);
	    %let _parm=&coeff&ii;
	    %let t&ii=&_parm*&_var;
	    %if &ii=1 %then %str(&&t&ii);
	    %else %str(+ &&t&ii);
	    %end;
	%end;

%mend rhseq;


***************************************************************************;
/* Purpose: To compute the number of elements that are listed in a 
	    macro variable vars. */

%macro nvar(vars);

	%local count word;
	%let count=1;

	%let word=%qscan(&vars, &count, %str( ));
	%do %while (&word ne);
	%let count=%eval(&count+1);
	%let word=%qscan(&vars, &count, %str( ));
	%end;
	%eval(&count-1)

%mend nvar;


***************************************************************************;
/* Purpose: To rename a list of variables specifed in vars to &pre1, 
	    &pre2, ... */

%macro renamer(vars, pre);

	%local ii;

	%do ii=1 %to %nvar(&vars);
	    %scan(&vars, &ii)=&pre.&ii
	%end;

%mend renamer;


***************************************************************************;
%macro dsrm(ds, lib=work, mt=data);

	proc datasets memtype=&mt nolist lib=&lib;
	delete &ds;
	run;
	quit;

%mend dsrm;

