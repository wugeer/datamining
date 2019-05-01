# turnover_feat
```
CREATE OR REPLACE FUNCTION rst.p_rst_turnover_feat(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN
	
perform rst.p_rst_turnover_feat_avg($1);
perform rst.p_rst_turnover_feat_update($1);
perform rst.p_rst_turnover_feat_update(($1+'1 day'::interval)::date);
perform rst.p_rst_turnover_feat_update(($1+'2 day'::interval)::date);
perform rst.p_rst_turnover_feat_update(($1+'3 day'::interval)::date);
perform rst.p_rst_turnover_feat_update(($1+'4 day'::interval)::date);
perform rst.p_rst_turnover_feat_update(($1+'5 day'::interval)::date);
perform rst.p_rst_turnover_feat_update(($1+'6 day'::interval)::date);
perform rst.p_rst_turnover_feat_update(($1+'7 day'::interval)::date);
	RETURN 1;
END

$function$
;

```