-- !preview conn=con

-- Uses defined view `c2_neuropsych`
SELECT
  *
FROM 
  c2_neuropsych
WHERE
  ptid      >= 'UM00000543'::text AND 
  form_date >= '2017-03-15'::date
ORDER BY
  ptid, form_date;