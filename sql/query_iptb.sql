-- !preview conn=con

SELECT
  *
FROM
  ipad_toolbox
WHERE
  ptid      >= 'UM00000543'::text AND 
  form_date >= '2017-03-15'::date AND
  date IS NOT NULL
ORDER BY
  ptid, form_date;