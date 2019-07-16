-- !preview conn=con

SELECT
  *
FROM
  hvlt AS h
FULL OUTER JOIN
  cowa_cfl AS c ON 
    h.ptid      = c.ptid AND
    h.form_date = c.form_date
FULL OUTER JOIN
  emory_wcst AS e ON
    h.ptid      = e.ptid AND
    h.form_date = e.form_date
FULL OUTER JOIN
  jolo AS j ON
    h.ptid      = j.ptid AND
    h.form_date = j.form_date
FULL OUTER JOIN
  wtar as w ON
    h.ptid      = w.ptid AND
    h.form_date = w.form_date
WHERE
  h.ptid      >= 'UM00000543'::text AND 
  h.form_date >= '2017-03-15'::date
ORDER BY
  h.ptid, h.form_date;
