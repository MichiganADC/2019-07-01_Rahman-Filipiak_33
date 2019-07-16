-- !preview conn=con

-- Uses defined view `d1_ift_madcdx_reqs`, which returns all the dx fields 
-- that are required to derive the MADC Dx using the `derive_consensus_dx`
-- helper function from helpers.R
SELECT 
  *
FROM 
  d1_ift_madcdx_reqs -- view
WHERE
  ptid      >= 'UM00000543'::text AND 
  form_date >= '2017-03-15'::date
ORDER BY
  ptid, form_date;