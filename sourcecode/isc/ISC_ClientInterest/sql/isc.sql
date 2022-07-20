select integer(ci.SOURCEID) as MIP_ACTV_SEQ_ID
, ci.SOURCEID
, ci.id , ci.LEAD_SCORE
, case when ci.LEADSOURCE = 'LSCTUS' then 1
       else  0  end as HRM
       , ci.STATUS as LEAD_STATUS
       , ci.LEADSOURCE
, ci.CREATEDDATE
, ci.CI_TS as MIP_CI_TS
, ci.NAME  as CI_NAME
, ci.LEAD, ci.ISDELETED , ci.ISCONVERTED
, ci.ACCOUNT_NAME, ci.CONTACT,
4 as PARTCOL
from ISC.CLIENT_INTEREST_V ci
where ci.SOURCEID is not null
and ci.CREATEDDATE > ?