Select cop.URN_IDM_COMP , cop.DOM_BUY_GRP, cop.SAP_CUST_NUM, cop.prmry_st_Prov_name
,cop.MAIN_DOM_CLIENT_ID , cop.MAIN_IND_CD, cop.src_UPDT_TS,
4 AS PARTCOL
from V2INAT2.V_COMP_ON_PAGE cop
WHERE cop.SAP_CUST_NUM is not null
AND nvl(cop.UPDT_TS, cop.CREATE_TS) >= ?