SELECT
'url-' || DIM_PAGE_URL_ID AS DIM_PAGE_URL_ID,
PAGE_URL,
REG_PAGE_CAT_NAME,
REG_PAGE_CAT_CD,
PAGE_CTRY,
NLU_KEYWRD_VAL_1,
NLU_KEYWRD_VAL_2,
NLU_KEYWRD_VAL_3,
NLU_KEYWRD_VAL_4,
NLU_KEYWRD_VAL_5,
4 as PARTCOL
FROM
V2DIGT2.V_DIM_URL
WHERE REG_PAGE_CAT_CD IN ('PC090', 'PC030')