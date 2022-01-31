----- Updated 4/2/2021 -- Use this to match CDS Exactly if CDS data is in a Table

 select --distinct causes an error when building the Category ID
     'url-' || x2.DIM_PAGE_URL_ID as "entity.id" 
   -- , '' as "entity.name"
    , dsr.TITLE as "entity.name" 
    --, cds.TITLE 
     
    -- testing ,  cds.ut10, cds.ut15,cds.ut17,cds.ut20,cds.ut30
       
------ Updated 4/02 ----------from CDS only---------------------------  
    , case when cds.ut15 is not null and cds.ut17 is null then
           '"' || cds.ut10 || '","' || cds.ut10 || ':' || cds.ut15             
       when cds.ut15 is not null and  cds.ut20 is null then          
            '"' || cds.ut10 || '","' || cds.ut10 || ':' || cds.ut15  ||    
             '","' || cds.ut10 || ':' || cds.ut15  || ':' || cds.ut17    
       when cds.ut15 is not null and  cds.ut30 is null then
            '"' || cds.ut10 || '","' || cds.ut10 || ':' || cds.ut15  ||    
            '","' || cds.ut10 || ':' || cds.ut15  || ':' || cds.ut17  ||   
             '","' || cds.ut10 || ':' || cds.ut15  || ':' || cds.ut17  || ':' ||
                     cds.ut20  --|| '"""'                              
       when cds.ut15 is not null and cds.ut30 is not null then       
            '"' || cds.ut10 || '","' || cds.ut10 || ':' || cds.ut15  ||    
            '","' || cds.ut10 || ':' || cds.ut15  || ':' || cds.ut17  ||   
            '","' || cds.ut10 || ':' || cds.ut15  || ':' || cds.ut17  || ':' ||
                     cds.ut20  ||                
            '","'  || cds.ut10 || ':' || cds.ut15  || ':' || cds.ut17  || ':' ||
                     cds.ut20 || ':' || cds.ut30    
       when cds.ut10 is not null and  cds.ut15 is null then
              '"' || cds.ut10  
       else ''  end  -- as "entity.ut_codes"

||   ---- Next Check for UT descriptions in case Xref table is empty
   case when  ut.UT_LVL_10_DSCR is not null then  '","' 
          else '"'  end
 || 
    case when ut.UT_LVL_15_DSCR is not null and ut.UT_LVL_17_DSCR is null then
           ut.UT_LVL_10_DSCR || '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || '"'        
       when ut.UT_LVL_15_DSCR is not null and  ut.UT_LVL_20_DSCR is null then
            ut.UT_LVL_10_DSCR || '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  ||    
             '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || ':' || ut.UT_LVL_17_DSCR  || '"'              
       when ut.UT_LVL_15_DSCR is not null and  ut.UT_LVL_30_DSCR is null then
            ut.UT_LVL_10_DSCR || '"",""' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  ||    
            '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || ':' || ut.UT_LVL_17_DSCR  ||   
            '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || ':' || ut.UT_LVL_17_DSCR  || ':' ||
                     ut.UT_LVL_20_DSCR  || '"'         
       when ut.UT_LVL_15_DSCR is not null and ut.UT_LVL_30_DSCR is not null then
            ut.UT_LVL_10_DSCR || '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  ||    
            '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || ':' || ut.UT_LVL_17_DSCR  ||   
            '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || ':' || ut.UT_LVL_17_DSCR  || ':' ||
                    ut.UT_LVL_20_DSCR  ||                
            '","' || ut.UT_LVL_10_DSCR || ':' || ut.UT_LVL_15_DSCR  || ':' || ut.UT_LVL_17_DSCR  || ':' ||
                     ut.UT_LVL_20_DSCR || ':' || ut.UT_LVL_30_DSCR || '"'  
       when ut.UT_LVL_10_DSCR is not null and  ut.UT_LVL_15_DSCR is null then
              ut.UT_LVL_10_DSCR || '"' 
       else ''  end
 --)
 as "entity.category"
 --------------------------------------------------------------------------------   
    
 --- Concat description
  ,  coalesce(  ( dsr.DESCR0 || 
      case when dsr.DESCR5 is not null then
            ', ' || dsr.DESCR1 || ', ' || dsr.DESCR2 || ', ' || dsr.DESCR3 || ', ' || dsr.descr4
               ||  ', ' || dsr.DESCR5
        when dsr.DESCR4 is not null then          
          ', ' || dsr.DESCR1 || ', ' || dsr.DESCR2 || ', ' || dsr.DESCR3 || ', ' || dsr.descr4
        when dsr.DESCR3 is not null then  ', ' || dsr.DESCR1 || ', ' || dsr.DESCR2 || ', ' || dsr.DESCR3
        when dsr.DESCR2 is not null then  ', ' || dsr.DESCR1 || ', ' || dsr.DESCR2 
        when dsr.DESCR1 is not null then  ', ' || dsr.DESCR1  
        else ''  end  
        )
        , cds.MESSAGE ) as "entity.message"  -- page descriptionas "entity.message" 
        
    
    , coalesce(cds.Page_URL, x2.PAGE_URL ) as "entity.pageUrl"
    , '' as "entity.thumbnailUrl", '' as "entity.value", '' as "entity.inventory", '' as "entity.margin"  -- these 3 are blank in table

  -- Get the Category Name based on Category Code
    , case when cds.PAGECATCODE = 'PC090' then 'article/blog' -- Article Blogs
           when cds.PAGECATCODE = 'PC030' then 'product' -- Products
           else x2.REG_PAGE_CAT_NAME end  as "entity.common-type"
       
     ,cds.Content_ID  as  "entity.common-contentId" 
     , 'CDS' as "entity.common-source"   --, coalesce('x2.DATA_SRC_CD , A.DATA_SRC_CD )
     -- , cds.PAGECATCODE as "entity.common-page-categoryCode"
     , coalesce( cds.PAGECATCODE,x2.REG_PAGE_CAT_CD) as "entity.common-page-categoryCode"
   
     , cds.LANG_CD  as "entity.common-language"  -- use CDS lang
     
     , cds.lang_cd  -- language _ Country
       || '_' || 
      case when substr(x2.page_url,12,1) = '/' and substr(x2.page_url,18,1) = '/' and
              upper(substr(x2.page_url,13,2)) in ('AE', 'AF', 'AG', 'AI', 'AT', 'AU', 'AW', 'AZ', 'BB', 'BD', 'BE', 'BG', 'BH', 'BM', 'BN', 'BR', 'BS', 'BW', 'BZ', 'CA', 'CH', 'CM', 'CN', 'CR', 'CW', 'CY', 'CZ', 'DE', 'DK', 'DM', 'EE', 'EG', 'ES', 'ET', 'FI', 'FR', 'GB', 'GD', 'GH', 'GR', 'GY', 'HK', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IN', 'IQ', 'IT', 'JM', 'JO', 'KE', 'KH', 'KN', 'KW', 'KY', 'KZ', 'LB', 'LC', 'li', 'LK', 'LT', 'LU', 'LV', 'LY', 'MC', 'MP', 'MS', 'MU', 'MW', 'MX', 'MY', 'NA', 'NG', 'NL', 'NO', 'NP', 'NZ', 'OM', 'PH', 'PK', 'PL', 'PR', 'PT', 'QA', 'RO', 'RS', 'RU', 'SA', 'SC', 'SE', 'SG', 'SI', 'SK', 'SL', 'SR', 'SV', 'TC', 'TH', 'TR', 'TT', 'TZ', 'UA', 'UG', 'UK', 'UM', 'US', 'UZ', 'VC', 'VG', 'VN', 'WW', 'YE', 'ZA', 'ZM', 'ZW')
              then (substr(x2.page_url,13,2))
        else  lower(x2.PAGE_CTRY)    -- else A.LANG_CD || '_' || A.CTRY end
          end as "entity.common-locale"  -- en_BE 'uk' for GB (lang_cd + Ctry)
 
     , coalesce(cds.CNTRY, x2.PAGE_CTRY) as "entity.common-country"  
     , coalesce(cds.geo,g.IBM_GBL_IOT_CD)  as "entity.common-geo" 
    
-- Logic assumes keywords go in order, would not have a 1 and 3 without a 2.
-- Example: CSV Structure "[""AI platform"",""Business process"",""B2B gateway"",""Business intelligence""]"
---- updated 3/31 removed extra quotes -----------       
    
 , case when  x2.NLU_KEYWRD_VAL_2 is not null 
            and x2.NLU_KEYWRD_VAL_3 is null then
        '["' || x2.NLU_KEYWRD_VAL_1 || '","' ||  x2.NLU_KEYWRD_VAL_2
             || '"]' 
         when  x2.NLU_KEYWRD_VAL_2 is not null 
         and x2.NLU_KEYWRD_VAL_4 is null then
         '["' || x2.NLU_KEYWRD_VAL_1 || '","' ||  x2.NLU_KEYWRD_VAL_2 ||   
         '","' ||  x2.NLU_KEYWRD_VAL_3 
             || '"]'   
         when  x2.NLU_KEYWRD_VAL_2 is not null 
            and x2.NLU_KEYWRD_VAL_4 is not null then
          '["' || x2.NLU_KEYWRD_VAL_1 || '","' ||  x2.NLU_KEYWRD_VAL_2 || 
         '","' ||  x2.NLU_KEYWRD_VAL_3 
             || '","' ||  x2.NLU_KEYWRD_VAL_4 || '"]'  
         when  x2.NLU_KEYWRD_VAL_1 is not null 
            and x2.NLU_KEYWRD_VAL_2 is null then 
         '["' || x2.NLU_KEYWRD_VAL_1 || '"]' 
          else ''  end as "entity.common-topics"    --- update 2 
                
   -- , coalesce(x2.UT_LVL_10_CD, cds.UT10) as "entity.ut-level10code"
    
    ,cds.UT10 as "entity.ut-level10code"
    , coalesce(ut.UT_LVL_10_DSCR, ut.UT_LVL_10_DSCR) as "entity.ut-level10"
    
   -- , coalesce(x2.UT_LVL_15_CD, cds.UT15) as "entity.ut-level15code"
    , cds.UT15 as "entity.ut-level15code"
    , coalesce(ut.UT_LVL_15_DSCR, ut.UT_LVL_15_DSCR) as "entity.ut-level15"
    
    --, coalesce(x2.UT_LVL_17_CD, cds.UT17) as "entity.ut-level17code"
    , cds.UT17 as "entity.ut-level17code"
    , coalesce(ut.UT_LVL_17_DSCR, ut.UT_LVL_17_DSCR) as "entity.ut-level17"
    
    --, coalesce(x2.UT_LVL_20_CD, cds.UT20) as "entity.ut-level20code"
    , cds.UT20 as "entity.ut-level20code"
    , coalesce(ut.UT_LVL_20_DSCR, ut.UT_LVL_20_DSCR) as "entity.ut-level20"
    
    --, coalesce(x2.UT_LVL_30_CD, cds.ut30) as "entity.ut-level30code"
    , cds.ut30 as "entity.ut-level30code"
    , coalesce(ut.UT_LVL_30_DSCR, ut.UT_LVL_30_DSCR) as "entity.ut-level30"
   -- reformat time..
  , cds.PUBLISHEDTIME as "entity.content-publishedTime"
  , cds.MODIFIEDTIME as "entity.content-modifiedTime"
 --  , coalesce(cds.PUBLISHEDTIME, x2.CREATE_TS) as "entity.content-publishedTime"
 --  , coalesce(cds.MODIFIEDTIME  , x2.UPDT_TS) as "entity.content-modifiedTime"
   , coalesce(cds.authorName, x2.PAGE_OWNR_ID ) as "entity.content-authorName"
    
 from ACS_SBOX0.MIS_CDS_URLS cds
 left join V2DIGT2.V_DIM_URL x2 on cds.Page_URL = x2.PAGE_URL -- NLU Keywords Only
 left join V2REFR2.V_REF_UT_BRAND ut on ut.UT_CD =
                   coalesce( cds.UT30, cds.UT20, cds.UT17, cds.UT15, cds.UT10 )
 left join V2REFR2.V_REF_IBM_GBL_GEO g on g.CTRY = 
              coalesce(x2.PAGE_CTRY, cds.cntry)  and g.CTRY_SEQ = 1 
 left join  ACS_SBOX0.MIS_TITLE_URLs dsr on substr(dsr.URL,9,length(dsr.url) - 8) = cds.PAGE_URL  
         

--where cds.CONTENTID in ('00e14f870881e1bb','00e14f9dce04d8ec',
--'00e14f8a4601e8e3', '00e14f991f84fd4c' )
WITH UR FOR READ ONLY;             
