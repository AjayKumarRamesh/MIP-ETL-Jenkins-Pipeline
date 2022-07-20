MERGE INTO MAP_CORE.MCT_ISC_CLIENT_INTERESTS mic
USING ( VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,))
AS SOURCE (MIP_ACTV_SEQ_ID,
           ID,
           LEAD_SCORE,
           HRM,
           LEAD_STATUS,
           LEADSOURCE,
           CREATEDDATE,
           MIP_CI_TS,
           CI_NAME,
           LEAD,
           ISDELETED,
           ISCONVERTED,
           ACCOUNT_NAME,
           CONTACT)
ON mic.MIP_ACTIVITY_SEQ_ID = SOURCE.MIP_ACTV_SEQ_ID
WHEN MATCHED THEN UPDATE SET
mic.CI_ID = SOURCE.ID
mic.LEAD_SCORE = SOURCE.LEAD_SCORE
mic.HRM = SOURCE.HRM
mic.LEAD_STATUS = SOURCE.LEAD_STATUS
mic.LEADSOURCE = SOURCE.LEADSOURCE
mic.CREATEDDATE = SOURCE.CREATEDDATE
mic.MIP_CI_TS = SOURCE.MIP_CI_TS
mic.CI_NAME = SOURCE.CI_NAME
mic.LEAD = SOURCE.LEAD
mic.ISDELETED = SOURCE.ISDELETED
mic.ISCONVERTED = SOURCE.ISCONVERTED
mic.ACCOUNT_NAME = SOURCE.ACCOUNT_NAME
mic.CONTACT = SOURCE.CONTACT
WHEN NOT MATCHED THEN INSERT (MIP_ACTIVITY_SEQ_ID,
                              CI_ID,
                              LEAD_SCORE,
                              HRM,
                              LEAD_STATUS,
                              LEADSOURCE,
                              CREATEDDATE,
                              MIP_CI_TS,
                              CI_NAME,
                              LEAD,
                              ISDELETED,
                              ISCONVERTED,
                              ACCOUNT_NAME,
                              CONTACT)
VALUES ( SOURCE.MIP_ACTV_SEQ_ID,
         SOURCE.ID,
         SOURCE.LEAD_SCORE,
         SOURCE.HRM,
         SOURCE.LEAD_STATUS,
         SOURCE.LEADSOURCE,
         SOURCE.CREATEDDATE,
         SOURCE.MIP_CI_TS,
         SOURCE.CI_NAME,
         SOURCE.LEAD,
         SOURCE.ISDELETED,
         SOURCE.ISCONVERTED,
         SOURCE.ACCOUNT_NAME,
         SOURCE.CONTACT)