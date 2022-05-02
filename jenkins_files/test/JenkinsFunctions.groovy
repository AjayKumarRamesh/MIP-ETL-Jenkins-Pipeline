import groovy.transform.Field

// COS folder, Source Folder, cert files
dagstoCOS = [RUBY_TO_MIP:['Ruby', 'RubyToMIP', 'digikeystore.jks', 'javacerts.jks'],
            CDSExtract:['CDS', 'CDStoAdobe', 'digikeystore.jks', 'javacerts.jks'],
            CDStoMIP:['CDS', 'CDStoAdobe', 'digikeystore.jks', 'javacerts.jks'],
            CDStoMIP_FullRefresh:['CDS', 'CDStoAdobe', 'digikeystore.jks', 'javacerts.jks'],
            IWM:['IWM', 'MRS', 'digikeystore.jks', 'mrs_db2_cloud_stage.ks', 'mrs_db2_prod_cloud.ks', 'mrs_db2_test.ks'],
            BDS_GEO_HIER:['.', '.', 'digikeystore.jks', 'javacerts.jks'],
            MIP_SPSS_SCORING:['.', '.', 'digikeystore.jks', 'javacerts.jks'],
            ADHOC_MKTO_LEADXREF:['Adhoc_xref', '.', 'Universal-trustore.jks', 'digikeystore.jks', 'marketo_sftp_pem.pem', 'marketo_sftp_prod.pem'],
            GRP_EVENTS_IDM:['.', '.', 'digikeystore.jks', 'javacerts.jks'],
            CMDP_COP_to_MIP:['.', 'AccountIngestion', 'digikeystore.jks', 'javacerts.jks'],
            'MIP-MARKETO-INTERACTION':['.', 'MipToMarketo', 'digikeystore.jks', 'javacerts.jks'],
            MKTO_UNSUB_EMAIL_ACTIVITY:['.', '.', 'digikeystore.jks', 'javacerts.jks'],
            SPSS_SCORING:['.', '.', 'digikeystore.jks', 'javacerts.jks']]
// image, jar 
airflow = [RUBY_TO_MIP:['ruby_image', 'ruby_app_jar'],
           CDSExtract:['cds_image', 'cds_app_jar'],
           CDStoMIP:['cds_image', 'cds_app_jar'],
           CDStoMIP_FullRefresh:['cds_image', 'cds_app_jar'],
           IWM:['iwm_image', 'iwm_app_jar'],
           BDS_GEO_HIER:['bds_image','bds_app_jar'],
           MIP_SPSS_SCORING:['spss_image','spss_app_jar'],
           ADHOC_MKTO_LEADXREF:['adhoc_image','adhoc_app_jar'],
           GRP_EVENTS_IDM:['grp_image','grp_app_jar'],
           CMDP_COP_to_MIP:['cmdp_cop_image', 'cmdp_cop_jar'],
           'MIP-MARKETO-INTERACTION':['mip_mkto_image','mip_mkto_app_jar'],
           MKTO_UNSUB_EMAIL_ACTIVITY:['unsubemail_image','unsubemail_app_jar'],
           SPSS_SCORING:['spss_scoring_image', 'spss_scoring_app_jar']]
           
envNum = [dev:1,test:2,prod:3]

def getCOSObjects(String IBMCLOUD_CREDS, String IBMCLOUD_COS_CRN, 
                  String IBMCLOUD_COS_REGION, String IBMCLOUD_COS_BUCKET, String dag_ID) {
    //Prepare IBMCLOUD COS access
    sh "ibmcloud plugin install cloud-object-storage"
    sh "ibmcloud login --apikey ${IBMCLOUD_CREDS_PSW} -r us-south"
    sh "ibmcloud cos config region --region=${IBMCLOUD_COS_REGION}"
    sh "ibmcloud cos config crn --crn=${IBMCLOUD_COS_CRN}"

    //Download and Install Flare
    sh "ibmcloud cos object-get --bucket ${IBMCLOUD_COS_BUCKET} --key 'map_project_files/Flare-v2.1-Log4j2.jar' Flare-v2.1-Log4j2.jar"
    sh "ls -al"
    sh 'mvn install:install-file -Dfile=Flare-v2.1-Log4j2.jar -DgroupId=com.flare -DartifactId=base -Dversion=2.1-Log4j2 -Dpackaging=jar'

    sh "mvn clean compile package -f ${dagstoCOS[dag_ID][1]}/pom.xml"

    //Download Spark
    sh "ibmcloud cos object-get --bucket ${IBMCLOUD_COS_BUCKET} --key 'map_project_files/spark-3.0.1-bin-hadoop2.7.zip' spark-3.0.1-bin-hadoop2.7.zip"
    sh "unzip spark-3.0.1-bin-hadoop2.7.zip"

    //Create cert folder
    sh "mkdir spark-3.0.1-bin-hadoop2.7/cert"
    //Get certs
    for (int i = 2; i < dagstoCOS[dag_ID].size(); i++) {
        sh "ibmcloud cos object-get --bucket ${IBMCLOUD_COS_BUCKET} --key 'map_project_files/${dagstoCOS[dag_ID][0]}/cert/${dagstoCOS[dag_ID][i]}' spark-3.0.1-bin-hadoop2.7/cert/${dagstoCOS[dag_ID][i]}"
    } 

    sh 'ls -al spark-3.0.1-bin-hadoop2.7/cert/'

     //Copy db2jcc4.jar zip to spark jars folder
    sh "ibmcloud cos object-get --bucket ${IBMCLOUD_COS_BUCKET} --key 'map_project_files/db2jcc4.jar' ./db2jcc4.jar"
    //sh "unzip db2_db2driver_for_jdbc_sqlj.zip"
    sh "cp db2jcc4.jar spark-3.0.1-bin-hadoop2.7/jars"
    //Copy the Dockerfile to spark-3.0.1-bin-hadoop2.7
    sh 'cp Dockerfile spark-3.0.1-bin-hadoop2.7'
    //Copy Maven artifacts to Spark jars folder
    sh "cp -r ${dagstoCOS[dag_ID][1]}/target/. spark-3.0.1-bin-hadoop2.7/examples/jars"
}

def test() {
    sh "echo 'test'"
}

def cloudLogin(String credentials, String env, boolean install) {
    sh "ibmcloud config --check-version=false"
    if (install) {
        sh "ibmcloud plugin install container-service"
        sh "ibmcloud plugin install container-registry"
    }
    //sh "ibmcloud plugin install container-service"
    sh "ibmcloud login --apikey ${credentials} -r us-south"
    sh "ibmcloud ks cluster config --cluster map-dal10-16x64-0${envNum[env]}"
    sh "kubectl config current-context"
}

def checkDagStatus(String airflow_pod, String dag_ID, boolean canFail) {
    //while loop checking for status 
    //AIRFLOW_POD = sh(script:'kubectl get pods -n airflow --no-headers -o custom-columns=":metadata.name" | grep airflow-scheduler', returnStdout: true).trim()
    DAG_CURRENT_RUN = sh(script:"kubectl exec -n airflow ${airflow_pod} -- airflow dags list-runs -d ${dag_ID} --state running -o json", returnStdout: true).trim()
    DAG_STATUS = ""
    timeout(time: 2, unit: "HOURS") {
        def dag_run_object = readJSON text: DAG_CURRENT_RUN
        if (dag_run_object.size() > 0) {
            //def dag_run_object = readJSON text: DAG_CURRENT_RUN
            DAG_EXECUTION_DATE = dag_run_object[0]['execution_date']
            while (DAG_STATUS != "success") {
                DAG_STATUS = sh(script:"""kubectl exec -n airflow ${airflow_pod} -- airflow dags state ${dag_ID} ${DAG_EXECUTION_DATE} | egrep 'running|failed|success'""", returnStdout: true).trim()
                sh "echo 'Checking DAG status before pause: ${DAG_STATUS}'"
                if (DAG_STATUS == "failed" && !canFail) {
                    sh "exit 1"
                }
                sleep(10)
            }
            sh "echo 'DAG no longer in run state, continuing with deployment.'"
        } else {
            sh "echo 'No current ${dag_ID} DAGs found running.'"
        }
    }

}

def moveImage(String image, String source_env, String dest_env) {
    //move images from dev to test (or source to dest)
    sh "ibmcloud cr login"
    sh "docker pull us.icr.io/${source_env}-namespace/${image}"
    sh "docker tag us.icr.io/${source_env}-namespace/${image} us.icr.io/${dest_env}-namespace/${image}"
    sh "docker push us.icr.io/${dest_env}-namespace/${image}"
}

def getAirflowVars(String airflow_pod, String dag_ID) {

    image_ref = sh(script:"kubectl exec -n airflow ${airflow_pod} -- airflow variables get ${airflow[dag_ID][0]}", returnStdout: true).trim()
    jar_ref = sh(script:"kubectl exec -n airflow ${airflow_pod} -- airflow variables get ${airflow[dag_ID][1]}", returnStdout: true).trim()

    return [image_ref, jar_ref]
}

def setAirflowVars(String airflow_pod, String dag_ID, String image, String jar) {
    sh "kubectl exec -n airflow ${airflow_pod} -- airflow variables set ${airflow[dag_ID][0]} ${image}"
    sh "kubectl exec -n airflow ${airflow_pod} -- airflow variables set ${airflow[dag_ID][1]} ${jar}"
}

return this