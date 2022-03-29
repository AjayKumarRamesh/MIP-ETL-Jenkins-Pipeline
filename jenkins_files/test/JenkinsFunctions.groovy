import groovy.transform.Field

dags = [RUBY_TO_MIP:"Found me"]
airflow = [RUBY_TO_MIP:['ruby_image', 'ruby_app_jar']]
envNum = [dev:1,test:2,prod:3]

def getCerts(String dag_ID) {
    sh "echo '${dag_ID}'"
    if (dags[dag_ID] == "Found me") {
        sh "echo 'Yes'"
    }
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
    /**
    sh "kubectl exec -n airflow ${AIRFLOW_POD} -- airflow variables set ruby_image ${RUBY_IMAGE}"
    sh "kubectl exec -n airflow ${AIRFLOW_POD} -- airflow variables set ruby_app_jar ${RUBY_APP_JAR}"
    sh "kubectl exec -n airflow ${AIRFLOW_POD} -- airflow variables get 'ruby_image'"
    sh "kubectl exec -n airflow ${AIRFLOW_POD} -- airflow variables get 'ruby_app_jar'"
    */
}

def createAirflowVars(String airflow_pod, String dag_ID, String image, String jar) {
    sh "kubectl exec -n airflow ${airflow_pod} -- airflow variables set ${airflow[dag_ID][0]} ${image}"
    sh "kubectl exec -n airflow ${airflow_pod} -- airflow variables set ${airflow[dag_ID][1]} ${jar}"
}

return this