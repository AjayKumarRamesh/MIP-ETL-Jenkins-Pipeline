import groovy.transform.Field

dags = [RUBY_TO_MIP:"Found me"]
envNum = [dev:1,test:2,:prod:3]

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

def checkDagStatus(String airflow_pod, String dag_ID, String status) {
    //while loop checking for status 
    //AIRFLOW_POD = sh(script:'kubectl get pods -n airflow --no-headers -o custom-columns=":metadata.name" | grep airflow-scheduler', returnStdout: true).trim()
    DAG_CURRENT_RUN = sh(script:"kubectl exec -n airflow ${airflow_pod} -- airflow dags list-runs -d ${dag_ID} --state running -o json", returnStdout: true).trim()

    timeout(time: 2, unit: "HOURS") {
        def dag_run_object = readJSON text: DAG_CURRENT_RUN
        if (dag_run_object.size() > 0) {
            //def dag_run_object = readJSON text: DAG_CURRENT_RUN
            DAG_EXECUTION_DATE = dag_run_object[0]['execution_date']
            while (DAG_STATUS == status) {
                DAG_STATUS = sh(script:"""kubectl exec -n airflow ${airflow_pod} -- airflow dags state ${dag_ID} ${DAG_EXECUTION_DATE} | egrep 'running|failed|success'""", returnStdout: true).trim()
                sh "echo 'Checking DAG status before pause: ${DAG_STATUS}'"
                sleep(10)
            }
            sh "echo 'DAG no longer in run state, continuing with deployment.'"
        } else {
            sh "echo 'No current ${dag_ID} DAGs found running.'"
        }
    }

}

def moveImage() {
    //move images from dev to test 
    sh "ibmcloud cr login"
    sh "docker pull us.icr.io/map-dev-namespace/${RUBY_IMAGE}"
    sh "docker tag us.icr.io/map-dev-namespace/${RUBY_IMAGE} us.icr.io/mip-test-namespace/${RUBY_IMAGE}"
    sh "docker push us.icr.io/mip-test-namespace/${RUBY_IMAGE}"
}

return this