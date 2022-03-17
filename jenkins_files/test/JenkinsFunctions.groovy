import groovy.transform.Field

dags = [RUBY_TO_MIP:"Found me"]

def getCerts(String dag_ID) {
    sh "echo '${dag_ID}'"
    if (dags[dag_ID] == "Found me") {
        sh "echo 'Yes'"
    }
}

def test() {
    sh "echo 'test'"
}

return this