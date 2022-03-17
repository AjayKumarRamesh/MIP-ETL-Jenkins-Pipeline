def dags = [RUBY_TO_MIP:"Found me"]

def getCerts(dag_ID) {
    assertTrue(dags[dag_ID] == "Found me")
}

return this