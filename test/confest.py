import airflow.models as af_models

for module in glob.glob("dags/*.py"):
    assert any(
        isinstance(var, af_models.DAG)
        for var in vars(module).values()
    )
