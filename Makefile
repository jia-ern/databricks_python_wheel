BUILD_DIR='dist'

#############################################################################
# MAIN TARGETS
#############################################################################
install::
	uv python install 3.12
	uv venv -p 3.12
	uv sync

databricks_wheel::
	uv build --wheel --out-dir $(BUILD_DIR)

databricks_workflow_all::
	migrate_sf_to_databricks


#############################################################################
# Databricks Workflow Deployment
#############################################################################
migrate_sf_to_databricks::
	python infra/create_databricks_workflow.py

