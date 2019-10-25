CFN_TEMPLATES := $(wildcard CloudFormation/*.yaml)
PIPELINE_CFN_NAME := 'pipedrive-automation-pipeline'
PIPELINE_NAME := 'pipedrive-automation'
PIPELINE_TEMPLATE := 'CodePipeline/pipeline.yaml'

help:
	@echo "Targets:"
	@echo "    get-pipeline  -- Download the pipline CloudFormation template for into CodePipeline/pipeline.yml"
	@echo "    get-pipeline-params -- Download the parameters to the pipeline CloudFormation stack
	@echo "    put-pipeline  -- Update the pipeline with CodePipeline/pipeline.yml"

get-pipeline:
	@echo "Downloading pipeline template"
	@mkdir -vp CodePipeline
	@aws cloudformation get-template --stack-name $(PIPELINE_CFN_NAME) \
		--query 'TemplateBody' --output text > CodePipeline/pipeline.yaml

get-pipeline-params:
	@echo "Downloading pipeline parameters"
	@mkdir -vp CodePipeline
	@aws cloudformation describe-stacks --stack-name $(PIPELINE_CFN_NAME) \
		--query 'Stacks[0].Parameters' > CodePipeline/params.json

put-pipeline:
	@echo "Updating pipeline"
	@if aws codepipeline get-pipeline-state --name $(PIPELINE_NAME) | grep -q '"status": "InProgress"';then \
		echo "ERROR: Pipeline is in progress, please wait until it's complete before updating."; \
		exit 1; \
	else \
	        true; \
	fi
	@aws cloudformation deploy --stack-name $(PIPELINE_CFN_NAME) \
		--template-file $(PIPELINE_TEMPLATE) \
		--capabilities CAPABILITY_IAM
	@command -v cfn-tail > /dev/null && AWS_PROFILE=${AWS_DEFAULT_PROFILE} cfn-tail $(PIPELINE_CFN_NAME)
	@aws cloudformation wait stack-update-complete --stack-name $(PIPELINE_CFN_NAME)
	@aws cloudformation describe-stacks --stack-name $(PIPELINE_CFN_NAME) --output text --query 'Stacks[0].StackStatus'

cloudformation:
	@for f in $(CFN_TEMPLATES); do \
		echo $$f; \
		aws cloudformation validate-template --template-body file://$$f || break; \
	done

.PHONY: pipeline cloudformation
