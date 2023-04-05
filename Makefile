TWITTER_DIR = twitter
REDDIT_DIR = reddit
TERRAFORM_FOLDER = terraform

.PHONY: build_twitter_layer build_reddit_layer terraform_init terraform_plan terraform_apply terraform_destroy full_build init_full_build 

$(TWITTER_DIR)/layer:
	$(MAKE) clean_twitter_layer
	cd $(TWITTER_DIR) && mkdir python

$(REDDIT_DIR)/layer:
	$(MAKE) clean_reddit_layer
	cd $(REDDIT_DIR) && mkdir python

init_full_build:
	$(MAKE) terraform_init
	$(MAKE) build_twitter_layer
	$(MAKE) build_reddit_layer
	$(MAKE) terraform_apply

full_build:
	$(MAKE) build_twitter_layer
	$(MAKE) build_reddit_layer
	$(MAKE) terraform_apply

terraform_init:
	cd terraform && terraform init

terraform_plan:
	cd terraform && terraform plan

terraform_apply:
	cd terraform && terraform apply

terraform_destroy:
	cd terraform && terraform destroy

build_twitter_layer: $(TWITTER_DIR)/layer
	cd $(TWITTER_DIR) && pip install -r requirements.txt -t python/
	cd $(TWITTER_DIR)&& zip -r layer.zip python

build_reddit_layer: $(REDDIT_DIR)/layer
	cd $(REDDIT_DIR) && pip install -r requirements.txt -t python/
	cd $(REDDIT_DIR)&& zip -r layer.zip python
	
clean_twitter_layer:
	rm -rf $(TWITTER_DIR)/python

clean_reddit_layer:
	rm -rf $(REDDIT_DIR)/python