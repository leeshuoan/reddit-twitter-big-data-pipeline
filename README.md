# Building the pipeline
Builds the entire big data pipeline programmatically using terraform, an Infrastructure as Code framework. Scraping topics are read from `./terraform/topic.txt`
## Prerequisites
###  For Windows Users:
- Make your life easier, install [Windows Subsystem for Linux](https://learn.microsoft.com/en-us/windows/wsl/install) and follow the steps below

###  For Mac Users:
- Make your life easier, install [Brew](https://brew.sh/)

1. [Install Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
2. [Install AWS CLI](https://aws.amazon.com/cli/)
3. Install `zip` (Check if it already exists by going into your CLI and run `zip`)
    a. [Linux](https://www.tecmint.com/install-zip-and-unzip-in-linux/)
    b. [MacOS](https://formulae.brew.sh/formula/zip)
4. Install `make` (Check if it already exists by going into your CLI and run `make`)
    a. **Linux:** Similarly, run `sudo <package-manager> make`. E.g. Ubuntu: `sudo apt install make`
    b. [MacOS](https://formulae.brew.sh/formula/make)

## How to run
1. Run `aws configure` and define credentials for the AWS account you want to use. Ensure that the IAM user has the `AdministratorFullAccess` policy in order for terraform to build the required resources 
2. In the terraform directory, rename `terraform.tfvars.example` to `terraform.tfvars` and add your own details into the file.
### Using Make Commands:
With `make` commands, you can run it from the root directory:
|Command|Use case|
|:---|:---|
|`make init_full_build`| Suitable for first run. Runs terraform init, creates layers needed for both twitter and reddit lambdas, and runs terraform apply to start building the infrastructure |
|`make full_build`| Suitable for subsequent runs where layers need to be updated. Creates layers needed for both twitter and reddit lambdas, and runs terraform apply to start building the infrastructure |
|`make terraform_<command>`| Runs terraform functions such as `init`, `plan`, `apply` and `destroy` from root directory|
|`make build_<source>_layer`| Builds source layers such as `twitter` and `reddit` for Lambda function|
|`make clean_<source>_layer`| Removes Python folder with dependencies|

### Using terraform Commands:
Enter terraform directory with `cd terraform`
|Command|Use case|
|:---|:---|
|`terraform <action>`|`init`, `plan`, `apply`, `destroy`, Learn more [Here](https://developer.hashicorp.com/terraform/cli/commands) |

# Testing locally
1. Create your own venv by running `python -m venv venv`
2. Activate your Python venv by running **MAC:** `source venv/bin/activate` or **Windows:** `source.bat`
3. Install the required libraries by running `pip install -r requirements.txt`
4. Rename `.env.example` to `.env` and add your own details into the file

# Architecture
![Architecture Diagram](https://user-images.githubusercontent.com/72493091/229983026-90c403ef-1380-4e31-a5c6-210676b47ae2.png)

# Pipeline Demonstration
- [Building the pipeline](https://youtu.be/XNzLogp0kxc)
- [Running the pipeline](https://youtu.be/qNZpKcV4xRo)
