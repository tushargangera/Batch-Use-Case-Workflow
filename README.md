# Batch Use Case Workflow

This Terraform script sets up a batch processing workflow in an AWS Databricks Workspace. The workflow consists of several notebooks that perform various tasks such as data processing, quality checks, and data lake population.

## Prerequisites

Before running this Terraform script, ensure you have the following:

- An AWS Databricks Workspace instance.
- AWS IAM credentials with appropriate permissions.
- Terraform installed on your local machine.

## Configuration

1. Clone this repository to your local machine.
2. Create a `variables.tf` file in the project directory and set the following variables:

    ```hcl
    variable "databricks_host" {
      description = "The host URL of your Databricks workspace"
    }

    variable "databricks_token" {
      description = "The Databricks API token"
    }

    variable "databricks_instance_profile" {
      description = "The ARN of the instance profile to be used by Databricks"
    }
    ```

3. Modify the `main.tf` file to reference these variables for Databricks host, token, and instance profile.
4. Ensure that your AWS IAM credentials are properly configured and accessible.
5. Ensure you have the necessary Databricks notebooks stored in your workspace under the specified paths.

## Usage

1. Initialize Terraform in the project directory:

    ```bash
    terraform init
    ```

2. Review the execution plan:

    ```bash
    terraform plan
    ```

3. Apply the changes to provision the infrastructure:

    ```bash
    terraform apply
    ```

4. Once the Terraform apply completes successfully, your Databricks batch processing workflow will be set up according to the configurations specified in the `main.tf` file.

## Structure

- `main.tf`: Contains the main Terraform configuration for provisioning Databricks resources including instance profile, notebooks, cluster, job, etc.
- `variables.tf`: Defines input variables for Databricks host, token, and instance profile.
- `README.md`: This file, providing an overview of the project and instructions for usage.
- `Notebooks/`: Directory containing the Databricks notebooks referenced in the Terraform script.
- `outputs.tf`: Defines the output values to be displayed after provisioning.

## Additional Notes

- Ensure that the specified paths for notebooks in the `main.tf` file match the actual paths in your Databricks Workspace.
- Customize the email notifications, schedule, cluster settings, and other configurations in the `main.tf` file according to your requirements.
- Review and test thoroughly before deploying to production environments.

```

This README.md now includes instructions for creating a `variables.tf` file and setting the necessary variables for Databricks host, token, and instance profile.
