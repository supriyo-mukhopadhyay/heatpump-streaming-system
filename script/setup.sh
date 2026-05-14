#!/bin/bash
set -e
export de_project="de-c3w2lab1"
export AWS_DEFAULT_REGION="us-east-1"
export VPC_ID=$(aws ec2 describe-vpcs --filter Name=tag:Name,Values=de-c3w2lab1 --query Vpcs[].VpcId --output text)

# Define Terraform variables
echo "export TF_VAR_project=$de_project" >> $HOME/.bashrc
echo "export TF_VAR_region=$AWS_DEFAULT_REGION" >> $HOME/.bashrc
echo "export TF_VAR_vpc_id=$VPC_ID" >> $HOME/.bashrc
echo "export TF_VAR_public_subnet_a_id=$(aws ec2 describe-subnets --filters "Name=tag:aws:cloudformation:logical-id,Values=PublicSubnetA" "Name=vpc-id,Values=$VPC_ID" --output text --query "Subnets[].SubnetId")" >> $HOME/.bashrc
echo "export TF_VAR_data_lake_name=$de_project-$(aws sts get-caller-identity --query 'Account' --output text)-$AWS_DEFAULT_REGION-data-lake"  >> $HOME/.bashrc
echo "export TF_VAR_glue_scripts_name=$de_project-$(aws sts get-caller-identity --query 'Account' --output text)-$AWS_DEFAULT_REGION-glue-scripts"  >> $HOME/.bashrc

source $HOME/.bashrc

# Final success message
echo "Setup completed successfully. All environment variables and Terraform backend configurations have been set."