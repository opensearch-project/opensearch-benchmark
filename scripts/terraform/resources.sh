#!/bin/bash

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "AWS_ACCESS_KEY_ID is not set"
    exit 1
fi
if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "AWS_SECRET_ACCESS_KEY is not set"
    exit 1
fi

# Function to get all AWS regions
get_all_regions() {
    aws ec2 describe-regions --region us-east-1 --query 'Regions[].RegionName' --output text
}

get_resource() {
    title="$1"
    command="$2"
    if [ -n "${command}" ]; then
        echo "${title} ${command}"
    fi
}

# Print info
output_info() {
    region="$1"
    resources="$2"

    if [ ${#resources[@]} -gt 0 ]; then
        echo -e "Region: ${region}\n"
        for r in "${resources[@]}"; do
            if [ ! "$r" == "" ]; then
                echo -e "${r}\n"
            fi
        done
        printf "+%.0s" {1..80}
        echo ""
    fi
}

# Loop through each region and list/delete resources
for region in $(get_all_regions); do
    # Set AWS region for the commands
    export AWS_DEFAULT_REGION=$region

    ec2_instances=$(get_resource "EC2:" "$(aws ec2 describe-instances --query 'Reservations[].Instances[].InstanceId' --output text)")
    s3_buckets=$(get_resource "S3:" "$(aws s3api list-buckets --query 'Buckets[].Name' --output text)")
    vpcs=$(get_resource "VPC:" "$(aws ec2 describe-vpcs --query 'Vpcs[].VpcId' --output text)")
    secrets=$(get_resource "SecretsManager:" "$(aws secretsmanager list-secrets --query 'SecretList[].Name' --output text)")
    kms=$(get_resource "KMS:" "$(aws kms list-keys --query 'Keys[].KeyId' --output text)")
    sg=$(get_resource "SecurityGroup:" "$(aws ec2 describe-security-groups --query 'SecurityGroups[].GroupId' --output text)")
    ip=$(get_resource "IP:" "$(aws ec2 describe-addresses --query 'Addresses[].PublicIp' --output text)")

    resources=("$ec2_instances" "$s3_buckets" "$vpcs" "$secrets" "$kms" "$sg" "$ip")
    output_info "$region" "${resources[@]}"
done