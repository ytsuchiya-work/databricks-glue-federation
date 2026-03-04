#!/bin/bash
#
# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Databricks Unity Catalog → AWS Glue Federation Setup                     ║
# ║                                                                            ║
# ║  This script creates a federation between your Databricks Unity Catalog   ║
# ║  and AWS Glue, allowing you to query your Databricks tables using         ║
# ║  Amazon Athena, EMR, Redshift Spectrum, and other AWS services.           ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
#
# Usage:
#   ./setup.sh                    # Interactive mode (recommended for first-time)
#   ./setup.sh --config .env      # Use config file
#   ./setup.sh --help             # Show help
#

set -euo pipefail

# ============================================================================
# COLORS AND FORMATTING
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

print_banner() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC}  ${BOLD}Databricks Unity Catalog → AWS Glue Federation${NC}                          ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}                                                                           ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}  ${DIM}Query your Databricks tables from Athena, EMR, and more!${NC}               ${CYAN}║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_step() {
    local step=$1
    local total=$2
    local message=$3
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step ${step}/${total}: ${message}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_success() {
    echo -e "  ${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "  ${RED}✗${NC} $1"
}

print_warning() {
    echo -e "  ${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "  ${DIM}→${NC} $1"
}

show_help() {
    echo "Usage: ./setup.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --config FILE    Load configuration from a file"
    echo "  --dry-run        Show what would be created without creating it"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./setup.sh                    # Interactive mode"
    echo "  ./setup.sh --config .env      # Use config file"
    echo ""
    echo "For more information, visit:"
    echo "  https://github.com/databricks/databricks-glue-federation"
}

check_prerequisites() {
    print_step 1 7 "Checking prerequisites"
    
    local missing=()
    
    # Check AWS CLI
    if command -v aws &> /dev/null; then
        local aws_version=$(aws --version 2>&1 | cut -d/ -f2 | cut -d' ' -f1)
        print_success "AWS CLI installed (v${aws_version})"
    else
        missing+=("aws")
        print_error "AWS CLI not found"
        echo ""
        echo -e "  ${YELLOW}To install AWS CLI:${NC}"
        echo "    macOS:   brew install awscli"
        echo "    Linux:   curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip' && unzip awscliv2.zip && sudo ./aws/install"
        echo "    Windows: https://aws.amazon.com/cli/"
    fi
    
    # Check if AWS is configured
    if aws sts get-caller-identity &> /dev/null; then
        local aws_account=$(aws sts get-caller-identity --query 'Account' --output text)
        local aws_user=$(aws sts get-caller-identity --query 'Arn' --output text | rev | cut -d'/' -f1 | rev)
        print_success "AWS credentials configured (Account: ${aws_account}, User: ${aws_user})"
    else
        print_error "AWS credentials not configured"
        echo ""
        echo -e "  ${YELLOW}To configure AWS credentials:${NC}"
        echo "    Run: aws configure"
        echo "    Enter your Access Key ID, Secret Access Key, and Region"
        missing+=("aws-credentials")
    fi
    
    # Check curl
    if command -v curl &> /dev/null; then
        print_success "curl installed"
    else
        missing+=("curl")
        print_error "curl not found"
    fi
    
    # Check jq (optional but helpful)
    if command -v jq &> /dev/null; then
        print_success "jq installed (for JSON formatting)"
    else
        print_warning "jq not found (optional - install for better output)"
        echo -e "  ${DIM}Install with: brew install jq (macOS) or apt install jq (Linux)${NC}"
    fi
    
    if [ ${#missing[@]} -gt 0 ]; then
        echo ""
        print_error "Missing required tools. Please install them and try again."
        exit 1
    fi
}

prompt_with_help() {
    local var_name=$1
    local prompt_text=$2
    local help_text=$3
    local default_value=${4:-""}
    local is_secret=${5:-"false"}
    
    echo ""
    echo -e "${BOLD}${prompt_text}${NC}"
    echo -e "${DIM}${help_text}${NC}"
    
    if [ -n "$default_value" ]; then
        echo -e "${DIM}Default: ${default_value}${NC}"
    fi
    
    echo -n "> "
    
    if [ "$is_secret" = "true" ]; then
        read -s user_input
        echo ""
    else
        read user_input
    fi
    
    if [ -z "$user_input" ] && [ -n "$default_value" ]; then
        user_input="$default_value"
    fi
    
    eval "$var_name='$user_input'"
}

validate_databricks_url() {
    local url=$1
    if [[ ! "$url" =~ ^https://.*\.cloud\.databricks\.com$ ]] && [[ ! "$url" =~ ^https://.*\.azuredatabricks\.net$ ]]; then
        print_warning "URL doesn't look like a standard Databricks workspace URL"
        echo -e "  ${DIM}Expected format: https://dbc-xxxxx.cloud.databricks.com${NC}"
        read -p "  Continue anyway? (y/n): " confirm
        if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
            exit 1
        fi
    fi
}

validate_oauth_credentials() {
    local workspace_url=$1
    local client_id=$2
    local client_secret=$3
    
    print_info "Testing OAuth credentials..."
    
    local token_url="${workspace_url}/oidc/v1/token"
    local response=$(curl -s -X POST "${token_url}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=client_credentials&client_id=${client_id}&client_secret=${client_secret}&scope=all-apis" 2>&1)
    
    if echo "$response" | grep -q "access_token"; then
        print_success "OAuth credentials are valid!"
        return 0
    else
        print_error "OAuth authentication failed"
        echo ""
        echo -e "  ${YELLOW}Possible issues:${NC}"
        echo "    • Client ID or Secret is incorrect"
        echo "    • Service Principal doesn't have workspace access"
        echo "    • Workspace URL is wrong"
        echo ""
        echo -e "  ${DIM}Response: ${response}${NC}"
        return 1
    fi
}

# ============================================================================
# MAIN SETUP FUNCTIONS
# ============================================================================

collect_configuration() {
    print_step 2 7 "Gathering configuration"
    
    echo ""
    echo -e "${BOLD}Let's set up your federation! I'll guide you through each step.${NC}"
    
    # Resource prefix
    prompt_with_help "PREFIX" \
        "1. Choose a name prefix for your resources" \
        "This will be used to name all AWS resources (e.g., 'mycompany' → 'mycompany-glue-federation')" \
        "databricks-federation"
    
    # Databricks Workspace URL
    prompt_with_help "DATABRICKS_WORKSPACE_URL" \
        "2. Enter your Databricks Workspace URL" \
        "Find this in your browser's address bar when logged into Databricks.
   Example: https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
    
    # Remove trailing slash
    DATABRICKS_WORKSPACE_URL="${DATABRICKS_WORKSPACE_URL%/}"
    validate_databricks_url "$DATABRICKS_WORKSPACE_URL"
    
    # Unity Catalog name
    prompt_with_help "UC_CATALOG_NAME" \
        "3. Enter the Unity Catalog name you want to federate" \
        "This is the catalog in Databricks that contains your tables.
   Find it in: Databricks → Catalog (left sidebar) → Look at the top-level items
   Example: main, production, my_catalog"
    
    # OAuth Client ID
    prompt_with_help "DATABRICKS_CLIENT_ID" \
        "4. Enter your Databricks OAuth Client ID" \
        "This is the Application (client) ID of your Service Principal.
   Find it in: Databricks Account Console → User Management → Service Principals
   Example: 50f5acd6-beca-4d1b-831f-9d3cf171b89a"
    
    # OAuth Client Secret
    prompt_with_help "DATABRICKS_CLIENT_SECRET" \
        "5. Enter your Databricks OAuth Client Secret" \
        "This is the secret you created for your Service Principal.
   ⚠️  This will be stored in AWS Secrets Manager (encrypted)" \
        "" \
        "true"
    
    # Validate OAuth before continuing
    if ! validate_oauth_credentials "$DATABRICKS_WORKSPACE_URL" "$DATABRICKS_CLIENT_ID" "$DATABRICKS_CLIENT_SECRET"; then
        echo ""
        read -p "Do you want to continue anyway? (y/n): " confirm
        if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
            exit 1
        fi
    fi
    
    # S3 Bucket
    prompt_with_help "S3_BUCKET_NAME" \
        "6. Enter the S3 bucket name where your table data is stored" \
        "Just the bucket name, not the full path!
   Find it in: Unity Catalog → Your Catalog → Storage Location
   Example: my-company-datalake-bucket (not s3://my-company-datalake-bucket/path/...)"
    
    # Strip s3:// prefix and any path if user included it
    S3_BUCKET_NAME="${S3_BUCKET_NAME#s3://}"
    S3_BUCKET_NAME="${S3_BUCKET_NAME%%/*}"
    
    # AWS Region
    prompt_with_help "AWS_REGION" \
        "7. Enter the AWS Region" \
        "This should match where your S3 bucket and Databricks workspace are located.
   Example: us-west-2, us-east-1, eu-west-1" \
        "$(aws configure get region 2>/dev/null || echo 'us-west-2')"
    
    # Summary
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Configuration Summary${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "  Prefix:            ${PREFIX}"
    echo "  Workspace URL:     ${DATABRICKS_WORKSPACE_URL}"
    echo "  Unity Catalog:     ${UC_CATALOG_NAME}"
    echo "  Client ID:         ${DATABRICKS_CLIENT_ID}"
    echo "  Client Secret:     ********** (hidden)"
    echo "  S3 Bucket:         ${S3_BUCKET_NAME}"
    echo "  AWS Region:        ${AWS_REGION}"
    echo ""
    
    read -p "Does this look correct? (y/n): " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        echo "Setup cancelled. Please run the script again."
        exit 1
    fi
}

create_secret() {
    print_step 3 7 "Creating AWS Secrets Manager secret"
    
    local secret_name="${PREFIX}-oauth-secret"
    
    # Check if secret already exists
    if aws secretsmanager describe-secret --secret-id "$secret_name" --region "$AWS_REGION" &> /dev/null; then
        print_warning "Secret already exists: ${secret_name}"
        print_info "Updating secret value..."
        aws secretsmanager put-secret-value \
            --secret-id "$secret_name" \
            --secret-string "{\"USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET\":\"${DATABRICKS_CLIENT_SECRET}\"}" \
            --region "$AWS_REGION" > /dev/null
    else
        print_info "Creating new secret..."
        aws secretsmanager create-secret \
            --name "$secret_name" \
            --description "OAuth secret for Databricks Unity Catalog federation (${PREFIX})" \
            --secret-string "{\"USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET\":\"${DATABRICKS_CLIENT_SECRET}\"}" \
            --region "$AWS_REGION" > /dev/null
    fi
    
    SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$secret_name" --region "$AWS_REGION" --query 'ARN' --output text)
    print_success "Secret created: ${secret_name}"
    
    # Add resource policy
    print_info "Configuring secret access policy..."
    aws secretsmanager put-resource-policy \
        --secret-id "$secret_name" \
        --resource-policy "{
            \"Version\": \"2012-10-17\",
            \"Statement\": [
                {
                    \"Effect\": \"Allow\",
                    \"Principal\": {\"Service\": [\"glue.amazonaws.com\", \"lakeformation.amazonaws.com\"]},
                    \"Action\": [\"secretsmanager:GetSecretValue\", \"secretsmanager:DescribeSecret\"],
                    \"Resource\": \"*\"
                },
                {
                    \"Effect\": \"Allow\",
                    \"Principal\": {\"AWS\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:root\"},
                    \"Action\": [\"secretsmanager:GetSecretValue\", \"secretsmanager:DescribeSecret\"],
                    \"Resource\": \"*\"
                }
            ]
        }" \
        --region "$AWS_REGION" > /dev/null
    
    print_success "Secret policy configured"
}

create_iam_role() {
    print_step 4 7 "Creating IAM role for Glue federation"
    
    local role_name="${PREFIX}-glue-role"
    ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${role_name}"
    
    # Check if role exists
    if aws iam get-role --role-name "$role_name" &> /dev/null; then
        print_warning "Role already exists: ${role_name}"
    else
        print_info "Creating IAM role..."
        aws iam create-role \
            --role-name "$role_name" \
            --assume-role-policy-document "{
                \"Version\": \"2012-10-17\",
                \"Statement\": [{
                    \"Effect\": \"Allow\",
                    \"Principal\": {\"Service\": [\"glue.amazonaws.com\", \"lakeformation.amazonaws.com\"]},
                    \"Action\": \"sts:AssumeRole\"
                }]
            }" \
            --description "Role for Glue federation with Databricks Unity Catalog (${PREFIX})" > /dev/null
        print_success "IAM role created: ${role_name}"
    fi
    
    # Attach S3 policy with comprehensive permissions
    print_info "Attaching S3 access policy..."
    aws iam put-role-policy \
        --role-name "$role_name" \
        --policy-name "S3-access" \
        --policy-document "{
            \"Version\": \"2012-10-17\",
            \"Statement\": [
                {
                    \"Effect\": \"Allow\",
                    \"Action\": [
                        \"s3:GetObject\",
                        \"s3:GetObjectVersion\",
                        \"s3:GetObjectTagging\"
                    ],
                    \"Resource\": [\"arn:aws:s3:::${S3_BUCKET_NAME}/*\"]
                },
                {
                    \"Effect\": \"Allow\",
                    \"Action\": [
                        \"s3:ListBucket\",
                        \"s3:GetBucketLocation\",
                        \"s3:GetBucketVersioning\"
                    ],
                    \"Resource\": [\"arn:aws:s3:::${S3_BUCKET_NAME}\"]
                }
            ]
        }"
    print_success "S3 access policy attached"
    
    # Attach Secrets policy
    print_info "Attaching secrets access policy..."
    aws iam put-role-policy \
        --role-name "$role_name" \
        --policy-name "secrets-access" \
        --policy-document "{
            \"Version\": \"2012-10-17\",
            \"Statement\": [{
                \"Effect\": \"Allow\",
                \"Action\": [\"secretsmanager:GetSecretValue\", \"secretsmanager:DescribeSecret\", \"secretsmanager:PutSecretValue\"],
                \"Resource\": [\"${SECRET_ARN}\"]
            }]
        }"
    print_success "Secrets access policy attached"
    
    print_info "Waiting for IAM propagation (15 seconds)..."
    sleep 15
}

create_glue_connection() {
    print_step 5 7 "Creating AWS Glue connection"
    
    local connection_name="${PREFIX}-connection"
    local iceberg_url="${DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest"
    local token_url="${DATABRICKS_WORKSPACE_URL}/oidc/v1/token"
    
    # Check if connection exists
    if aws glue get-connection --name "$connection_name" --region "$AWS_REGION" &> /dev/null; then
        print_warning "Connection already exists: ${connection_name}"
        print_info "Deleting and recreating..."
        aws glue delete-connection --connection-name "$connection_name" --region "$AWS_REGION" 2>/dev/null || true
        sleep 2
    fi
    
    print_info "Creating Glue connection to Databricks..."
    
    local result=$(aws glue create-connection \
        --region "$AWS_REGION" \
        --connection-input "{
            \"Name\": \"${connection_name}\",
            \"Description\": \"Connection to Databricks Unity Catalog (${UC_CATALOG_NAME})\",
            \"ConnectionType\": \"DATABRICKSICEBERGRESTCATALOG\",
            \"ConnectionProperties\": {
                \"ROLE_ARN\": \"${ROLE_ARN}\",
                \"INSTANCE_URL\": \"${iceberg_url}\"
            },
            \"AuthenticationConfiguration\": {
                \"AuthenticationType\": \"OAUTH2\",
                \"SecretArn\": \"${SECRET_ARN}\",
                \"OAuth2Properties\": {
                    \"OAuth2GrantType\": \"CLIENT_CREDENTIALS\",
                    \"OAuth2ClientApplication\": {
                        \"UserManagedClientApplicationClientId\": \"${DATABRICKS_CLIENT_ID}\"
                    },
                    \"TokenUrl\": \"${token_url}\",
                    \"TokenUrlParametersMap\": {
                        \"scope\": \"all-apis\"
                    }
                }
            }
        }" 2>&1)
    
    # Check status
    local status=$(aws glue get-connection --name "$connection_name" --region "$AWS_REGION" --query 'Connection.Status' --output text 2>/dev/null || echo "UNKNOWN")
    
    if [ "$status" = "READY" ]; then
        print_success "Glue connection created: ${connection_name} (Status: READY)"
        CONNECTION_NAME="$connection_name"
    else
        print_error "Connection creation failed or not ready. Status: ${status}"
        echo -e "  ${DIM}${result}${NC}"
        exit 1
    fi
}

register_with_lakeformation() {
    print_step 6 7 "Registering with Lake Formation"
    
    local connection_arn="arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:connection/${CONNECTION_NAME}"
    
    # Get the current user's ARN
    local caller_arn=$(aws sts get-caller-identity --query 'Arn' --output text)
    
    print_info "Registering connection with Lake Formation..."
    
    # Register resource
    aws lakeformation register-resource \
        --resource-arn "$connection_arn" \
        --role-arn "$ROLE_ARN" \
        --with-federation \
        --region "$AWS_REGION" 2>/dev/null || print_warning "Resource may already be registered"
    
    print_success "Connection registered with Lake Formation"
    
    # Make the current user a Data Lake Administrator (bypasses all LF permission checks)
    print_info "Adding you as a Data Lake Administrator..."
    
    aws lakeformation put-data-lake-settings \
        --data-lake-settings "{
            \"DataLakeAdmins\": [
                {\"DataLakePrincipalIdentifier\": \"${caller_arn}\"}
            ],
            \"CreateDatabaseDefaultPermissions\": [],
            \"CreateTableDefaultPermissions\": []
        }" \
        --region "$AWS_REGION" 2>/dev/null || print_warning "Could not update Data Lake settings (you may need to do this manually)"
    
    print_success "You are now a Data Lake Administrator (full access to all tables)"
    
    # Grant permissions on the default database (Glue often checks this)
    print_info "Granting permissions on default database..."
    aws lakeformation grant-permissions \
        --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
        --resource "{\"Database\": {\"Name\": \"default\"}}" \
        --permissions "DESCRIBE" \
        --region "$AWS_REGION" 2>/dev/null || true
    
    # Also grant to the Glue role explicitly
    aws lakeformation grant-permissions \
        --principal "{\"DataLakePrincipalIdentifier\": \"${ROLE_ARN}\"}" \
        --resource "{\"Database\": {\"Name\": \"default\"}}" \
        --permissions "DESCRIBE" \
        --region "$AWS_REGION" 2>/dev/null || true
    
    print_success "Default database permissions granted"
}

create_federated_catalog() {
    print_step 7 7 "Creating federated Glue catalog"
    
    local catalog_name="${PREFIX}-catalog"
    
    # Check if catalog exists
    if aws glue get-catalog --catalog-id "${AWS_ACCOUNT_ID}:${catalog_name}" --region "$AWS_REGION" &> /dev/null; then
        print_warning "Catalog already exists: ${catalog_name}"
    else
        print_info "Creating federated catalog..."
        
        aws glue create-catalog \
            --name "$catalog_name" \
            --region "$AWS_REGION" \
            --catalog-input "{
                \"Description\": \"Federated catalog for ${UC_CATALOG_NAME} from Databricks Unity Catalog\",
                \"FederatedCatalog\": {
                    \"Identifier\": \"${UC_CATALOG_NAME}\",
                    \"ConnectionName\": \"${CONNECTION_NAME}\"
                },
                \"CreateTableDefaultPermissions\": [],
                \"CreateDatabaseDefaultPermissions\": []
            }"
        
        print_success "Federated catalog created: ${catalog_name}"
    fi
    
    CATALOG_NAME="$catalog_name"
    
    # Wait for catalog to be fully available
    print_info "Waiting for catalog to be ready (5 seconds)..."
    sleep 5
    
    # Grant permissions
    print_info "Granting Lake Formation permissions..."
    
    # Grant catalog-level permissions to IAM_ALLOWED_PRINCIPALS
    aws lakeformation grant-permissions \
        --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
        --resource "{\"Catalog\": {\"Id\": \"${AWS_ACCOUNT_ID}:${catalog_name}\"}}" \
        --permissions "ALL" \
        --region "$AWS_REGION" 2>/dev/null || true
    
    # Also grant to the Glue role explicitly (fixes permission errors)
    aws lakeformation grant-permissions \
        --principal "{\"DataLakePrincipalIdentifier\": \"${ROLE_ARN}\"}" \
        --resource "{\"Catalog\": {\"Id\": \"${AWS_ACCOUNT_ID}:${catalog_name}\"}}" \
        --permissions "ALL" \
        --region "$AWS_REGION" 2>/dev/null || true
    
    print_success "Catalog permissions granted"
    
    # Grant database and table permissions (PARALLEL for speed)
    local databases=$(aws glue get-databases --catalog-id "${AWS_ACCOUNT_ID}:${catalog_name}" --region "$AWS_REGION" --query 'DatabaseList[*].Name' --output text 2>/dev/null || echo "")
    
    # First, grant database-level permissions (quick, sequential)
    for db in $databases; do
        aws lakeformation grant-permissions \
            --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
            --resource "{\"Database\": {\"CatalogId\": \"${AWS_ACCOUNT_ID}:${catalog_name}\", \"Name\": \"${db}\"}}" \
            --permissions "ALL" \
            --region "$AWS_REGION" 2>/dev/null || true
        # Also grant to Glue role
        aws lakeformation grant-permissions \
            --principal "{\"DataLakePrincipalIdentifier\": \"${ROLE_ARN}\"}" \
            --resource "{\"Database\": {\"CatalogId\": \"${AWS_ACCOUNT_ID}:${catalog_name}\", \"Name\": \"${db}\"}}" \
            --permissions "ALL" \
            --region "$AWS_REGION" 2>/dev/null || true
        print_success "Database permissions granted: ${db}"
    done
    
    # Collect all tables for parallel processing
    print_info "Collecting tables for permission grants..."
    local temp_file=$(mktemp)
    local total_tables=0
    
    for db in $databases; do
        local tables=$(aws glue get-tables --catalog-id "${AWS_ACCOUNT_ID}:${catalog_name}" --database-name "$db" --region "$AWS_REGION" --query 'TableList[*].Name' --output text 2>/dev/null || echo "")
        for table in $tables; do
            echo "${db}|${table}" >> "$temp_file"
            total_tables=$((total_tables + 1))
        done
    done
    
    if [ "$total_tables" -gt 0 ]; then
        print_info "Granting permissions on ${total_tables} tables (50 parallel jobs)..."
        
        local parallel_jobs=50
        local granted=0
        
        while IFS='|' read -r db table; do
            (
                aws lakeformation grant-permissions \
                    --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
                    --resource "{\"Table\": {\"CatalogId\": \"${AWS_ACCOUNT_ID}:${catalog_name}\", \"DatabaseName\": \"${db}\", \"Name\": \"${table}\"}}" \
                    --permissions "ALL" \
                    --region "$AWS_REGION" 2>/dev/null || true
                echo -e "  ${GREEN}✓${NC} ${db}.${table}"
            ) &
            
            # Limit parallel jobs
            while [ $(jobs -r | wc -l) -ge $parallel_jobs ]; do
                sleep 0.1
            done
        done < "$temp_file"
        
        # Wait for all background jobs
        wait
        
        print_success "All table permissions granted!"
    else
        print_info "No tables found (they'll be granted when you run sync-permissions.sh)"
    fi
    
    rm -f "$temp_file"
}

print_success_summary() {
    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                                                                           ║${NC}"
    echo -e "${GREEN}║   🎉  Federation Setup Complete!                                         ║${NC}"
    echo -e "${GREEN}║                                                                           ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${BOLD}What was created:${NC}"
    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │  AWS Secrets Manager                                                     │"
    echo "  │    └── ${PREFIX}-oauth-secret                                            │"
    echo "  │                                                                          │"
    echo "  │  IAM Role                                                                │"
    echo "  │    └── ${PREFIX}-glue-role                                               │"
    echo "  │                                                                          │"
    echo "  │  AWS Glue                                                                │"
    echo "  │    ├── Connection: ${PREFIX}-connection                                  │"
    echo "  │    └── Catalog: ${PREFIX}-catalog                                        │"
    echo "  │                                                                          │"
    echo "  │  Lake Formation                                                          │"
    echo "  │    └── Registered with federation enabled                                │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""
    echo -e "${BOLD}Your Unity Catalog tables are now accessible in AWS!${NC}"
    echo ""
    echo -e "${YELLOW}Try it out with Amazon Athena:${NC}"
    echo ""
    echo "  1. Open the Athena console: https://${AWS_REGION}.console.aws.amazon.com/athena"
    echo ""
    echo "  2. Run this query:"
    echo -e "     ${CYAN}SELECT * FROM \"${CATALOG_NAME}\".\"default\".your_table_name LIMIT 10;${NC}"
    echo ""
    echo -e "${YELLOW}Or list your tables with AWS CLI:${NC}"
    echo ""
    echo -e "  ${CYAN}aws glue get-tables --catalog-id \"${AWS_ACCOUNT_ID}:${CATALOG_NAME}\" --database-name \"default\" --region ${AWS_REGION}${NC}"
    echo ""
    echo -e "${YELLOW}When you create NEW tables in Unity Catalog:${NC}"
    echo ""
    echo -e "  Run: ${CYAN}./sync-permissions.sh --catalog ${CATALOG_NAME}${NC}"
    echo ""
    echo "  This grants Lake Formation permissions on all tables (runs in parallel, fast!)."
    echo ""
    echo -e "${DIM}To remove this federation, run: ./cleanup.sh --prefix ${PREFIX}${NC}"
    echo ""
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    # Parse arguments
    CONFIG_FILE=""
    DRY_RUN=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Load config file if specified
    if [ -n "$CONFIG_FILE" ]; then
        if [ -f "$CONFIG_FILE" ]; then
            source "$CONFIG_FILE"
            echo "Loaded configuration from: $CONFIG_FILE"
        else
            echo "Config file not found: $CONFIG_FILE"
            exit 1
        fi
    fi
    
    print_banner
    check_prerequisites
    
    # Get AWS Account ID
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)
    
    collect_configuration
    
    if [ "$DRY_RUN" = true ]; then
        echo ""
        echo -e "${YELLOW}DRY RUN - No resources will be created${NC}"
        echo "The following would be created:"
        echo "  - Secret: ${PREFIX}-oauth-secret"
        echo "  - IAM Role: ${PREFIX}-glue-role"
        echo "  - Glue Connection: ${PREFIX}-connection"
        echo "  - Glue Catalog: ${PREFIX}-catalog"
        exit 0
    fi
    
    create_secret
    create_iam_role
    create_glue_connection
    register_with_lakeformation
    create_federated_catalog
    print_success_summary
}

main "$@"

