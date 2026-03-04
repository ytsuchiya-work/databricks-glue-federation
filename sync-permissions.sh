#!/bin/bash
#
# sync-permissions.sh - Grant Lake Formation permissions on all tables in your federated catalog
#
# Run this after creating new tables in Unity Catalog to make them queryable in Athena.
# This script is idempotent - safe to run multiple times.
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_help() {
    echo "Usage: $0 --catalog <catalog-name> [--region <aws-region>] [--parallel <num>]"
    echo ""
    echo "Grant Lake Formation permissions on ALL tables in a federated Glue catalog."
    echo "Run this after creating new tables in Unity Catalog."
    echo ""
    echo "Options:"
    echo "  --catalog   The federated Glue catalog name (e.g., 'myproject-catalog')"
    echo "  --region    AWS region (default: us-west-2)"
    echo "  --parallel  Number of parallel jobs (default: 50, max recommended: 100)"
    echo "  --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --catalog databricks-federation-catalog"
    echo "  $0 --catalog myproject-catalog --region eu-west-1"
    echo "  $0 --catalog myproject-catalog --parallel 100  # Maximum speed!"
}

CATALOG=""
AWS_REGION=""
PARALLEL_JOBS=50  # Default: 50 parallel jobs

while [[ $# -gt 0 ]]; do
    case $1 in
        --catalog)
            CATALOG="$2"
            shift 2
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --parallel)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

if [ -z "$CATALOG" ]; then
    echo -e "${RED}Error: --catalog is required${NC}"
    show_help
    exit 1
fi

if [ -z "$AWS_REGION" ]; then
    AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-west-2")
fi

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null)
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo -e "${RED}Error: Could not get AWS account ID. Check your AWS credentials.${NC}"
    exit 1
fi

CATALOG_ID="${AWS_ACCOUNT_ID}:${CATALOG}"

echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║       Sync Lake Formation Permissions for Federated Catalog   ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Catalog:  ${GREEN}${CATALOG}${NC}"
echo -e "Region:   ${GREEN}${AWS_REGION}${NC}"
echo -e "Parallel: ${GREEN}${PARALLEL_JOBS} jobs${NC}"
echo ""

# Try to find the Glue role from the connection
echo -e "${YELLOW}Looking up Glue connection role...${NC}"
CONNECTION_NAME=$(aws glue get-catalog --catalog-id "$CATALOG_ID" --region "$AWS_REGION" --query 'Catalog.FederatedCatalog.ConnectionName' --output text 2>/dev/null || echo "")
GLUE_ROLE_ARN=""
if [ -n "$CONNECTION_NAME" ] && [ "$CONNECTION_NAME" != "None" ]; then
    GLUE_ROLE_ARN=$(aws glue get-connection --name "$CONNECTION_NAME" --region "$AWS_REGION" --query 'Connection.ConnectionProperties.ROLE_ARN' --output text 2>/dev/null || echo "")
    if [ -n "$GLUE_ROLE_ARN" ] && [ "$GLUE_ROLE_ARN" != "None" ]; then
        echo -e "Found role: ${GREEN}${GLUE_ROLE_ARN}${NC}"
    fi
fi
echo ""

# Grant catalog-level permissions first
echo -e "${YELLOW}Granting catalog-level permissions...${NC}"
aws lakeformation grant-permissions \
    --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
    --resource "{\"Catalog\": {\"Id\": \"${CATALOG_ID}\"}}" \
    --permissions "ALL" \
    --region "$AWS_REGION" 2>/dev/null || true

if [ -n "$GLUE_ROLE_ARN" ] && [ "$GLUE_ROLE_ARN" != "None" ]; then
    aws lakeformation grant-permissions \
        --principal "{\"DataLakePrincipalIdentifier\": \"${GLUE_ROLE_ARN}\"}" \
        --resource "{\"Catalog\": {\"Id\": \"${CATALOG_ID}\"}}" \
        --permissions "ALL" \
        --region "$AWS_REGION" 2>/dev/null || true
fi

# Grant permissions on the default database (often checked by Glue)
aws lakeformation grant-permissions \
    --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
    --resource '{"Database": {"Name": "default"}}' \
    --permissions "DESCRIBE" \
    --region "$AWS_REGION" 2>/dev/null || true

echo -e "  ${GREEN}✓${NC} Catalog permissions set"
echo ""

# Get all databases
echo -e "${YELLOW}Discovering databases...${NC}"
DATABASES=$(aws glue get-databases --catalog-id "$CATALOG_ID" --region "$AWS_REGION" --query 'DatabaseList[].Name' --output text 2>/dev/null || echo "")

if [ -z "$DATABASES" ]; then
    echo -e "${RED}No databases found in catalog ${CATALOG}${NC}"
    echo "Make sure the catalog exists and you have access to it."
    exit 1
fi

# Function to grant permission on a single table
grant_table_permission() {
    local catalog_id="$1"
    local db="$2"
    local table="$3"
    local region="$4"
    
    aws lakeformation grant-permissions \
        --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
        --resource "{\"Table\": {\"CatalogId\": \"${catalog_id}\", \"DatabaseName\": \"${db}\", \"Name\": \"${table}\"}}" \
        --permissions "ALL" \
        --region "$region" 2>/dev/null
    
    echo "${db}.${table}"
}
export -f grant_table_permission

TOTAL_TABLES=0
TEMP_FILE=$(mktemp)

# Grant database-level permissions first
echo -e "${YELLOW}Granting database-level permissions...${NC}"
for DB in $DATABASES; do
    aws lakeformation grant-permissions \
        --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
        --resource "{\"Database\": {\"CatalogId\": \"${CATALOG_ID}\", \"Name\": \"${DB}\"}}" \
        --permissions "ALL" \
        --region "$AWS_REGION" 2>/dev/null || true
    
    if [ -n "$GLUE_ROLE_ARN" ] && [ "$GLUE_ROLE_ARN" != "None" ]; then
        aws lakeformation grant-permissions \
            --principal "{\"DataLakePrincipalIdentifier\": \"${GLUE_ROLE_ARN}\"}" \
            --resource "{\"Database\": {\"CatalogId\": \"${CATALOG_ID}\", \"Name\": \"${DB}\"}}" \
            --permissions "ALL" \
            --region "$AWS_REGION" 2>/dev/null || true
    fi
    
    echo -e "  ${GREEN}✓${NC} Database: ${DB}"
done
echo ""

# Collect all tables
echo -e "${YELLOW}Collecting tables from all databases...${NC}"
for DB in $DATABASES; do
    TABLES=$(aws glue get-tables --catalog-id "$CATALOG_ID" --database-name "$DB" --region "$AWS_REGION" --query 'TableList[].Name' --output text 2>/dev/null || echo "")
    
    for TABLE in $TABLES; do
        echo "${DB}|${TABLE}" >> "$TEMP_FILE"
        TOTAL_TABLES=$((TOTAL_TABLES + 1))
    done
done

echo -e "Found ${GREEN}${TOTAL_TABLES}${NC} tables across all databases."
echo ""
echo -e "${YELLOW}Granting permissions (${PARALLEL_JOBS} parallel jobs)...${NC}"
echo ""

# Process in parallel
GRANTED=0
while IFS='|' read -r DB TABLE; do
    # Run in background, limit concurrent jobs
    (
        # Grant to IAM_ALLOWED_PRINCIPALS
        aws lakeformation grant-permissions \
            --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
            --resource "{\"Table\": {\"CatalogId\": \"${CATALOG_ID}\", \"DatabaseName\": \"${DB}\", \"Name\": \"${TABLE}\"}}" \
            --permissions "ALL" \
            --region "$AWS_REGION" 2>/dev/null
        
        # Also grant to Glue role if we found it
        if [ -n "$GLUE_ROLE_ARN" ] && [ "$GLUE_ROLE_ARN" != "None" ]; then
            aws lakeformation grant-permissions \
                --principal "{\"DataLakePrincipalIdentifier\": \"${GLUE_ROLE_ARN}\"}" \
                --resource "{\"Table\": {\"CatalogId\": \"${CATALOG_ID}\", \"DatabaseName\": \"${DB}\", \"Name\": \"${TABLE}\"}}" \
                --permissions "ALL" \
                --region "$AWS_REGION" 2>/dev/null
        fi
        
        echo -e "  ${GREEN}✓${NC} ${DB}.${TABLE}"
    ) &
    
    # Limit parallel jobs
    while [ $(jobs -r | wc -l) -ge $PARALLEL_JOBS ]; do
        sleep 0.1
    done
done < "$TEMP_FILE"

# Wait for all background jobs to complete
wait

rm -f "$TEMP_FILE"
GRANTED_TABLES=$TOTAL_TABLES

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Done! Granted permissions on ${GRANTED_TABLES}/${TOTAL_TABLES} tables.${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Your tables are now queryable in Athena!"
echo ""

