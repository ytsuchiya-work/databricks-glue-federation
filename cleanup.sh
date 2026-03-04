#!/bin/bash
#
# cleanup.sh - Remove Databricks Unity Catalog → AWS Glue Federation
#
# Usage:
#   ./cleanup.sh --prefix myproject
#

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_banner() {
    echo ""
    echo -e "${YELLOW}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║  Databricks → AWS Glue Federation Cleanup                                 ║${NC}"
    echo -e "${YELLOW}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_help() {
    echo "Usage: ./cleanup.sh --prefix PREFIX [--region REGION]"
    echo ""
    echo "Options:"
    echo "  --prefix PREFIX    The prefix used when setting up the federation"
    echo "  --region REGION    AWS region (default: from AWS CLI config)"
    echo "  --force            Skip confirmation prompts"
    echo "  --help             Show this help message"
    echo ""
    echo "Example:"
    echo "  ./cleanup.sh --prefix myproject --region us-west-2"
}

# Parse arguments
PREFIX=""
AWS_REGION=""
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --force)
            FORCE=true
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

if [ -z "$PREFIX" ]; then
    echo -e "${RED}Error: --prefix is required${NC}"
    show_help
    exit 1
fi

# Smart prefix handling: strip common suffixes if user provided full resource name
# e.g., "myproject-catalog" -> "myproject", "myproject-connection" -> "myproject"
PREFIX="${PREFIX%-catalog}"
PREFIX="${PREFIX%-connection}"
PREFIX="${PREFIX%-glue-role}"
PREFIX="${PREFIX%-oauth-secret}"

if [ -z "$AWS_REGION" ]; then
    AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-west-2")
fi

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)

print_banner

echo "This will delete the following resources:"
echo ""
echo "  • Glue Catalog: ${PREFIX}-catalog"
echo "  • Glue Connection: ${PREFIX}-connection"
echo "  • IAM Role: ${PREFIX}-glue-role"
echo "  • Secret: ${PREFIX}-oauth-secret"
echo "  • Lake Formation registration"
echo ""

if [ "$FORCE" != true ]; then
    read -p "Are you sure you want to delete these resources? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Cleanup cancelled."
        exit 0
    fi
fi

echo ""
echo -e "${BLUE}Starting cleanup...${NC}"
echo ""

# Delete Glue Catalog
echo -n "Deleting Glue Catalog... "
aws glue delete-catalog --catalog-id "${AWS_ACCOUNT_ID}:${PREFIX}-catalog" --region "$AWS_REGION" 2>/dev/null && echo -e "${GREEN}done${NC}" || echo -e "${YELLOW}skipped (not found)${NC}"

# Deregister from Lake Formation
echo -n "Deregistering from Lake Formation... "
aws lakeformation deregister-resource --resource-arn "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:connection/${PREFIX}-connection" --region "$AWS_REGION" 2>/dev/null && echo -e "${GREEN}done${NC}" || echo -e "${YELLOW}skipped (not found)${NC}"

# Delete Glue Connection
echo -n "Deleting Glue Connection... "
aws glue delete-connection --connection-name "${PREFIX}-connection" --region "$AWS_REGION" 2>/dev/null && echo -e "${GREEN}done${NC}" || echo -e "${YELLOW}skipped (not found)${NC}"

# Delete IAM Role policies
echo -n "Deleting IAM Role policies... "
aws iam delete-role-policy --role-name "${PREFIX}-glue-role" --policy-name "S3-access" 2>/dev/null || true
aws iam delete-role-policy --role-name "${PREFIX}-glue-role" --policy-name "secrets-access" 2>/dev/null || true
echo -e "${GREEN}done${NC}"

# Delete IAM Role
echo -n "Deleting IAM Role... "
aws iam delete-role --role-name "${PREFIX}-glue-role" 2>/dev/null && echo -e "${GREEN}done${NC}" || echo -e "${YELLOW}skipped (not found)${NC}"

# Delete Secret
echo -n "Deleting Secret... "
aws secretsmanager delete-secret --secret-id "${PREFIX}-oauth-secret" --force-delete-without-recovery --region "$AWS_REGION" 2>/dev/null && echo -e "${GREEN}done${NC}" || echo -e "${YELLOW}skipped (not found)${NC}"

echo ""
echo -e "${GREEN}✓ Cleanup complete!${NC}"
echo ""

