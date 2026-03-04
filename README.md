# Databricks Unity Catalog から AWS Glue へのフェデレーション

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Databricks Unity Catalog のテーブルを Amazon Athena、EMR、Redshift Spectrum、およびその他の AWS サービスからクエリできるようにします。

---

## 背景

re:Invent 2025 にて、AWS は Glue のフェデレーテッドカタログを発表しました。これにより、Iceberg REST API を通じて Databricks Unity Catalog などの外部カタログソースに接続できるようになりました。

問題は？動作させるためには以下が必要です：

- 特定のフォーマット（`USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET`）で Secrets Manager のシークレットを作成する
- 適切な信頼ポリシーを持つ IAM ロールを設定する
- ロールに `secrets-access` ポリシーをアタッチする（これは分かりにくい）
- `--with-federation` を使って Glue コネクションを Lake Formation に登録する
- 正しい設定でフェデレーテッドカタログを作成する

私はこれらすべてのデバッグに数時間を費やしました。このスクリプトはその全行程を自動化します。

---

## 仕組み

```
┌─────────────────────────┐         ┌─────────────────────────┐
│  Databricks             │         │  AWS                    │
│  Unity Catalog          │         │                         │
│                         │         │  ┌─────────────────┐    │
│  ┌─────────────────┐    │  ────>  │  │  Glue Catalog   │    │
│  │  my_catalog     │    │ Iceberg │  │  (Federated)    │    │
│  │   └── default   │    │  REST   │  └────────┬────────┘    │
│  │        └── tbl  │    │   API   │           │             │
│  └─────────────────┘    │         │           v             │
│                         │         │  ┌─────────────────┐    │
└─────────────────────────┘         │  │ Athena / EMR /  │    │
                                    │  │ Redshift / etc  │    │
                                    │  └─────────────────┘    │
                                    └─────────────────────────┘
```

このスクリプトを実行すると、Athena から直接 Databricks のテーブルをクエリできます：

```sql
SELECT * FROM "my-federated-catalog"."default".my_table LIMIT 10;
```

---

## クイックスタート

```bash
git clone https://github.com/YOURUSERNAME/databricks-glue-federation.git
cd databricks-glue-federation
./setup.sh
```

スクリプトが必要な情報をすべて対話的に聞いてきます。

---

## 前提条件

- AWS CLI がインストール・設定済みであること（`aws configure`）
- OAuth 認証情報を持つ Databricks サービスプリンシパル
- フェデレーションしたい Unity Catalog へのアクセス権

### 提供が必要な情報：

| 値 | 確認場所 |
|---|---|
| ワークスペース URL | Databricks にログイン時のブラウザのアドレスバー |
| カタログ名 | Databricks カタログエクスプローラー |
| OAuth クライアント ID | アカウントコンソール > サービスプリンシパル |
| OAuth クライアントシークレット | サービスプリンシパル設定時に作成 |
| S3 バケット | カタログのストレージロケーション |

---

## 使い方

### 対話モード

```bash
./setup.sh
```

### 設定ファイルを使用する場合

```bash
cp examples/config.example.env .env
# .env をお好みの値で編集
./setup.sh --config .env
```

### ドライラン（テスト実行）

```bash
./setup.sh --dry-run
```

---

## 作成されるリソース

| リソース | 名前 | 用途 |
|---|---|---|
| Secrets Manager | `{prefix}-oauth-secret` | OAuth 認証情報の保存 |
| IAM ロール | `{prefix}-glue-role` | Glue が S3 とシークレットにアクセスするため |
| Glue コネクション | `{prefix}-connection` | Databricks Iceberg REST API への接続 |
| Glue カタログ | `{prefix}-catalog` | AWS で参照可能なフェデレーテッドカタログ |

---

## クリーンアップ

```bash
./cleanup.sh --prefix your-prefix
```

---

## 新しいテーブルが表示されない場合

AWS Lake Formation では、フェデレーテッドカタログに対してテーブルごとに明示的な権限が必要です。Unity Catalog で新しいテーブルを作成した場合は、以下を実行してください：

```bash
./sync-permissions.sh --catalog your-catalog-name
```

このスクリプトは：
- すべてのデータベースのすべてのテーブルを検出
- Lake Formation の権限を並列で付与（デフォルトで 50 並列）
- 冪等性あり（何度実行しても安全）

**速度：**
| テーブル数 | 所要時間（50 並列） | 所要時間（100 並列） |
|---|---|---|
| 1000 | 約 30 秒 | 約 15 秒 |
| 5000 | 約 2 分 | 約 1 分 |
| 10000 | 約 4 分 | 約 2 分 |

最大速度で実行する場合：`./sync-permissions.sh --catalog my-catalog --parallel 100`

---

## トラブルシューティング

### 「Insufficient Lake Formation permission(s) on ...」

最もよくあるエラーです。以下を確認してください：

1. **正しいカタログをクエリしていますか？** 元の UC カタログ名ではなく、フェデレーテッド Glue カタログ名（例：`prefix-catalog`）を使用してください：
   ```bash
   aws glue get-catalogs --query 'CatalogList[].Name'
   ```

2. **IAM ロールが Lake Formation 管理者になっていますか？** LF コンソールで確認するか、以下を実行してください：
   ```bash
   aws lakeformation get-data-lake-settings --query 'DataLakeSettings.DataLakeAdmins'
   ```

3. **特定のロールに権限を付与：**
   ```bash
   ROLE_ARN=$(aws sts get-caller-identity --query 'Arn' --output text)
   aws lakeformation grant-permissions \
     --principal "{\"DataLakePrincipalIdentifier\": \"${ROLE_ARN}\"}" \
     --resource '{"Catalog": {"Id": "ACCOUNT:CATALOG"}}' \
     --permissions "ALL"
   ```

4. **default データベースへの権限を付与：**
   ```bash
   aws lakeformation grant-permissions \
     --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
     --resource '{"Database": {"Name": "default"}}' \
     --permissions "DESCRIBE"
   ```

### 「Access Denied for the given secret ID」

IAM ロールにシークレットへアクセスするための明示的なポリシーが必要です。スクリプトはこれを自動で処理しますが、手動で設定する場合：

```json
{
  "Effect": "Allow",
  "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret", "secretsmanager:PutSecretValue"],
  "Resource": ["arn:aws:secretsmanager:REGION:ACCOUNT:secret:YOUR-SECRET-*"]
}
```

### 「The data source is not registered」

カタログを作成する前に、Glue コネクションを Lake Formation に登録する必要があります：

```bash
aws lakeformation register-resource \
  --resource-arn "arn:aws:glue:REGION:ACCOUNT:connection/CONNECTION" \
  --role-arn "arn:aws:iam::ACCOUNT:role/ROLE" \
  --with-federation
```

### Athena が「TABLE_NOT_FOUND」または「no accessible columns」を返す

フェデレーテッドカタログでは、Lake Formation にテーブルごとの権限が必要です。以下を実行してください：

```bash
./sync-permissions.sh --catalog your-catalog-name
```

または単一テーブルの場合：

```bash
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
  --resource '{"Table": {"CatalogId": "ACCOUNT:CATALOG", "DatabaseName": "default", "Name": "your_table"}}' \
  --permissions "ALL"
```

注意：`TableWildcard` はフェデレーテッドカタログでは機能しません（AWS の制限事項）。

---

## よくある質問

**Delta テーブルで動作しますか？**

はい、UniForm（Iceberg）が有効であれば動作します。

**複数のカタログをフェデレーションできますか？**

はい。異なるプレフィックスでスクリプトを複数回実行してください。

**サービスプリンシパルにはどの権限が必要ですか？**

クエリしたいテーブルに対する `USE CATALOG` と `SELECT` 権限です。

**なぜ新しいテーブルが自動的に Athena に表示されないのですか？**

AWS Lake Formation では、フェデレーテッドカタログに対してテーブルごとの明示的な権限が必要です。これは AWS の制限事項で、ワイルドカードは機能しません。新しいテーブルを作成した後に `./sync-permissions.sh` を実行してください。

**同期を自動化できますか？**

はい！cron ジョブまたは Lambda を設定して `sync-permissions.sh` を定期的に実行してください（例：5 分ごと）。

---

## ライセンス

MIT
