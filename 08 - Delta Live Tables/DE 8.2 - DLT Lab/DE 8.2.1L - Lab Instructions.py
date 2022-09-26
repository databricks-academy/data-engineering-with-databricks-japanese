# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="2eb97b71-b2ab-4b68-afdc-1663ec49e9d4"/>
# ラボ：SQLノートブックをDelta Live Tablesに移行する（Lab: Migrating SQL Notebooks to Delta Live Tables）

このノートブックはラボのエクササイズ用に全体構造を説明して、ラボの環境を構成します。そして、シミュレートされたデータストリーミングを提供して、すべてが終了するとクリーンアップを実行します。 このようなノートブックは、通常プロダクションパイプラインのシナリオでは必要としていません。

## 学習目標（Learning Objectives）
このラボでは、以下のことが学べます。
* 既存のデータパイプラインをDelta Live Tablesに変換する

# COMMAND ----------

# MAGIC %md <i18n value="782da0e9-5fc2-4deb-b7a4-939af49e38ed"/>
## 使用するデータセット（Datasets Used）

このデモでは、簡略化されて人工的に生成された医療データを使用します。 2つのデータセットのスキーマは以下の通りです。 様々な段階でこれらのスキーマを操作することに留意してください。

#### レコーディング（Recordings）
主なデータセットは、医療機器からJSON形式で配信される心拍数の記録を使用します。

| フィールド     | 型      |
| --------- | ------ |
| device_id | int    |
| mrn       | long   |
| time      | double |
| heartrate | double |

#### PII
これらのデータは後に外部システムで保存されている患者情報の静的テーブルと結合し、名前で患者を特定できるようになります。

| フィールド | 型      |
| ----- | ------ |
| mrn   | long   |
| name  | string |

# COMMAND ----------

# MAGIC %md <i18n value="b691e21b-24a5-46bc-97d8-a43e9ae6e268"/>
## はじめる（Getting Started）

まずは次のセルを実行して、ラボ環境を構成します。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-08.2.1L

# COMMAND ----------

# MAGIC %md <i18n value="c68290ac-56ad-4d6e-afec-b0a61c35386f"/>
## 初期データの配置（Land Initial Data）
先に進む前に、データを用いてランディングゾーンをシードします。 後でこのコマンドを再実行して追加データを配置します。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="7cb98302-06c2-4384-bdf7-2260cbf2662d"/>
以下のセルを実行して、次の構成段階で使用する値を出力します。

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="784d3bc4-5c4e-4ef8-ab56-3ebaa92238b0"/>
## パイプラインを作成し構成する（Create and Configure a Pipeline）

1. サイドバーの **ワークフロー**ボタンをクリックします。
1. **Delta Live Tables**タブを選択します。
1. **パイプラインを作成**をクリックします。
1. **製品エディション**は**Advanced**のままにします。
1. **パイプライン名**を入力します。名前は一意である必要があるため、上記のセルに記載されている**Pipeline Name**を使用することをおすすめします。
1. **ノートブックライブラリ** では、ナビゲーターを使って上記指定のノートブックを探して選択します。
1. **構成**に、3つのパラメータを追加します。
    * **構成を追加**をクリックし, "key"を**spark.master**、"value"を**local[\*]**にします。
    * **構成を追加**をクリックし, "key"を**datasets_path**、"value"を上記セルにある値にします。
    * * **構成を追加**をクリックし, "key"を**source**、"value"を上記セルにある値にします。 
1. **`Target`** フィールドに上記セルにある値を入力します。<br/>
**`da_<name_<hash>_dewd_dlt_lab_82`**というパターンのデータベース名になります。
1. **`Storage Location`** フィールドに上記セルにある値を入力します。
1. **パイプラインモード**を**トリガー**に設定します。
1. オートスケールを無効化します。
　　オートスケールを無効化する機能がないUIの場合は、**Cluster mode**から**Fixed size**を選択します。
1. **`ワーカー`** の数を **`1`** （1つ）に設定します。
1. **Photonアクセラレータを使用**をチェックします。
1. **作成**をクリックします。

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="3340e93d-1fad-4549-bf79-ec239b1d59d4"/>
## DLTパイプラインノートブックを開き、完了する（Open and Complete DLT Pipeline Notebook）

作業は [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab)という付録のノートブックで行います。<br/> このノートブックは、最終的にパイプラインとしてデプロイします。

ノートブックを開き、そこに記載されている手順に従って、セルに入力していきます。<br/>これらのセルは、以前のセクションと同様にマルチホップアーキテクチャを実装します。

# COMMAND ----------

# MAGIC %md <i18n value="90a66079-16f8-4503-ab48-840cbdd07914"/>
## パイプラインを実行する（Run your Pipeline）

実行間で同じクラスタを再利用して開発ライフサイクルを加速させる**開発**モードを選択します。<br/> これにより、ジョブが失敗した際の自動再試行もオフになります。

**開始**をクリックして、テーブルの最初の更新を開始します。

Delta Live Tablesは、すべての必要なインフラストラクチャを自動でデプロイし、すべてのデータセット間の依存関係を特定します。

**注**：最初のテーブルの更新では、関係を特定しインフラをデプロイするため、数分程度の時間を要する場合があります。

# COMMAND ----------

# MAGIC %md <i18n value="d1797d22-692c-43ce-b146-1e0248e65da3"/>
## 開発モードでコードをトラブルシューティングする（Troubleshooting Code in Development Mode）

初回でパイプラインが失敗しても、落胆しないでください。 Delta Live Tablesは開発中であり、エラーメッセージは常に改善されています。

テーブル間の関係はDAGとしてマップされているため、エラーメッセージはデータセットが見つからないことを示す場合がよくあります。

以下のDAGを考えてみましょう。

<img src="https://files.training.databricks.com/images/dlt-dag.png" />

`**  Dataset not found: 'recordings_parsed'`** というエラーメッセージが発生した場合、いくつかの原因が考えられます。
1. **`recordings_parsed`** を定義するロジックが無効である
1. **`recordings_bronze`** からの読み取りにエラーが発生した
1. **`recordings_parsed`** または **`recordings_bronze`** にタイプミスがある

原因を特定する最も安全な方法は、最初の取り込みテーブルを皮切りに、テーブルまたはビューの定義を繰り返しまたDAGに追加することです。 後でテーブルまたはビューの定義をコメントアウトし、実行と実行の間でコメントアウトを外すことができます。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
