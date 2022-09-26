# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="82f45276-7911-4045-b57d-c2891df6f785"/>
# Auto Loaderを使った増分データの取り込み

増分ルETLは最後の取り込み以降に検出された新しいデータのみを扱えるため、重要です。 新しいデータだけを確実に処理することで無駄なプロセスを省き、企業がデータパイプラインを拡張するのを助けます。

データレイクハウスの実装を成功させるための最初のステップは、クラウドストレージからDelta Lakeテーブルに取り込むことです。

もともとデータレイクからデータベースへファイルを取り込むのは、複雑なプロセスでした。

Databricks Auto Loaderは、クラウドファイルストレージに新しいデータファイルが到着すると、段階的かつ効率的な処理を行う簡易メカニズムを提供します。 このノートブックでは、そのAuto Loaderの操作を見ていきます。
![](https://files.training.databricks.com/images/autoloader-detection-modes.png)

Auto Loaderが提供する利点と拡張性を考えると、Databricksはクラウドオブジェクトストレージからデータを取り込む際の一般的な**ベストプラクティス**として、Auto Loaderの活用をお勧めします。

## 学習目標（Learning Objectives）
このレッスンでは、以下のことが学べます。
* Auto Loaderコードを実行し、データをクラウドストレージからDelta Lakeへ段階的に取り込む
* Auto Loader用に構成されたディレクトリに新しいファイルが到着すると何が起きるか説明する
* ストリーミングAuto Loaderクエリによって供給されたテーブルを照会する

## 使用するデータセット（Dataset Used）
このデモでは、心拍数の記録を示す、簡略化して人工的に生成されたJSON形式の医療データを使用します。

| フィールド     | 型      |
| --------- | ------ |
| device_id | int    |
| mrn       | long   |
| time      | double |
| heartrate | double |

# COMMAND ----------

# MAGIC %md <i18n value="3b8b82a4-9dd3-4b4d-a591-dca88bd064c6"/>
## はじめる（Getting Started）

次のセルを実行してデモをリセットし、必要な変数とヘルプ関数を構成します。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-06.1

# COMMAND ----------

# MAGIC %md <i18n value="188f9a32-fc72-40da-844a-b87cd14e358e"/>
## Auto Loaderを使う（Using Auto Loader）

以下のセルでは、PySpark APIを用いたDatabricks Auto Loaderを使用して、関数を定義します。 このコードは、構造化ストリーミングの読み取りと書き込みの両方を含みます。

次のノートブックでは、構造化ストリーミングのより堅牢な概要を示します。 Auto Loaderのオプションをさらに詳しく知りたい場合は、こちらの<a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">ドキュメント</a>を参考にしてください。

自動<a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">スキーマ推論と進化</a>を使ってAuto Loaderを使用する場合、ここに示す4つの引数を用いることでほぼすべてのデータセットの取り込みが可能になることに留意しましょう。 その引数とは次の通りです。

| 引数                         | 説明                    | 使い方                                                                                                                               |
| -------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
|  **`data_source`**           | ソースデータのディレクトリ         | Auto Loaderは、この場所に新しく到着したファイルを検出して取り込み用にキューし、 **`.load()`** メソッドに渡します                                                               |
|  **`source_format`**         | ソースデータの形式             | すべてのAuto Loaderクエリの形式は **`cloudFiles`** ですが、ソースデータの形式は常に **`cloudFiles.format`** オプションに指定する必要があります                                    |
|  **`table_name`**            | ターゲットテーブルの名前          | Spark構造化ストリーミングは、テーブル名を文字列として **`table()`** メソッドに渡すことで、Delta Lakeテーブルに直接書き込むのをサポートします。 既存のテーブルに追加することも、新しいテーブルを作成することもできる点に留意してください |
|  **`checkpoint_directory`**  | ストリームに関するメタデータを保存する場所 | この引数は、 **`および`**  **`cloudFiles.schemaLocation`** オプションに渡します。 チェックポイントはストリーミングの進行状況を追跡し、スキーマロケーションはソースデータセットにあるフィールドの更新を追跡します。        |

**注**：以下のコードは、Auto Loaderの機能性を示すために合理化されています。 この後のレッスンでは、Delta Lakeに保存する前のソースデータに適用できるその他の変換方法を見ていきます。

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

# MAGIC %md <i18n value="cf70df0f-f945-4efb-b8d0-e90d86d0bf9b"/>
以下のセルでは、既に定義された関数とセットアップスクリプトで指定したいくつかのパス変数を使ってAuto Loaderストリームを開始します。

ここでは、JSONファイルのソースディレクトリから読み取ります。

# COMMAND ----------

query = autoload_to_table(data_source = f"{DA.paths.working_dir}/tracker",
                          source_format = "json",
                          table_name = "target_table",
                          checkpoint_directory = f"{DA.paths.checkpoints}/target_table")

# COMMAND ----------

# MAGIC %md <i18n value="6d136721-85f1-474f-aaa1-3de8c2981e21"/>
Auto LoaderはSpark構造化ストリーミングを使用して段階的にデータをロードするため、上記のコードは実行を終了していないかのように映ります。

これを**継続的にアクティブなクエリ**と捉えることができます。 つまり、データソースに新たなデータが到着するとすぐにロジックに従って処理され、ターゲットテーブルにロードされるということです。 これについてはまもなく説明をします。

# COMMAND ----------

# MAGIC %md <i18n value="96a80d56-2fea-4865-8d80-73aea7a0169a"/>
## ストリーミングレッスン用のヘルパー関数（Helper Function for Streaming Lessons）

ノートブックベースのレッスンでは、ストリーミング関数と、これらの操作の結果に対するバッチおよびストリーミングクエリを組み合わせます。 これらのノートブックは教育目的であり、インタラクティブなセルごとの実行を目的としています。 このパターンは本番環境用ではありません。

以下では、特定のストリーミングクエリによってデータが確実に書き出されるのに十分な時間、ノートブックが次のセルを実行しないようにするヘルパー関数を定義します。 このコードは本番ジョブでは必要ありません。

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md <i18n value="56f6dfa2-c638-4812-8d7b-d4d480f97364"/>
## ターゲットテーブルを照会する（Query Target Table）

Auto Loaderを使ってデータがDelta Lakeに取り込まれると、ユーザーは他のテーブルと同じようにデータを操作できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM target_table

# COMMAND ----------

# MAGIC %md <i18n value="2bcece5b-7175-4b55-ab2c-40a67115a764"/>
形式が正しくなくテーブルに収まらないデータをキャプチャするため、 **`_rescued_data`** の列はAuto Loaderによって自動的に追加されることに留意しましょう。

Auto Loaderはデータのフィールド名を正しくキャプチャしましたが、すべてのフィールドは **`STRING`** 型としてエンコードした点に注意しましょう。 JSONはテキストベースの形式のため最も安全で許容度が高い型であり、取り込みの際に起きる型の不一致によるデータの削除や無視を最小限に抑えることができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE target_table

# COMMAND ----------

# MAGIC %md <i18n value="96d695c6-45f5-4d3e-b822-2fec12d72664"/>
以下のセルを用いて、ターゲットテーブルのレコーディングを要約するテンポラリビューを定義します。

以下のビューを使い、Auto Loaderで新しいデータを自動で取り込む方法を示します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts AS
# MAGIC   SELECT device_id, count(*) total_recordings
# MAGIC   FROM target_table
# MAGIC   GROUP BY device_id;
# MAGIC   
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md <i18n value="8048e413-c4c9-406c-be74-c541f976c8e3"/>
## 新しいデータの配置（Land New Data）

前述の通り、Auto LoaderはファイルをクラウドオブジェクトストレージのディレクトリからDelta Lakeテーブルへ段階的に処理するために構成されています。

JSONファイルを **`source_path`** で指定した場所から **`target_table`** というテーブルに処理するクエリを構成して、実行しています。  **`source_path`** ディレクトリのコンテンツを見てみましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="f7573e55-b6ab-46ad-9eae-cd037b4f554f"/>
今この場所に単一のJSONファイルが確認できるはずです。

以下のセルにあるメソッドは、このディレクトリにデータを書き込む外部システムをモデル化するためにセットアップスクリプトで構成されました。 以下のセルを実行する度、新しいファイルが **`source_path`** ディレクトリに配置されます。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="a8eeb778-e14e-42fc-90d1-86ed69ad06fb"/>
以下のセルを用いて、 **`source_path`** のコンテンツを再び表示します。 先ほどのセルを実行した回数だけ、追加のJSONファイルが確認できるはずです。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="d8323fbe-942c-405a-b3f1-5f4a3785ad95"/>
## 取り込みの進捗状況を追跡する（Tracking Ingestion Progress）

これまで多くのシステムは、ソースディレクトリにあるすべてのレコードを再処理して現在の結果を計算したり、テーブルの最終更新後に到着した新しいデータを特定するために、データエンジニアにカスタムロジックを実装してもらったりしなければいけない作りになっていました。

Auto Loaderを使って、テーブルは既に更新されました。

以下のクエリを実行して、新しいデータが取り込まれたことを確認します。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md <i18n value="b630929e-6d09-46d8-9cd0-2d5418b5840c"/>
先ほど構成したAuto Loaderクエリは、自動的にレコードを検出してソースディレクトリからターゲットテーブルに処理します。 レコードが取り込まれるためわずかな遅延が発生しますが、デフォルトのストリーミング構成で実行するAuto Loaderクエリはほぼリアルタイムで結果を更新します。

以下のクエリは、テーブル履歴を表しています。  **`STREAMING UPDATE`** ごとに新しいテーブルのバージョンを示す必要があります。 これらの更新イベントは、ソースに到着するデータの新しいバッチと一致します。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY target_table

# COMMAND ----------

# MAGIC %md <i18n value="a01651f5-70c8-45a4-859e-f9976eacf7a1"/>
## クリーンアップ（Clean up）
引き続き、上記のセルを使って新しいデータを配置し、テーブル結果を探ってみましょう。

終了したら、続行する前に次のセルを実行してすべてのアクティブなストリームを停止し、生成したリソースを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
