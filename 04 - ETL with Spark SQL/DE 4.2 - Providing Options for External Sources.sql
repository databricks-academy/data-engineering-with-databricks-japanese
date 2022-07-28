-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # 外部ソース用にオプションを指定する（Providing Options for External Sources）
-- MAGIC ファイルを直接照会することは自己記述的な形式には適していますが、多くのデータソースでは適切にレコードを取り込むためには追加の設定もしくはスキーマの宣言が必要となります。
-- MAGIC 
-- MAGIC このレッスンでは、外部のデータソースを使用してテーブルを作成します。 これらのテーブルはまだDelta Lake形式では保存されません（したがってレイクハウス用には最適されません）が、この技を使用すると多様な外部システムからのデータ抽出を円滑にできます。
-- MAGIC 
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC - Spark SQLを使用して外部ソースからデータを抽出するためにオプションを設定する
-- MAGIC - さまざまなファイル形式用の外部データソースに対してテーブルを作成する
-- MAGIC - 外部ソースに対して定義されたテーブルを照会する際のデフォルト動作を説明する

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC 
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 直接的なクエリを使用できない場合 （When Direct Queries Don't Work）
-- MAGIC 
-- MAGIC セッションの間に直接的なクエリを保持するためにビューを使用できますが、この手法の実用性は限られています。
-- MAGIC 
-- MAGIC CSVは最も一般的なファイル形式の1つですが、これらのファイルに対する直接的なクエリを使用しても望ましい結果は滅多に得られません。

-- COMMAND ----------

SELECT * FROM csv.`${da.paths.working_dir}/sales-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 上記から次のことが分かります：
-- MAGIC 1. ヘッダの列がテーブルの列として抽出されています
-- MAGIC 1. すべての列が１つの列として読み込まれています
-- MAGIC 1. ファイルはパイプ（ **`|`** ）区切りを使用しています
-- MAGIC 1. 最後の列には、切り捨てられるネスト化されたデータが含まれています。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 読み取りオプションを使用して外部データに対してテーブルを登録する（Registering Tables on External Data with Read Options）
-- MAGIC 
-- MAGIC Sparkはデフォルトの設定で一部の自己記述的なデータソースを効率的に抽出できますが、多くの形式ではスキーマの宣言もしくは他のオプションが必要となります。
-- MAGIC 
-- MAGIC 外部ソースに対してテーブルを作成する際、多くの<a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">追加設定</a>を行えますが、以下の構文ではほとんどの形式からデータを抽出するための基本を示します。
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC オプションは、引用符を使用しないテキストのキーおよび引用符を使用する値で渡されることにご注意ください。 Sparkは、カスタムのオプションで多くの<a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">データソース</a>をサポートーしており、外部<a href="https://docs.databricks.com/libraries/index.html" target="_blank">ライブラリ</a>を介して追加のシステムが非公式にサポートされている可能性があります。
-- MAGIC 
-- MAGIC **注**：ワークスペースの設定によっては、一部のデータソースに対して、ライブラリを読み込み、必要なセキュリティ設定を行うには管理者の手伝いが必要になる場合があります。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以下のセルでは、Spark SQL DDLを使用して、外部のCSVソースに対して次の情報を指定したテーブルを作成する方法を示します：
-- MAGIC 1. 列の名前と型
-- MAGIC 1. ファイル形式
-- MAGIC 1. フィールドを区切るための区切り文字
-- MAGIC 1. ヘッダの有無
-- MAGIC 1. このデータの保存先へのパス

-- COMMAND ----------

CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルの宣言時にはデータが移動していないことにご注意ください。 ファイルを直接クエリしてビューを作成したときと同様に、あくまで外部の場所に保存されているファイルを指しているだけです。
-- MAGIC 
-- MAGIC 次のセルを実行してデータが正しく読み込まれていることを確認しましょう。

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルの宣言時に渡されたメタデータとオプションはすべてメタストアに保持され、この場所のデータが常にこのオプションを使用して読み取られるようにします。
-- MAGIC 
-- MAGIC **注**：データソースとしてCSVを扱っている場合は、ソースディレクトリにデータファイルが追加されても列の順序が変わらないようにするのが重要です。 このデータ形式には強力なスキーマ強制がないため、Sparkは、テーブルの宣言時に指定された順序で列を読み込み列の名前とデータ型を適用します。
-- MAGIC 
-- MAGIC テーブルに対して **`DESCRIBE EXTENDED`** を実行すると、このテーブルの定義に関連づけられているすべてのメタデータが表示されます。

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 外部のデータソースを使用したテーブルの制限（Limits of Tables with External Data Sources）
-- MAGIC 
-- MAGIC Databricksで講座を受けたり弊社の文献を読んだりしたことのある方は、Delta Lakeとレイクハウスのことを聞いたことがあるかもしれません。 外部のデータソースに対してテーブルもしくはクエリを定義するとき、Delta Lakeとレイクハウスを伴うパフォーマンスの保証は期待**できません**のでご注意ください。
-- MAGIC 
-- MAGIC 例えば、Delta Lakeのテーブルを使用すると、常にソースデータの最新バージョンがクエリされることが保証されますが、他のデータソースに対して登録されているテーブルを使用した場合は、キャッシュされた以前のバージョンを表している可能性があります。
-- MAGIC 
-- MAGIC 以下のセルでは、テーブルの元になっているファイルを直接更新する外部システムを表しているものと考えられるロジックが実行されます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("sales_csv")
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(f"{DA.paths.working_dir}/sales-csv"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルにある現在のレコード数を見ても、表示される数字にはこれらの新しく挿入された列は反映されません。

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 以前、このデータソースを照会したとき、Sparkはその元になっているデータを自動的にローカルストレージのキャッシュに格納しました。 これにより、その後のクエリでSparkは、このローカルのキャッシュを照会するだけで最適なパフォーマンスを実現できます。
-- MAGIC 
-- MAGIC この外部データソースは、このデータは更新すべきだとSparkに伝えるようには設定されていません。
-- MAGIC 
-- MAGIC  **`REFRESH TABLE`** コマンドを実行すると、データのキャッシュを手動で更新**できます**。

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルを更新するとキャッシュが無効になるため、元のデータソースを再スキャンしてすべてのデータをまたメモリに取り込む必要が出てくることにご注意ください。
-- MAGIC 
-- MAGIC 非常に大きなデータセットの場合、この処理には長時間がかかる場合があります。

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## SQLデータベースからデータを抽出（Extracting Data from SQL Databases）
-- MAGIC SQLデータベースは、非常に一般的なデータソースであり、Databricksにはさまざまな種類のSQLに接続するための標準JDBCドライバが備わっています。
-- MAGIC 
-- MAGIC このような接続を生成する一般的な構文は次の通りです：
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC 下のコードサンプルでは、<a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>を使用して接続します。
-- MAGIC 
-- MAGIC **注意：**SQLiteは、ローカルファイルにデータベースを保管し、ポート、ユーザー名、パスワードを必要としません。
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png" /> **要注意**：JDBCサーバのバックエンド構成は、このノートブックがシングルノードクラスタで実行されていることを前提としています。 複数のワーカーを使用したクラスタを実行している場合、エグゼキュータで実行されているクライアントはドライバに接続することができません。

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC これで、ローカルで定義されているかのようにこのテーブルを照会できます。

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルのメタデータを確認すると、外部システムからスキーマの情報を取り込んだことが分かります。 （この接続に関連づけられているユーザー名とパスワードが含まれる）ストレージのプロパティは自動的に編集されます。

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC テーブルは **`MANAGED`** として表示されますが、指定された場所の中身を表示するとローカルに保持されているデータがないことが分かります。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jdbc_users_path = f"{DA.paths.user_db}/users_jdbc/"
-- MAGIC print(jdbc_users_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(jdbc_users_path)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC データウェアハウスなど、一部のSQLシステムにはカスタムのドライバがあることにご注意ください。 Sparkがさまざまな外部のデータソースと相互作用する方法は異なりますが、2つの基本的な方法は次の通り要約できます：
-- MAGIC 1. ソーステーブルを丸ごとDatabricksに移動させて、現在アクティブなクラスタでロジックを実行する
-- MAGIC 1. クエリを外部のSQLデータベースに転送して、Databricksに結果のみを返す
-- MAGIC 
-- MAGIC どちらにしても、外部のSQLデータベースで非常に大きなデータセットを扱うと、次の理由のどちらかのせいで深刻なオーバーヘッドにつながってしまいます。
-- MAGIC 1. 公共インターネットですべてのデータを移動させることによるネットワーク転送の遅延
-- MAGIC 1. ビッグデータクエリに最適化されていないソースシステムでのクエリロジックの実行

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
