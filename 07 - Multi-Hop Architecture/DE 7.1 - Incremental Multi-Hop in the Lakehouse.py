# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="c84bb70e-0f3a-4cb9-a8b4-882200c7c940"/>
# レイクハウスの増分マルチホップ（Incremental Multi-Hop in the Lakehouse）

構造化ストリーミングAPIとSpark SQLを組み合わせて増分データを処理する方法を理解したので、ここからは構造化ストリーミングとDelta Lakeの緊密な統合について学んでいきます。



## 学習目標（Learning Objectives）
このレッスンでは、以下のことが学べます。
* ブロンズテーブルとシルバーテーブル、ゴールドテーブルについて説明する
* Delta Lakeのマルチホップパイプラインを構築する

# COMMAND ----------

# MAGIC %md <i18n value="8f7d994a-fe1f-4628-825e-30c35b9ff187"/>
## レイクハウスの増分更新（Incremental Updates in the Lakehouse）

ユーザーはDelta Lakeを使うことで、統合したマルチホップパイプラインでストリーミングとバッチワークロードを簡単に組み合わせることができます。 パイプラインの各段階は、ビジネスでのコアなユースケースを推進するにあたり価値のあるデータの状態を表しています。 すべてのデータとメタデータはクラウドのオブジェクトストレージにあるため、複数のユーザーとアプリケーションがほぼリアルタイムでデータにアクセスでき、そのためアナリストは処理中の最新データにアクセスすることが可能です。

![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)

- **ブロンズ**テーブルには、様々なソース（JSONファイル、RDBMSデータ、IoTデータなど）から取り込まれた未加工のデータが含まれます。

- **シルバー**テーブルはデータのより洗練されたビューを提示します。 様々なブロンズテーブルのフィールドを統合することで、ストリーミングレコードをエンリッチ化させたり、または最近のアクティビティに基づいてアカウントステータスを更新したりできます。

- **ゴールド**テーブルは、レポーティングやダッシュボーディングによく使われるビジネスレベルの集約を提示します。 ここには、日々のアクティブウェブサイトユーザー、店舗ごとの週間売上、または部門別四半期ごとの売上総利益などの集約が含まれます。

最終的に出力されるのは、ビジネス指標の実用的な洞察、ダッシュボードおよびレポートです。

ETLパイプラインのすべての段階でビジネスロジックを検討することにより、不必要なデータの重複を減らし、すべての履歴データに対するアドホッククエリを制限して、ストレージとコンピュートコストを最適化します。

各段階はバッチまたはストリーミングジョブとして構成することができ、そしてACIDトランザクションのおかげで成功するか完全に失敗します。

# COMMAND ----------

# MAGIC %md <i18n value="9008b325-00b1-41a3-bc43-9c693bade882"/>
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

# MAGIC %md <i18n value="7b621659-663d-4fea-b26c-5eefdf4d025a"/>
## はじめる（Getting Started）

次のセルを実行して、ラボ環境を構成します。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-07.1

# COMMAND ----------

# MAGIC %md <i18n value="045c9907-e803-4506-8e69-4e370f06cd1d"/>
## データシミュレーター（Data Simulator）
Databricks Auto Loaderは、クラウドオブジェクトストアに到着したファイルを自動で処理します。

このプロセスをシミュレートするため、コースを通して次の操作を複数回実行するように求められます。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="d5d9393e-0a91-41f5-95f3-82f1be290add"/>
## ブロンズテーブル：未加工のJSONレコーディングを取り込む（Bronze Table: Ingesting Raw JSON Recordings）

以下ではスキーマ推論を備えたAuto Loaderを使って、未加工のJSONソースで読み取りを構成します。

Spark DataFrame APIを使って増分読み取りを設定する必要がありますが、一度構成されると、すぐにテンポラリビューを登録してデータのストリーミング変換にSpark SQLが活用できることに留意してください。

**注**：JSONデータソースでは、Auto Loaderは各列を文字列として推測するように設定しています。 ここでは、 **`cloudFiles.schemaHints`** オプションを使用して **`time`** 列のデータ型を指定する方法を示します。 フィールドに不適切な型が入力されるとNULL値になることに注意してください。

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %md <i18n value="7fdec7ea-277e-4df4-911b-b2a4d3761b6a"/>
ここではソースファイルとそれが取り込まれた時間を示す追加のメタデータを用いることで、未加工のデータをエンリッチ化します。 この追加したメタデータは、破損したデータを検出した際に発生するエラーのトラブルシューティングに有益な情報をもたらしますが、ダウンストリームの処理中には無視することができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %md <i18n value="6f60f3aa-65ff-4204-9cf8-00f456d4497b"/>
以下のコードはエンリッチ化された未加工のデータをPySpark APIに渡し、Delta Lakeテーブルへの増分書き込みを処理します。

# COMMAND ----------

(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))

# COMMAND ----------

# MAGIC %md <i18n value="6fd28dc4-1516-4f6a-8478-290d366a342c"/>
次のセルを用いて別ファイルの到着をトリガーすると、書き込んだストリーミングクエリによって変更が素早く検出されることを確認できます。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="4d7848cc-ecff-474d-be27-21717d9f08d1"/>
### 静的ルックアップテーブルをロードする（Load Static Lookup Table）
Delta LakeがデータにもたらすACID保証はテーブルレベルで管理され、完全で正常なコミットのみをテーブルに反映するようにします。 これらのデータを他のデータソースと統合する場合、それらのソースがどのようにデータをバージョン管理するのか、そしてどのような整合性がそれらを保証するのかに注目してください。

この簡略化されたデモでは、レコーディングに患者データを追加するため静的CSVファイルをロードしています。 本番環境では、Databricksの<a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a>機能を利用して、Delta Lakeにあるこれらデータの最新ビューを維持するということもできます。

# COMMAND ----------

(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.datasets}/healthcare/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md <i18n value="00ed3fc7-7c17-44f0-b56f-5e824a72bd9c"/>
## シルバーテーブル：エンリッチ化されたレコーディングデータ（Silver Table: Enriched Recording Data）
シルバーレベルの2番目のホップとして、以下のエンリッチ処理とチェックを行います。
- レコーディングデータとPIIを結合して、患者名を追加する
- レコーディングの時間を人間が読める形式 **`'yyyy-MM-dd HH:mm:ss'`** に解析する
- <= 0 の心拍数は患者の不在または送信エラーを意味するため排除する

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM bronze_tmp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))

# COMMAND ----------

# MAGIC %md <i18n value="f7f66dc8-f5b5-4682-bfb6-e97aab650874"/>
別の新しいファイルをトリガーし、前の両方のクエリを介して伝播するのを待ちます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="d6a2ecd9-043e-4488-8a70-3ee3389cf681"/>
## ゴールドテーブル：一日の平均（Gold Table: Daily Averages）

ここでは **`recordings_enriched`** からデータのストリームを読み取り別のストリームを書き込むことで、患者別平均値を表す集約ゴールドテーブルを作成します。

# COMMAND ----------

(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md <i18n value="de6370ea-e1a0-4212-98eb-53fd012e73b0"/>
以下では  **`.trigger(availableNow=True)`** を使用している点に注意してください。 これににより、すべての利用可能なデータをマイクロバッチで処理するようこの1回限りのジョブをトリガーする際、構造化ストリーミングの強みを生かし続けることが可能になります。 要約すると、その強みとは以下の通りです。
- 1回限りのエンドツーエンド・フォールトトレランス処理
- アップストリームデータソースにおける変更の自動検出

おおよそのデータ増加率が分かっていれば、このジョブに割り当てるクラスタのサイズを適切に選び、迅速で費用効果の高い処理を確保できます。 カスタマーは、データの最終的な集約ビューを更新するのに掛かるコストを評価し、十分な情報を得た上でこのオペレーションを実行する頻度を決定できます。

このテーブルにサブスクライブしているダウンストリーム処理には、高価な集約を再実行する必要はありません。 むしろファイルを逆シリアル化するだけで、この既に集約されたソースに対し、含まれるフィールドに基づいたクエリが素早くプッシュダウンされます。

# COMMAND ----------

(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))

# COMMAND ----------

# MAGIC %md <i18n value="5ffbd353-850f-431e-8455-827f87cad2ca"/>
#### Deltaを使った完全な出力に関する重要な考察（Important Considerations for complete Output with Delta）

 **`complete`** の出力モードを使用すると、ロジックを実行する度にテーブルの状態全体が書き換えられます。 これは集約を計算するためには理想ですが、構造化ストリーミングがデータはアップストリームロジックにのみ追加されると想定しているため、このディレクトリからストリームを読み取ることは**できません**。

**注**：この行動を変えるために特定のオプションを設定することができますが、その他の制限が追加されてしまいます。 詳細を知りたい場合は、<a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Deltaストリーミング：更新と削除を無視する</a>を参照してください。

登録したばかりのゴールドDeltaテーブルは、次のクエリを実行する度にデータの現状に関する静的読み取りを行います。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %md <i18n value="bcca7247-9716-44ed-8424-e72170f0a2dc"/>
上記のテーブルには、全ユーザーの全日程が含まれていることに注意してください。 アドホッククエリの述語がここでエンコードされたデータと一致する場合、ソースにあるファイルへ述語をプッシュダウンして、より限定された集約ビューを迅速に生成できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md <i18n value="0822f785-38af-4b30-9154-8d82eb9fe000"/>
## 残りのレコードを処理する（Process Remaining Records）
次のセルは、2020年の残り期間の追加ファイルをソースディレクトリに配置します。 これらの処理はDelta Lakeの最初の3つのテーブルを通して確認できますが、最後のクエリを再実行して **`daily_patient_avg`** テーブルを更新する必要があります。なぜなら、このクエリはtrigger available now構文を使っているからです。

# COMMAND ----------

DA.data_factory.load(continuous=True)

# COMMAND ----------

# MAGIC %md <i18n value="f1f576bc-2b5d-46cf-9acb-6c7c4807c1af"/>
## まとめ（Wrapping Up）

最後に、すべてのストリームが停止していることを確認してください。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md <i18n value="82928cc5-5e2b-4368-90bd-dff62a27ff12"/>
## 概要（Summary）

Delta Lakeと構造化ストリーミングが組み合わさることで、レイクハウスのデータをほぼリアルタイムでアクセスして分析します。

# COMMAND ----------

# MAGIC %md <i18n value="e60b0dac-92ed-4480-a969-d0568ce83494"/>
## 追加のトピックとリソース（Additional Topics & Resources）

* <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">テーブルストリーミングの読み取りおよび書き込み</a>
* <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">構造化ストリーミングのプログラミングガイド</a>
* 『<a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">構造化ストリーミングを深く掘り下げる</a>』著者：Tathagata Das。 これは構造化ストリーミングがどのように機能するかを説明する非常に優れた動画です。
* <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">ラムダアーキテクチャ</a>
* <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">データウェアハウスモデル</a>
* <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Kafkaソースストリームを作成する</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
