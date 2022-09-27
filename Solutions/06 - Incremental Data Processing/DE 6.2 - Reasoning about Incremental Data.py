# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="988e4844-2372-4e5c-b603-bf59f9c0c638"/>
# MAGIC 
# MAGIC # 増分データについて推論する（Reasoning about Incremental Data）
# MAGIC 
# MAGIC Spark構造化ストリーミングは、Apache Sparkの機能を拡張して増分データセットを処理する際の構成と簿記を簡略化します。 これまでのビッグデータを用いたストリーミングでは、レイテンシを短縮してほぼリアルタイム分析に近いインサイトを提供することに重きを置いてきました。 構造化ストリーミングはこれらの目的を達成する上で非常に優れたパフォーマンスを発揮しますが、このレッスンではまず増分データ処理のアプリケーションに焦点を当てていきます。
# MAGIC 
# MAGIC 増分処理はデータレイクハウスでの正常な機能のためには絶対に必要なものではありませんが、世界最大の企業が世界最大規模のデータセットからインサイトを導き出すサポートをした我々の経験から、多くのワークロードは増分処理のアプローチを採用することで実質的な利益が得られるとの結論に至りました。 Databricksの核心であるコア機能の多くは、特にこれらの増加し続けるデータセットに対応するために最適化されています。
# MAGIC 
# MAGIC 次のデータセットとユースケースを考えてみましょう。
# MAGIC * データサイエンティストが、運用データベースで頻繁に更新されるレコードへの安全で匿名化されたバージョン管理アクセスを必要としている
# MAGIC * クレジットカードトランザクションでは、顧客の過去の行動と照らし合わせて不正を特定しフラグを立てる必要がある
# MAGIC * 多国籍小売業者が、購入履歴を利用してカスタム製品をおすすめしようとしている
# MAGIC * 不安定性を検知して対応するため、分散システムからのログファイルを分析する必要がある
# MAGIC * 何百万ものオンラインショッパーから得たクリックストリームデータを、UXのA/Bテストに活用しなければならない
# MAGIC 
# MAGIC 上記は、時間の経過とともに段階的かつ無限に増加するデータセットの一例にすぎません。
# MAGIC 
# MAGIC このレッスンでは、データの増分処理を可能にするSpark構造化ストリーミングの基本操作について学びます。 その次のレッスンでは、この増分処理モデルがどのようにデータレイクハウスのデータ処理を簡潔化させていのるか詳しく説明します。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * Spark構造化ストリーミングで使用されるプログラミングモデルを説明する
# MAGIC * ソースでのストリーミング書き込みを実行するために必要なオプションを構成する
# MAGIC * エンドツーエンドのフォールトトレランスの要件を説明する
# MAGIC * シンクへのストリーミング書き込みを実行するために必要なオプションを構成する

# COMMAND ----------

# MAGIC %md <i18n value="203d2cf2-e366-4b68-8e45-e17186a9503b"/>
# MAGIC 
# MAGIC ## はじめる（Getting Started）
# MAGIC 
# MAGIC 次のセルを実行して「クラスルーム」を構成します。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-06.2

# COMMAND ----------

# MAGIC %md <i18n value="bb701ed4-32db-4e97-bd72-0b90acaeca16"/>
# MAGIC 
# MAGIC ## 無限データをテーブルとして扱う（Treating Infinite Data as a Table）
# MAGIC 
# MAGIC Spark構造化ストリーミングの魅力は、増え続けるデータソースをまるでレコードの静的テーブルかのように扱える点にあります。
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png" width="800" />
# MAGIC 
# MAGIC 上記の図にある**date stream**は、時間とともに増加するデータソースを表しています。 データストリームの新しいデータに該当するものには、以下のようなものがあります。
# MAGIC * クラウドストレージに到着した新しいJSONログライフ
# MAGIC * CDCフィードでキャプチャされたデータベースの更新
# MAGIC * pub/subメッセージングフィードでキューされたイベント
# MAGIC * 前日に終了した売上のCSVファイル
# MAGIC 
# MAGIC 従来多くの組織では、結果を更新する度にソースデータセット全体を再処理するアプローチを取ってきました。 もう一つのアプローチは、最後に更新した後に追加されたファイルまたはレコードのみをキャプチャするカスタムロジックを書くことです。
# MAGIC 
# MAGIC 構造化ストリーミングを利用すると、データソースに対してクエリを定義して新しいレコードを自動的に検出し、以前定義したロジックを通してそのレコードを伝播することができます。
# MAGIC 
# MAGIC **Spark構造化ストリーミングは、Delta LakeおよびAuto Loaderと密接に統合できるようにDatabricksで最適化されています。**

# COMMAND ----------

# MAGIC %md <i18n value="08b87ec8-7f2b-4638-a93f-c6cf132ab1f0"/>
# MAGIC 
# MAGIC ## 基本概念（Basic Concepts）
# MAGIC 
# MAGIC - 開発者は**ソース**に対してストリーミング読み取りを構成することにより、**入力テーブル**を定義します。 これを行う構文は静的データの操作に似ています。
# MAGIC - **クエリ**が入力テーブルに対して定義されます。 データフレームAPIとSpark SQLの両方を使用して、入力テーブルに対する変換とアクションを簡単に定義できます。
# MAGIC - この入力テーブルに対する論理クエリは、**結果テーブル**を生成します。 結果テーブルには、ストリームに関する増分の情報が含まれます。
# MAGIC - ストリーミングパイプラインの**出力**は、外部**シンク**に書き込むことで結果テーブルの更新を維持します。 一般的に、シンクはファイルやpub/subメッセージングバスなど耐久性のあるシステムになります。
# MAGIC - **トリガー間隔**ごとに新しい行が入力テーブルに追加されます。 これらの新しい行は基本的にマイクロバッチトランザクションに類似していて、結果テーブルを通してシンクへ自動的に伝播されます。
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png" width="800" />
# MAGIC 
# MAGIC 
# MAGIC より詳しく知りたい場合は、<a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts" target="_blank">構造化ストリーミングのプログラミングガイド</a>（ここからいくつかの図を引用）にある類似セクションをご覧ください。

# COMMAND ----------

# MAGIC %md <i18n value="c0f129fa-daf9-4d01-8454-fa040442457f"/>
# MAGIC 
# MAGIC ## エンドツーエンドのフォールトトレランス
# MAGIC 
# MAGIC 構造化ストリーミングは、_チェックポインティング_（下で説明）と<a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">ログ先行書き込み</a>によって、エンドツーエンドの一回限りのフォールトトレランスを保証します。
# MAGIC 
# MAGIC 構造化ストリーミングソースとシンク、基盤となる実行エンジンが連携することでストリーム処理の進行状況を追跡します。 障害が発生した場合は、ストリーミングエンジンは再起動および/またはデータの再処理を試みます。 失敗したストリーミングクエリから回復する最善の方法は、<a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-from-query-failures" target="_blank">ドキュメント</a>を参照してください。
# MAGIC 
# MAGIC このアプローチは、ストリーミングソースが再生可能な状態の時_のみ_有効です。再生可能なソースには、クラウドベースのオブジェクトストレージとpub/subメッセージングサービスが含まれます。
# MAGIC 
# MAGIC 大まかにいうと、基盤となるストリーミングメカニズムは2つのアプローチに依存しています。
# MAGIC 
# MAGIC * まず、構造化ストリーミングはチェックポインティングやログ先行書き込みを使って、各トリガー間隔で処理されるデータのオフセット範囲を記録します。
# MAGIC * 次に、ストリーミングシンクは_べき等_であるように設計されています。つまり、同じデータにおける複数の書き込み（オフセットで特定）が起きても、シンクへの書き込みが重複されることは_ありません_。
# MAGIC 
# MAGIC まとめると、再生可能なデータソースとべき等性を持つシンクにより、構造化ストリーミングはどのような障害下でも**エンドツーエンドの一回限りのセマンティック**を保証します。

# COMMAND ----------

# MAGIC %md <i18n value="57f9dadd-71ff-41cd-bf26-a3b968e68fba"/>
# MAGIC 
# MAGIC ## ストリームを読み取る（Reading a Stream）
# MAGIC 
# MAGIC  **`spark.readStream()`** メソッドは、ストリームの構成とクエリに使われる **`DataStreamReader`** を返します。
# MAGIC 
# MAGIC 前のレッスンでは、Auto Loaderを使って段階的に読み取るために構成したコードを見てきました。 ここからは、Delta Lakeテーブルを段階的に読み取ることがいかに簡単かを説明します。
# MAGIC 
# MAGIC このコードはPySpark APIを使用して、 **`bronze`** というDelta Lakeテーブルを段階的に読み取り、 **`streaming_tmp_vw`** というストリーミングのテンポラリビューを登録します。
# MAGIC 
# MAGIC **注**：段階的な読み取りを構成する際には、数多くのオプション構成（ここでは示しません）を設定することができます。中でも最も重要なオプションは、<a href="https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate" target="_blank">入力レートを制限できます</a>。

# COMMAND ----------

(spark.readStream
    .table("bronze")
    .createOrReplaceTempView("streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %md <i18n value="4fadee61-b02e-4601-a3b8-26c2c9f06160"/>
# MAGIC 
# MAGIC ストリーミングテンポラリビューのクエリを実行すると、ソースに新しいデータが到着する度にクエリの結果が更新されます。
# MAGIC 
# MAGIC ストリーミングのテンポラリビューに対して実行されるクエリは、**増分クエリが常時有効**になっていると考えてください。
# MAGIC 
# MAGIC **注**：一般的には、開発やライブダッシュボーディング中に人が積極的にクエリの出力をモニターしない限り、ストリーミング結果をノートブックに返すことはありません。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_tmp_vw

# COMMAND ----------

# MAGIC %md <i18n value="147067e0-bb54-4556-849f-00b9710e7c48"/>
# MAGIC 
# MAGIC このデータは、前回のレッスンで書き出したDeltaテーブルと同じだということが分かるでしょう。
# MAGIC 
# MAGIC 続ける前に、ノートブック上部にある **`Stop Execution`** かセルのすぐ下にある **`Cancel`** をクリックするか、または次のセルを実行してすべてのアクティブなストリーミングクエリを停止してください。

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md <i18n value="2daca6ba-fca5-40d9-ba5f-b97cf609b20e"/>
# MAGIC 
# MAGIC ## ストリーミングデータの操作（Working with Streaming Data）
# MAGIC 静的データと同じように、ストリーミングのテンポラリビューに対するほとんどの変換を実行できます。 ここでは単純な集約を実行して、 **`device_id`** ごとのレコードの数を取得します。
# MAGIC 
# MAGIC ストリーミングのテンポラリビューを照会しているため、これは単一セットの結果を取得した後に終了しない、無期限に実行されるストリーミングクエリになります。 このようなストリーミングクエリでは、Databricksのノードブックにはストリーミングのパフォーマンスを監視するインタラクティブなダッシュボードが含まれます。 下で詳しく見ていきましょう。
# MAGIC 
# MAGIC この例に関する重要事項：これはストリームから見た入力の集約を表示しているだけです。 **これらのレコードはすべて、現時点ではどこにも保持されていません。**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(device_id) AS total_recordings
# MAGIC FROM streaming_tmp_vw
# MAGIC GROUP BY device_id

# COMMAND ----------

# MAGIC %md <i18n value="99d062fc-28c0-4b88-955e-f7d6e80ee472"/>
# MAGIC 
# MAGIC 続ける前に、ノートブック上部にある **`Stop Execution`** かセルのすぐ下にある **`Cancel`** をクリックするか、または次のセルを実行してすべてのアクティブなストリーミングクエリを停止してください。

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md <i18n value="c257cb96-badc-473c-873a-0fb4ced7c825"/>
# MAGIC 
# MAGIC ## サポートされていないオペレーション（Unsupported Operations）
# MAGIC 
# MAGIC ストリーミングデータフレームにおけるほとんどの操作は、静的データフレームと同じです。 しかし、いくつか<a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">例外</a>もあります。
# MAGIC 
# MAGIC データのモデルを、常に追加し続けるテーブルと捉えてください。 並べ替えは、ストリーミングデータを扱う際には、複雑すぎるかまたは論理的な理由で実行できない数少ない操作の1つです。
# MAGIC 
# MAGIC これらの例外についてすべて述べると、コースの範囲を超えてしまいます。 ウィンドウ処理やウォーターマーキングなどの高度なストリーミング方法を用いることで、増分ワークロードに機能を追加できることに注意しましょう。
# MAGIC 
# MAGIC コメントアウトを外して次のセルを実行し、この障害が起きた時の様子を見てみましょう。

# COMMAND ----------

# %sql
# SELECT * 
# FROM streaming_tmp_vw
# ORDER BY time

# COMMAND ----------

# MAGIC %md <i18n value="fbde5075-43da-4c12-882c-e6da63bdb026"/>
# MAGIC 
# MAGIC ## ストリーミング結果の永続化（Persisting Streaming Results）
# MAGIC 
# MAGIC 増分結果を永続化するには、ロジックをPySparkの構造化ストリーミングデータフレームAPIに渡す必要があります。
# MAGIC 
# MAGIC 上記では、PySparkストリーミングDataFrameからテンポラリビューを作成しました。 ストリーミングの一時ビューに対するクエリの結果から他の一時ビューを作成すると、再びストリーミングのテンポラリビューが作られます。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts_tmp_vw AS (
# MAGIC   SELECT device_id, COUNT(device_id) AS total_recordings
# MAGIC   FROM streaming_tmp_vw
# MAGIC   GROUP BY device_id
# MAGIC )

# COMMAND ----------

# MAGIC %md <i18n value="db473405-f8b5-4fc7-9f87-c669b3d8e7d8"/>
# MAGIC 
# MAGIC ## ストリームを書き込む（Writing a Stream）
# MAGIC 
# MAGIC ストリーミングクエリの結果を永続化するには、それらを耐久性に優れたストレージに書き出す必要があります。  **`DataFrame.writeStream`** メソッドは、出力を構成する際に使われる **`DataStreamWriter`** を返します。
# MAGIC 
# MAGIC Delta Lakeテーブルに書き込む際は、通常ここで示す3つの設定に注意する必要があります。
# MAGIC 
# MAGIC ### チェックポインティング（Checkpointing）
# MAGIC 
# MAGIC Databricksは、ストリーミングジョブの現状をクラウドストレージに保存することでチェックポイントを作ります。
# MAGIC 
# MAGIC チェックポインティングはログ先行書き込みと組み合わることで、終了したストリームを中断した箇所から再開して続けることができます。
# MAGIC 
# MAGIC チェックポイントは、異なるストリーム間で共有することはできません。 処理の保証を確実にするため、すべてのストリーミング書き込みにはチェックポイントが必要です。
# MAGIC 
# MAGIC ### 出力モード（Output Modes）
# MAGIC 
# MAGIC ストリーミングジョブには、静的またはバッチワークロードに似た出力モードがあります。 <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes" target="_blank">詳細はこちら</a>。
# MAGIC 
# MAGIC | モード              | 例                             | メモ                                                     |
# MAGIC | ---------------- | ----------------------------- | ------------------------------------------------------ |
# MAGIC | **Append（追加）**   |  **`.outputMode("append")`**    | **これがデフォルトです。**新しく追加された行だけが、バッチごとにターゲットテーブルへ段階的に追加されます |
# MAGIC | **Complete（完全）** |  **`.outputMode("complete")`**  | 書き込みがトリガーされる度に、結果テーブルが再計算されます。ターゲットテーブルはバッチごとに上書きされます  |
# MAGIC 
# MAGIC 
# MAGIC ### トリガー間隔
# MAGIC 
# MAGIC ストリーミング書き込みを定義すると際、 **`trigger`** メソッドはシステムが次のデータセットを処理するタイミングを指定します。
# MAGIC 
# MAGIC 
# MAGIC | トリガーの種類      | 例                                          | 動作                                                       |
# MAGIC | ------------ | ------------------------------------------ | -------------------------------------------------------- |
# MAGIC | 指定なし         |                                            | **これがデフォルトです。** これは **`processingTime="500ms"`** を使うのと同様です |
# MAGIC | 固定間隔のマイクロバッチ |  **`.trigger(processingTime="2 minutes")`**  | クエリはマイクロバッチで実行され、ユーザーが指定した間隔で開始されます                      |
# MAGIC | ワンタイムマイクロバッチ |  **`.trigger(once=True)`**                   | クエリは単一のマイクロバッチを実行し、すべての利用可能なデータを処理してから停止します              |
# MAGIC | トリガー型マイクロバッチ |  **`.trigger(availableNow=True)`**           | クエリは複数のマイクロバッチを実行し、すべての利用可能なデータを処理してから停止します              |
# MAGIC 
# MAGIC どのようにデータがシンクに書き込まれマイクロバッチの頻度を制御するか定義する際に、トリガーが指定されます。 デフォルトでは、Sparkは最後のトリガー以降に追加されたソースにあるすべてのデータを自動で検出して処理します。
# MAGIC 
# MAGIC **注意：**  **`Trigger.AvailableNow`** </a>は、DBR 10.1ではScalaでのみ、DBR 10.2以降ではPythonとScalaで使用できる新しいトリガーの種類です。

# COMMAND ----------

# MAGIC %md <i18n value="a8df4901-05f0-41cd-944d-935c21fd40fc"/>
# MAGIC 
# MAGIC ## まとめ（Pulling It All Together）
# MAGIC 
# MAGIC 以下のコードは、  **`spark.table()`** を使ってデータをストリーミングのテンポラリビューからロードしてデDataFrameに戻す方法を示しています。 Sparkは常にストリーミングビューをストリーミングDataFrameとして、静的ビューを静的DataFramesとしてロードすることに注意してください (つまり、増分処理は増分書き込みをサポートするために読み取りロジックで定義されなければなりません)。
# MAGIC 
# MAGIC この最初のクエリでは、増分バッチ処理を実行するために **`trigger(availableNow=True)`** の使い方を示します。

# COMMAND ----------

(spark.table("device_counts_tmp_vw")                               
    .writeStream                                                
    .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
    .outputMode("complete")
    .trigger(availableNow=True)
    .table("device_counts")
    .awaitTermination() # This optional method blocks execution of the next cell until the incremental batch write has succeeded
)

# COMMAND ----------

# MAGIC %md <i18n value="0e13fc25-53ff-4c40-a93b-e3c3c5dc2644"/>
# MAGIC 
# MAGIC 以下ではトリガー方法を変えて、トリガーされた増分バッチから4秒ごとにトリガーされる常時有効のクエリに変更します。
# MAGIC 
# MAGIC **注**：このクエリを開始する時はソーステーブルに新しいレコードは存在していません。 新しいデータはこの後すぐに追加します。

# COMMAND ----------

query = (spark.table("device_counts_tmp_vw")                               
              .writeStream                                                
              .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
              .outputMode("complete")
              .trigger(processingTime='4 seconds')
              .table("device_counts"))

# Like before, wait until our stream has processed some data
DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md <i18n value="d42a3739-71d7-4b9c-a1d2-a32feea1228f"/>
# MAGIC 
# MAGIC ## 出力を照会する（Querying the Output）
# MAGIC それでは、SQLから書き込んだ出力を照会しましょう。 結果はテーブルであるため、結果を戻すにはデータを逆シリアル化するだけで良いです。
# MAGIC 
# MAGIC 今はテーブル（ストリーミングDataFrameではない）を照会しているため、下記はストリーミングクエリでは**ありません**。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

# COMMAND ----------

# MAGIC %md <i18n value="a7d8882f-825c-4b16-a788-6a30f8696cf7"/>
# MAGIC 
# MAGIC ## 新しいデータの配置（Land New Data）
# MAGIC 
# MAGIC 前のレッスンと同じように、ヘルパー関数を構成して新しいレコードをソーステーブルに処理しました。
# MAGIC 
# MAGIC 下のセルを実行して、データの他のバッチを配置します。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="9e62e0fc-7705-4ee4-8407-07cc908cbfdc"/>
# MAGIC 
# MAGIC ターゲットテーブルを再度クエリして、  **`device_id`** ごとに更新された数を確認します。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

# COMMAND ----------

# MAGIC %md <i18n value="9d17fa00-4a3f-48a0-a15e-8ac39df11550"/>
# MAGIC 
# MAGIC ## クリーンアップ（Clean up）
# MAGIC 引き続き、上記のセルを使って新しいデータを配置し、テーブル結果を探ってみましょう。
# MAGIC 
# MAGIC 終了したら、続行する前に次のセルを実行してすべてのアクティブなストリームを停止し、生成したリソースを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
