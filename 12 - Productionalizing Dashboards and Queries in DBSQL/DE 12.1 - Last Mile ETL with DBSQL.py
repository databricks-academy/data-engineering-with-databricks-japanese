# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="85ab95fa-7d86-4b8b-9c7e-9f9b82dd637a"/>
# MAGIC 
# MAGIC # Databricks SQLを使用したラストワンマイルETL（Last Mile ETL with Databricks SQL）
# MAGIC 
# MAGIC 続ける前に、これまでに学習した内容を少しおさらいしておきましょう。
# MAGIC 1. Databricksワークスペースには、データエンジニアリングの開発ライフサイクルを簡素化するために役立つツール群が含まれています。
# MAGIC 1. Databricksノートブックにより、ユーザーはSQLと他のプログラミング言語を組み合わせてETLワークロードを定義することができます。
# MAGIC 1. Delta LakeはACIDに準拠したトランザクションを提供し、レイクハウスで簡単に増分データの処理を行うことができます。
# MAGIC 1. Delta Live TablesはSQL構文を拡張し、レイクハウスの数多くのデザインパターンをサポートしたり、インフラの展開を簡素化したりします。
# MAGIC 1. マルチタスクジョブによって完全なタスクオーケストレーションが可能となり、ノートブックとDLTパイプラインを組み合わせてスケジューリングを行いながら依存関係を追加することができます。
# MAGIC 1. Databricks SQLでは、SQLクエリの編集と実行、ビジュアライゼーションの作成、ダッシュボードの定義などが可能です。
# MAGIC 1. Data ExplorerはテーブルACLの管理を簡素化し、レイクハウスのデータをSQLアナリストが利用できるようにします。
# MAGIC 
# MAGIC このセクションでは、本番環境のワークロードをサポートするためにより多くのDBSQLの機能を説明することに焦点を当てます。
# MAGIC 
# MAGIC まず、Databricks SQLを活用した分析のためのラストワンマイルETLをサポートするクエリの構成に焦点を当てます。 このデモではDatabricks SQL UIを使用しますが、SQLウェアハウスは<a href="https://docs.databricks.com/integrations/partners.html" target="_blank">他の多くのツールと統合して外部クエリの実行を可能にし</a>、<a href="https://docs.databricks.com/sql/api/index.html" target="_blank">プログラムを使用して任意のクエリを実行するフルAPIサポート</a>を備えていることにご注意ください。
# MAGIC 
# MAGIC これらのクエリ結果から一連のビジュアライゼーションを生成し、ダッシュボードにまとめていきます。
# MAGIC 
# MAGIC 最後に、クエリやダッシュボードの更新をスケジュールし、アラートを設定することで本番用データセットの状態の時系列に沿った監視をサポートする方法について説明します。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * 分析ワークロードを支える本番環境のETLタスクをサポートするツールとしてDatabricks SQLを使用する
# MAGIC * Databricks SQLエディタを使用してSQLクエリおよびビジュアライゼーションを構成する
# MAGIC * Databricks SQLでダッシュボードを作成する
# MAGIC * クエリやダッシュボードの更新をスケジュールする
# MAGIC * SQLクエリのアラートを設定する

# COMMAND ----------

# MAGIC %md <i18n value="d6f21ada-50df-44ac-8551-72eca61d5af7"/>
# MAGIC 
# MAGIC ## セットアップスクリプトの実行（Run Setup Script）
# MAGIC 次のセルでは、SQLクエリを生成するために使用するクラスを定義したノートブックを実行します。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-12.1

# COMMAND ----------

# MAGIC %md <i18n value="5c6209ed-ee86-40bb-bbf9-8d9a8663f21c"/>
# MAGIC 
# MAGIC ## デモデータベースの作成（Create a Demo Database）
# MAGIC 次のセルを実行し、その結果をDatabricks SQL Editorへとコピーします。
# MAGIC 
# MAGIC これらのクエリでは、以下の操作が実行されます。
# MAGIC * 新規データベースの作成
# MAGIC * 2つのテーブルの宣言（これらはデータの読み込みに使用します）
# MAGIC * 2つの関数の宣言（これらはデータの生成に使用します）
# MAGIC 
# MAGIC コピーしたら、**実行**ボタンでクエリを実行します。

# COMMAND ----------

DA.generate_config()

# COMMAND ----------

# MAGIC %md <i18n value="d6a42a10-ba37-431d-82e2-f41f1d196e12"/>
# MAGIC 
# MAGIC **注**：上記のクエリは、環境を再構成するために、デモを完全にリセットした後に一度だけ実行するものです。 ユーザーは、これらのクエリを実行するためにカタログ上で **`CREATE`** および **`USAGE`** 権限を持っている必要があります。

# COMMAND ----------

# MAGIC %md <i18n value="8ee7715b-c65f-47d4-9109-f57438bed8a8"/>
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" /> **警告：**  **`USE`** 文はクエリを実行するデータベースをまだ変更しないため、<br/>先に進む前にデータベースを必ず選択しておいてください。

# COMMAND ----------

# MAGIC %md <i18n value="eba9ff4a-b242-4cd5-82a6-d004a5dacd8f"/>
# MAGIC 
# MAGIC ## クエリを作成してデータを読み込む（Create a Query to Load Data）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 次のセルを実行すると、前の手順で作成した **`user_ping`** テーブルにデータを読み込むためにフォーマットされたSQLクエリが出力されます。
# MAGIC 1. このクエリを**Load Ping Data**という名前で保存します。
# MAGIC 1. このクエリを実行して、データのバッチを読み込みます。

# COMMAND ----------

DA.generate_load()

# COMMAND ----------

# MAGIC %md <i18n value="f417b0ba-5c46-4f3a-8392-fe4cf2157e81"/>
# MAGIC 
# MAGIC クエリを実行すると、いくつかのデータが読み込まれ、テーブル内にあるデータのプレビューが返されるはずです。
# MAGIC 
# MAGIC **注**：データの定義と読み込みには乱数が使用されているため、ユーザーごとに若干異なる値が割り振られます。

# COMMAND ----------

# MAGIC %md <i18n value="030532dc-d29d-4974-99a0-4485467d9135"/>
# MAGIC 
# MAGIC ## クエリの更新スケジュールを設定する（Set a Query Refresh Schedule）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. SQLクエリエディターボックスの右上にある**スケジュール**をクリックします
# MAGIC 1. ドロップダウンを使用し、更新頻度を**1 week**、時刻を**12:00**へ変更します。
# MAGIC 1. 明日の曜日を選択します
# MAGIC 1. **OK**をクリックします
# MAGIC **注:** クラスの目的で1週間の更新スケジュールを使用していますが、1分ごとに更新するスケジュールなど、本番環境ではより短いトリガー間隔が設定する場合があります。

# COMMAND ----------

# MAGIC %md <i18n value="e2e67230-afa7-4bbc-8506-bf226b5f6848"/>
# MAGIC 
# MAGIC ## レコードの総数を追跡するクエリの作成（Create a Query to Track Total Records）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 以下のセルを実行します。
# MAGIC 1. このクエリを**User Counts**という名前で保存します。
# MAGIC 1. クエリを実行し、現在の結果を計算します。

# COMMAND ----------

DA.generate_user_counts()

# COMMAND ----------

# MAGIC %md <i18n value="59774aed-7953-4c3e-82c0-eada95504895"/>
# MAGIC 
# MAGIC ## 棒グラフのビジュアライゼーションの作成（Create a Bar Graph Visualization）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. クエリウィンドウの右下隅にある [スケジュールを更新] ボタンの下の**ビジュアライゼーションを追加**ボタンをクリックします
# MAGIC 1. 名前（デフォルトの状態は **`Visualization 1`** などになっています）をクリックし、名前を**Total User Records**へと変更します
# MAGIC 1. **X列**に **`user_id`** を設定します
# MAGIC 1. **Y列**に **`total_records`** を設定します
# MAGIC 1. **保存**をクリックします

# COMMAND ----------

# MAGIC %md <i18n value="c2eb4e36-df48-4137-94e5-6ae708ccef96"/>
# MAGIC 
# MAGIC ## 新しいダッシュボードの作成（Create a New Dashboard）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の一番下にある縦にドットが3つ並んだボタンをクリックし、**ダッシュボードに追加**を選択します
# MAGIC 1. **新規ダッシュボードを作成**オプションをクリックします
# MAGIC 1. ダッシュボードに<strong>User Ping Summary  **`<your_initials_here>`** </strong>という名前を付けます
# MAGIC 1. **保存**をクリックして新しいダッシュボードを作成します
# MAGIC 1. 新しく作成したダッシュボードが対象として選択されているはずですので、**OK**をクリックしてビジュアライゼーションを追加します

# COMMAND ----------

# MAGIC %md <i18n value="84ba9714-dcc2-4c06-803a-f566ad868c39"/>
# MAGIC 
# MAGIC ## 最近のPingの平均時間を計算するクエリの作成（Create a Query to Calculate the Recent Average Ping）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 次のセルを実行すると、フォーマットされたSQLクエリが出力されます。
# MAGIC 1. このクエリを**Avg Ping**という名前で保存します。
# MAGIC 1. クエリを実行し、現在の結果を計算します。

# COMMAND ----------

DA.generate_avg_ping()

# COMMAND ----------

# MAGIC %md <i18n value="2b7bc15b-cead-4e5f-b36a-a635597c5358"/>
# MAGIC 
# MAGIC ## ダッシュボードへのラインプロットビジュアライゼーションの追加（Add a Line Plot Visualization to your Dashboard）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. **ビジュアライゼーションの追加**ボタンをクリックします
# MAGIC 1. 名前（デフォルトの状態は **`Visualization 1`** などになっています）をクリックし、名前を**Avg User Ping**へと変更します
# MAGIC 1. **Visualization Type**に **`Line`** を選択します。
# MAGIC 1. **X列**に **`end_time`** を設定します。
# MAGIC 1. **Y列**に **`avg_ping`** を設定します。
# MAGIC 1. **Group by**に **`user_id`** を設定します。
# MAGIC 1. **保存**をクリックします
# MAGIC 1. 画面の一番下にある縦にドットが3つ並んだボタンをクリックし、**ダッシュボードに追加**を選択します
# MAGIC 1. 先ほど作成したダッシュボードを選択します
# MAGIC 1. **OK**をクリックしてビジュアライゼーションを追加します

# COMMAND ----------

# MAGIC %md <i18n value="99c42ddb-7993-4c6f-a3cf-842be65b02ed"/>
# MAGIC 
# MAGIC ## 統計情報の概要を報告するクエリの作成（Create a Query to Report Summary Statistics）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 以下のセルを実行します。
# MAGIC 1. このクエリを**Ping Summary**という名前で保存します。
# MAGIC 1. クエリを実行し、現在の結果を計算します。

# COMMAND ----------

DA.generate_summary()

# COMMAND ----------

# MAGIC %md <i18n value="353d04dd-997d-44b0-84f8-8352dcabdc53"/>
# MAGIC 
# MAGIC ## ダッシュボードに概要テーブルを追加する（Add the Summary Table to your Dashboard）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の一番下にある縦にドットが3つ並んだボタンをクリックし、**ダッシュボードに追加**を選択します
# MAGIC 1. 先ほど作成したダッシュボードを選択します
# MAGIC 1. **OK**をクリックしてビジュアライゼーションを追加します

# COMMAND ----------

# MAGIC %md <i18n value="87c5be59-847a-4e0d-b608-ad50f5f9415a"/>
# MAGIC 
# MAGIC ## ダッシュボードを確認して更新する（Review and Refresh your Dashboard）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 左側のサイドバーを使用して、**ダッシュボード**に移動します
# MAGIC 1. クエリを追加したダッシュボードを見つけます
# MAGIC 1. 青色の**更新**ボタンをクリックしてダッシュボードを更新します
# MAGIC 1. **スケジュール**ボタンをクリックしてダッシュボードのスケジュール設定オプションを確認します
# MAGIC   * ダッシュボードの更新をスケジュール設定すると、そのダッシュボードに関連付けられているすべてのクエリが実行されますのでご注意ください。
# MAGIC   * この時点ではダッシュボードのスケジュール設定を行わないでください

# COMMAND ----------

# MAGIC %md <i18n value="9ea31415-a5e2-445c-9e1d-46f0aab374a6"/>
# MAGIC 
# MAGIC ## ダッシュボードの共有（Share your Dashboard）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 青色の**Share**ボタンをクリックします
# MAGIC 1. 一番上のフィールドから**All Users**を選択します
# MAGIC 1. 右側のフィールドから**編集可能**を選択します
# MAGIC 1. **追加**をクリックします
# MAGIC 1. **資格情報**を**閲覧者として実行**に変更します
# MAGIC 
# MAGIC **注**：テーブルACLを使用して元となっているデータベースおよびテーブルに権限が付与されていないため、現時点ではダッシュボードを実行するための権限を持っている他のユーザーはいないはずです。 他のユーザーがダッシュボードの更新をトリガーできるようにするには、**所有者として実行**の権限を対象のユーザーに付与するか、クエリで参照しているテーブルの権限を追加する必要があります。

# COMMAND ----------

# MAGIC %md <i18n value="b5146776-0448-4f5d-a72c-e83b39ff4b98"/>
# MAGIC 
# MAGIC ## アラートを設定する（Set Up an Alert）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 左側のサイドバーを使用して、**アラート**に移動します
# MAGIC 1. 右上にある**アラートを作成**をクリックします
# MAGIC 1. **User Counts**クエリを選択します
# MAGIC 1. 画面の左上にあるフィールドをクリックし、アラートに **`<your_initials>Count Check`** という名前を付けます
# MAGIC 1. **トリガー条件**オプションを、次のように構成します。
# MAGIC   * **値列**： **`total_records`** 
# MAGIC   * **条件**： **`>`** 
# MAGIC   * **しきい値**： **`15`** 
# MAGIC 1. **リフレッシュ**で、**なし**を選択します
# MAGIC 1. **Create Alert**をクリックします
# MAGIC 1. 次の画面で、右上にある青色の**更新**をクリックし、アラートを評価します

# COMMAND ----------

# MAGIC %md <i18n value="08e96878-726f-44b7-8bfb-7effd43bbee3"/>
# MAGIC 
# MAGIC ## アラートの送信先オプションを確認する（Review Alert Destination Options）
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. アラートのプレビューから、画面の右側にある**送信先**の右にある青色の **追加**ボタンをクリックします
# MAGIC 1. 表示されたウィンドウの一番下にある**アラート送信先に新規送信先を作成する**というメッセージの中にある青いテキストを探してクリックします
# MAGIC 1. 利用可能なアラートオプションを確認します

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
