# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="51f698bd-880b-4a85-b187-9b96d8c2cf18"/>
# MAGIC 
# MAGIC # ラボ：Databricks使用したジョブのオーケストレーション（Lab: Orchestrating Jobs with Databricks）
# MAGIC 
# MAGIC このラボでは次のものからなるマルチタスクジョブを構成します：
# MAGIC * ストレージディレクトリに新しいデータバッチを配置するノートブック
# MAGIC * 複数のテーブルを通してこのデータを処理するDelta Live Tablesのパイプライン
# MAGIC * このパイプラインによって作成されたゴールドテーブルおよびDLTによるさまざまメトリックの出力を照会するノートブック
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このラボでは、以下のことが学べます。
# MAGIC * ノートブックをDatabricksジョブのタスクとしてスケジュールする
# MAGIC * DLTパイプラインをDatabricksジョブのタスクとしてスケジュールする
# MAGIC * DatabricksワークフローUIを使用してタスク間の線形依存関係を構成する

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-09.2.1L

# COMMAND ----------

# MAGIC %md <i18n value="b7163714-376c-41fd-8e38-80a7247fa923"/>
# MAGIC 
# MAGIC ## 初期データの配置（Land Initial Data）
# MAGIC 先に進む前に、データを用いてランディングゾーンをシードします。 後でこのコマンドを再実行して追加データを配置します。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="6bc33560-37f4-4f91-910d-669a1708ba66"/>
# MAGIC 
# MAGIC ## パイプラインを作成し構成する（Create and Configure a Pipeline）
# MAGIC 
# MAGIC ここで作成するパイプラインは前のレッスンで作成したものとほとんど同じです。
# MAGIC 
# MAGIC このパイプラインは、このレッスンで、スケジュールされたジョブの一環として使用します。
# MAGIC 
# MAGIC 以下のセルを実行して、次の構成段階で使用する値を出力します。

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="c8b235db-10cf-4a56-92d9-330b80da4f0f"/>
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. サイドバーの**ワークフロー**ボタンをクリックします
# MAGIC 1. **Delta Live Tables**タブを選択します
# MAGIC 1. **パイプラインを作成**をクリックします
# MAGIC 1. **パイプライン名**を入力します。名前は一意である必要があるため、上記のセルに記載されている**Pipeline Name**使用することをおすすめします
# MAGIC 1. **ノートブックライブラリ**では、ナビゲーターを使って上記のセルに記載されているノートブックを探して選択します
# MAGIC 1. **構成**の下に、3つのパラメータを追加します：
# MAGIC    * **構成を追加**をクリックし, "key"を**spark.master**、 "value"を **local[\*]** にします。
# MAGIC    * **構成を追加**をクリックし, "key"を**datasets_path**、 "value"を上記のセルに記載されている値にします。
# MAGIC    * **構成を追加**をクリックし, "key"を**source**、 "value"を上記のセルに記載されている値にします。
# MAGIC 1. **ターゲット**フィールドに、上記のセルで記載されているデータベースの名前を指定します。<br/> データベースの名前は、 **`da_<name>_<hash>_dewd_jobs_lab_92`** というパターンに従っているはずです。
# MAGIC 1. **ストレージの場所**フィールドに、上記で出力されている通りディレクトリをコピーします。
# MAGIC 1. **パイプラインモード**では、**トリガー**を選択します
# MAGIC 1. **オートスケーリングを有効化**ボックスのチェックを外します。
# MAGIC    (オートスケーリングを有効化がない新しいUIの場合、**Cluster mode**から**Fixed size**を選択します)
# MAGIC 1. ワーカーの数を **`0`** （0個）に設定します。
# MAGIC 1. **Photonアクセラレータを使用**をチェックします。
# MAGIC 1. **作成**をクリックします
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：このパイプラインは、このレッスンの後半でジョブによって実行されるため、直接は実行しません。<br/> しかし、**開始**ボタンをクリックしてもテストすることができます。

# COMMAND ----------

# ANSWER
 
# This function is provided for students who do not 
# want to work through the exercise of creating the pipeline.
DA.create_pipeline()

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="f98768ac-cbcc-42a2-8c51-ffdc3778aa11"/>
# MAGIC 
# MAGIC ## ノートブック・ジョブをスケジュールする (Schedule a Notebook Job)
# MAGIC 
# MAGIC ジョブ UIを使用して複数のタスクでワークロードをオーケストレーションする場合、常に1つのタスクをスケジュールすることから始めます。
# MAGIC 
# MAGIC 開始する前に、次のセルを実行して、このステップで使用される値を取得します。

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md <i18n value="fab2a427-5d5a-4a82-8947-c809d815c2a3"/>
# MAGIC 
# MAGIC ここでは、次のノートブックをスケジュールします。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. サイドバーの**ワークフロー**をクリックします。
# MAGIC 1. **ジョブ**タブを選択します。
# MAGIC 1. 青色の**ジョブ作成**ボタンをクリックします
# MAGIC 1. タスクを設定します：
# MAGIC     1. タスク名として**Batch-Job**と入力します
# MAGIC     1. **種類**から**ノートブック**を選択します
# MAGIC     1. **パス**から上記セルに記載されている**Batch Notebook Path**を選択します
# MAGIC     1. **クラスター**のドロップダウンから**既存の多目的クラスター**の下にあるクラスタを選択します
# MAGIC     1. **作成**をクリックします。
# MAGIC 1. 画面の左上でジョブ（タスクではなく）を **`Batch-Job`** （デフォルトの値）から前のセルに記載されている**ジョブの名前**に変更します
# MAGIC 1. 右上にある**今すぐ実行**ボタンをクリックしてジョブを開始します
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：汎用クラスタを選択する際、All-purposeコンピュートとして請求される警告が表示されます。 本番環境のジョブは常に、ワークロードにサイズを合わせた新しいジョブクラスタに対してスケジュールしたほうが良いです。こうしたほうが、費用を抑えられます。

# COMMAND ----------

# MAGIC %md <i18n value="1ab345ce-dff4-4a99-ad45-209793ddc581"/>
# MAGIC 
# MAGIC ## DLTパイプラインをタスクとしてスケジュールする（Schedule a DLT Pipeline as a Task）
# MAGIC 
# MAGIC このステップでは、レッスンの最初に構成したタスクが正常に終了した後に実行するDLTパイプラインを追加します。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の左上にある**ジョブの実行**の横の**タスク**タブをクリックします。
# MAGIC 1. 画面の中央下にある **+** が付いている大きな青色の円形をクリックして新規タスクを追加します。
# MAGIC 1. タスクを構成します。
# MAGIC     1. **タスク名**を**DLT**として指定します。
# MAGIC     1. **種類**から、 **`Delta Live Tablesパイプライン`** を選択します。
# MAGIC     1. **パイプライン**フィールドから、先ほど構成したDLTパイプラインを選択します。<br/> 注：パイプラインは**DLT-Job-Lab-92**で始まり、あなたのメールアドレスアドレスで終わるものになります。
# MAGIC     1. **依存先**フィールドは、以前に定義したタスク(Batch-Job)をデフォルトとして使用します。そのままにします。
# MAGIC     1. 青色の**タスクを作成**ボタンをクリックします。
# MAGIC 
# MAGIC 次に2つのボックスがある画面とその間の下矢印が表示されるはずです。
# MAGIC 
# MAGIC あなたの **`Batch-Job`** タスクは上にあります。このタスクの実行が完了すると、 **`DLT`** タスクが実行されます。

# COMMAND ----------

# MAGIC %md <i18n value="dd4e16c5-1842-4642-8159-117cfc84d4b4"/>
# MAGIC 
# MAGIC ## 追加のノートブックタスクをスケジュールする（Schedule an Additional Notebook Task）
# MAGIC 
# MAGIC DLTパイプラインで定義されたDLTメトリックとゴールドテーブルの一部を照会する追加のノートブックが用意されています。
# MAGIC 
# MAGIC これを最終タスクとしてジョブに追加します。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の中央下にある**+**が付いている大きな青色の円形をクリックして新規タスクを追加します
# MAGIC 1. タスクを設定します。
# MAGIC     1. **タスク名**を**Query-Results**として指定します
# MAGIC     1. **種類**は**ノートブック**のままにします
# MAGIC     1. **Path**は上記セルに記載されている**Query Notebook Path**を選択します
# MAGIC     1. **クラスター**のドロップダウンから**既存の多目的クラスター**の下にあるクラスタを選択します
# MAGIC     1. **依存先**はデフォルトとして前回定義したタスク**DLT**を使用します。
# MAGIC     1. 青色の**タスクを作成**ボタンをクリックします
# MAGIC 
# MAGIC 画面の右上にある**今すぐ実行**ボタンをクリックしてこのジョブを実行します。
# MAGIC 
# MAGIC **ジョブの実行**タブから、**アクティブな実行**セクションにあるこの実行の開始時刻をクリックして、タスクの進行状況を目で確認できます。
# MAGIC 
# MAGIC すべてのタスクが正常に終了したら、各タスクのコンテンツを確認して期待通りの動作であるかどうかを確認します。

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job()

# COMMAND ----------

DA.validate_job_config()

# COMMAND ----------

# ANSWER

# This function is provided to start the job and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
