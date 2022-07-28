# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
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
# MAGIC * ノートブックをDatabricksジョブとしてスケジュールする
# MAGIC * DLTパイプラインをDatabricksジョブとしてスケジュールする
# MAGIC * Databricks Jobs UIを使用してタスク間の線形依存関係を構成する

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-9.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 初期データの配置（Land Initial Data）
# MAGIC 先に進む前に、データを用いてランディングゾーンをシードします。 後でこのコマンドを再実行して追加データを配置します。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## パイプラインを作成し構成する（Create and Configure a Pipeline）
# MAGIC 
# MAGIC ここで作成するパイプラインは前のレッスンで作成したものとほとんど同じです。
# MAGIC 
# MAGIC このパイプラインは、このレッスンで、スケジュールされたジョブの一環として使用します。
# MAGIC 
# MAGIC 以下のセルを実行して、次の構成段階で使用する値を出力します。

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. サイドバーの**ジョブ**ボタンをクリックします
# MAGIC 1. **Delta Live Tables**タブを選択します
# MAGIC 1. **パイプラインを作成**をクリックします
# MAGIC 1. **パイプライン名**を入力します。これらの名前は一意である必要があるため、上記のセルに記載されている**Pipeline Name**使用することをおすすめします
# MAGIC 1. **ノートブックライブラリ**では、ナビゲーターを使って**DE 9.2.3L - DLTジョブ**という付録のノートブックを探して選択します
# MAGIC     * または、上記のセルに記載されている**Notebook Path**をコピーして、用意されているフィールドに貼り付けることもできます
# MAGIC 1. ソースの構成
# MAGIC     *  **`構成を追加`** をクリックします
# MAGIC     * **Key**フィールドに **`source`** という単語を入力します
# MAGIC     * 上で指定されている**Source**値を **`Value`** フィールドに入力します
# MAGIC 1. **ターゲット**フィールドに、上記のセルで**Target**の隣に表示されているデータベースの名前を指定します<br/> データベースの名前は、 **`dbacademy_<username>_dewd_jobs_lab_92`** というパターンに従っているはずです。
# MAGIC 1. **ストレージの場所**フィールドに、上記で出力されている通りディレクトリをコピーします
# MAGIC 1. **パイプラインモード**では、**トリガー**を選択します
# MAGIC 1. **オートパイロットオプション**ボックスのチェックを外します
# MAGIC 1. ワーカーの数を **`1`** （1つ）に設定します
# MAGIC 1. **作成**をクリックします
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：このパイプラインは、このレッスンの後半でジョブによって実行されるため、直接は実行しません。<br/> しかし、簡単にテストしたいユーザーは、今すぐ**開始**ボタンをクリックしても良いです。

# COMMAND ----------

# MAGIC %md --i18n-f98768ac-cbcc-42a2-8c51-ffdc3778aa11
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ここでは、まず、ノートブックバッチジョブをスケジュールします。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. Databricksの左側のナビゲーションバーを使って、Jobs UIに移動します。
# MAGIC 1. 青色の**ジョブ作成**ボタンをクリックします
# MAGIC 1. タスクを構成します：
# MAGIC     1. タスク名として**Batch-Job**と入力します
# MAGIC     1. ノートブックピッカーを使って**DE 9.2.2L - バッチジョブ**のノートブックを選択します
# MAGIC     1. **Cluster**のドロップダウンから**既存の多目的クラスター**の下にあるクラスタを選択します
# MAGIC     1. **作成**をクリックします。
# MAGIC 1. 画面の左上でジョブ（タスクではなく）を **`Batch-Job`** （デフォルトの値）から前のセルに記載されている**ジョブの名前**に変更します
# MAGIC 1. 右上にある**今すぐ実行**ボタンをクリックしてジョブを開始し、ジョブを簡単にテストします
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：汎用クラスタを選択する際、汎用コンピュートとして請求されるという警告が表示されます。 本番環境のジョブは常に、ワークロードにサイズを合わせた新しいジョブクラスタに対してスケジュールしたほうが良いです。こうしたほうが、費用を抑えられます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## DLTパイプラインをタスクとしてスケジュールする（Schedule a DLT Pipeline as a Task）
# MAGIC 
# MAGIC このステップでは、レッスンの最初に構成したタスクが正常に終了した後に実行するDLTパイプラインを追加します。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の左上で**タスク**タブをクリック（すでに選択中じゃない場合）します。
# MAGIC 1. 画面の中央下にある **+** が付いている大きな青色の円形をクリックして新規タスクを追加します
# MAGIC     1. **タスク名**を**DLT-Pipeline**として指定します
# MAGIC     1. **種類**から、 **`Delta Live Tablesパイプライン`** を選択します
# MAGIC     1. **パイプライン**フィールドを選択して、以前構成したDLTパイプラインを選択します<br/> 注：パイプラインは**Jobs-Labs-92**で始まり、あなたのメールアドレスアドレスで終わります。
# MAGIC     1. **Depends on**フィールドは、以前に定義したタスクをデフォルトとして使用しますが、以前行った値**reset**により**Jobs-Lab-92-あなたのメールアドレス**のような名前に変わっている可能性があります。
# MAGIC     1. 青色の**タスクを作成**ボタンをクリックします
# MAGIC 
# MAGIC 次に2つのボックスがある画面とその間の下に向いている矢印が表示されるはずです。
# MAGIC 
# MAGIC あなたの **`Batch-Job`** （おそらく、**Jobs-Labs-92-あなたのメールアドレスアドレス**のような名前に変わっている）タスクは、上にあります。このタスクの実行が完了すると、 **`DLT-Pipeline`** タスクが実行されます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 追加のノートブックタスクをスケジュールする（Schedule an Additional Notebook Task）
# MAGIC 
# MAGIC DLTパイプラインで定義されたDLTメトリックとゴールドテーブルの一部を照会する追加のノートブックが用意されています。
# MAGIC 
# MAGIC これを最終タスクとしてジョブに追加します。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の左上で**タスク**タブをクリック（すでに選択中じゃない場合）します。
# MAGIC 1. 画面の中央下にある**+**が付いている大きな青色の円形をクリックして新規タスクを追加します
# MAGIC     1. **タスク名**を**Query-Results**として指定します
# MAGIC     1. **種類**は**ノートブック**のままにします
# MAGIC     1. ノートブックピッカーを使って**DE 9.2.4L - クエリ結果ジョブ**のノートブックを選択します
# MAGIC     1. **依存先**はデフォルトとして前回定義したタスク**DLT-Pipeline**を使用するので注意してください
# MAGIC     1. **Cluster**のドロップダウンから**既存の多目的クラスター**の下にあるクラスタを選択します
# MAGIC     1. 青色の**タスクを作成**ボタンをクリックします
# MAGIC 
# MAGIC 画面の右上にある**今すぐ実行**ボタンをクリックしてこのジョブを実行します。
# MAGIC 
# MAGIC **ジョブの実行**タブから、**アクティブな実行**セクションにあるこの実行の開始時刻をクリックして、タスクの進行状況を目で確認できます。
# MAGIC 
# MAGIC すべてのタスクが正常に終了したら、各タスクのコンテンツを確認して期待通りの動作であるかどうかを確認します。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
