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
# MAGIC # Databricks使用したジョブのオーケストレーション（Orchestrating Jobs with Databricks）
# MAGIC 
# MAGIC Databricks Jobs UIの新しい更新により、ジョブの一部として複数のタスクをスケジュールする機能が追加され、Databricks Jobsがほとんどの本番ワークロードのオーケストレーションを完全に処理できるようになりました。
# MAGIC 
# MAGIC ここでは、ノートブックをトリガーされたスタンドアロンジョブとしてスケジュールする手順を確認してから、DLTパイプラインを使用して依存ジョブを追加します。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * ノートブックをDatabricksジョブとしてスケジュールする
# MAGIC * ジョブスケジューリングオプションとクラスタタイプの違いを説明する
# MAGIC * ジョブの実行を確認して進捗状況を追跡し、結果を確認する
# MAGIC * DLTパイプラインをDatabricksジョブとしてスケジュールする
# MAGIC * DatabricksジョブUIを使用してタスク間の線形依存関係を構成する

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-9.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## パイプラインを作成し構成する（Create and Configure a Pipeline）
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
# MAGIC ## パイプラインを作成し構成する（Create and Configure a Pipeline）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. サイドバーの**ジョブ**ボタンをクリックします
# MAGIC 1. **Delta Live Tables**タブを選択します
# MAGIC 1. **パイプラインを作成**をクリックします
# MAGIC 1. **パイプライン名**を入力します。これらの名前は一意である必要があるため、上記のセルに記載されている**Pipeline Name**使用することをおすすめします
# MAGIC 1. **ノートブックライブラリ**では、ナビゲーターを使って**DE 9.1.3L - DLTジョブ**という付録のノートブックを探して選択します。 または、**Notebook Path**をコピーして、用意されているフィールドに貼り付けることもできます。
# MAGIC 1. **ターゲット**フィールドに、上記のセルで**Target**の隣に表示されているデータベースの名前を指定します。<br/> データベースの名前は、 **`dbacademy_<username>_dewd_dlt_demo_91`** というパターンに従っているはずです。
# MAGIC 1. **ストレージの場所**フィールドに、上記で出力されている通りディレクトリをコピーします
# MAGIC 1. **パイプラインモード**では、**トリガー**を選択します。
# MAGIC 1. **オートパイロットオプション**ボックスのチェックを外します
# MAGIC 1. ワーカーの数を **`1`** （1つ）に設定します。
# MAGIC 1. **作成**をクリックします。
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：このパイプラインは、このレッスンの後半でジョブによって実行されるため、直接は実行しません。<br/> しかし、簡単にテストしたいユーザーは、今すぐ**開始**ボタンをクリックしても良いです。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## ノートブックジョブをスケジュールする（Schedule a Notebook Job）
# MAGIC 
# MAGIC Jobs UIを使用して複数のタスクでワークロードにオーケストレーションを実行する場合、常に1つのタスクをスケジュールすることから始めます。
# MAGIC 
# MAGIC 開始する前に、次のセルを実行して、このステップで使用される値を取得します。

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ここでは、まず、次のノートブックをスケジュールします
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. Databricksの左側のナビゲーションバーを使って、Jobs UIに移動します。
# MAGIC 1. 青色の **`ジョブを作成`** ボタンをクリックします
# MAGIC 1. タスクを構成します：
# MAGIC     1. タスク名として **`reset`** と入力します
# MAGIC     1. ノートブックピッカーを使って **`DE 9.1.2 - リセット`** のノートブックを選択します。
# MAGIC     1. **Cluster**のドロップダウンから**既存の多目的クラスター**の下にあるクラスタを選択します
# MAGIC     1. **作成**をクリックします。
# MAGIC 1. 画面の左上でジョブ（タスクではなく）を **`reset`**  （デフォルトの値）から前のセルに記載されている**Job Name**に変更します。
# MAGIC 1. 右上にある**今すぐ実行**ボタンをクリックしてこのジョブを実行します。
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：汎用クラスタを選択する際、汎用コンピュートとして請求されるという警告が表示されます。 本番環境のジョブは常に、ワークロードにサイズを合わせた新しいジョブクラスタに対してスケジュールしたほうが良いです。こうしたほうが、費用を抑えられます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## cronを使ってDatabricksジョブをスケジュールする（Cron Scheduling of Databricks Jobs）
# MAGIC 
# MAGIC Jobs UIの右側、**ジョブの詳細**セクションのすぐ下に、**スケジュール**というラベルの付いたセクションがあることに注意してください。
# MAGIC 
# MAGIC **スケジュールを編集**ボタンをクリックして、スケジュールオプションを確認します。
# MAGIC 
# MAGIC **スケジュールのタイプ**フィールドを**手動**から**スケジュール済み**に変更すると、cronスケジューリングUIが表示されます。
# MAGIC 
# MAGIC このUIでは、ジョブの時系列スケジューリングを設定するための幅広いオプションが使用できます。 UIで構成された設定は、cron構文で出力することもできます。これは、UIで使用できないカスタム構成が必要な場合に編集できます。
# MAGIC 
# MAGIC とりあえずは、ジョブ設定を**手動**スケジューリングのままにしておきます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 実行を確認する（Review Run）
# MAGIC 
# MAGIC 現在の構成では、この単一のノートブックは、単一のノートブックのみをスケジュールできる従来のDatabricksジョブUIと同じパフォーマンスを実現します。
# MAGIC 
# MAGIC ジョブの実行を確認するための手順
# MAGIC 1. 画面の左上で**ジョブの実行**タブを選択します（現在**タスク**を選択しているはずです）
# MAGIC 1. ジョブを見つけます。 **ジョブがまだ実行中**の場合、ジョブは**アクティブな実行**セクションの下に表示されます。 **ジョブの実行が完了している**場合、ジョブは**完了済みの実行アイテム**セクションに表示されます
# MAGIC 1. **開始時刻**列の下のタイムスタンプフィールドをクリックして、出力の詳細を開きます
# MAGIC 1. **ジョブがまだ実行中**の場合は、右側のパネルに**ステータス**が **`Pending`** もしくは **`Running`** であるノートブックのアクティブな状態が表示されます。 **ジョブが完了している**の場合は、右側のパネルに**ステータス**が **`Succeeded`** もしくは **`Failed`** であるノートブックの完全実行が表示されます。
# MAGIC 
# MAGIC ノートブックは、MAGICコマンド  **`%run`** を使用して、相対パスを使用して追加のノートブックを呼び出します。 このコースでは取り上げていませんが、<a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">Databricks Reposに追加された新機能により、相対パスを使用してPythonモジュールをロードできるようになりました</a>。
# MAGIC 
# MAGIC スケジュールされたノートブックの実際の結果は、新しいジョブとパイプラインの環境をリセットすることです。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## DLTパイプラインをタスクとしてスケジュールする（Schedule a DLT Pipeline as a Task）
# MAGIC 
# MAGIC このステップでは、レッスンの最初に構成したタスクが正常に終了した後に実行するDLTパイプラインを追加します。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 画面の左上に、**ジョブの実行**タブが現在選択されていることが表示されます。**タスク**タブをクリックします。
# MAGIC 1. 画面の中央下にある **+** が付いている大きな青色の円形をクリックして新規タスクを追加します
# MAGIC     1. **タスク名**を **`dlt`** として指定します
# MAGIC     1. **種類**から、 **`Delta Live Tablesパイプライン`** を選択します
# MAGIC     1. **パイプライン**フィールドを選択して、以前構成したDLTパイプラインを選択します<br/> 注：パイプラインは**Jobs-Demo-91**で始まり、あなたのメールアドレスアドレスで終わります。
# MAGIC     1. **依存先**フィールドは、以前に定義したタスクをデフォルトとして使用しますが、以前行った値**reset**により**Jobs-Demo-91-あなたのメールアドレス**のような名前に変わっている可能性があります。
# MAGIC     1. 青色の**タスクを作成**ボタンをクリックします
# MAGIC 
# MAGIC 次に2つのボックスがある画面とその間の下に向いている矢印が表示されるはずです。
# MAGIC 
# MAGIC あなたの **`reset`**  （おそらく、**Jobs-Demo-91-あなたのメールアドレスアドレス**のような名前に変わっている）タスクは、上にあり、 **`DLT-Pipeline`** タスクへと続きます。
# MAGIC 
# MAGIC このビジュアライゼーションは、これらのタスク間の依存関係を表しています。
# MAGIC 
# MAGIC **今すぐ実行**をクリックしてジョブを実行します。
# MAGIC 
# MAGIC **注**：ジョブとパイプラインのインフラストラクチャが展開されている間、数分待つ必要がある場合があります。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## マルチタスク実行結果を確認する（Review Multi-Task Run Results）
# MAGIC 
# MAGIC **ジョブの実行**タブをもう一度選択し、ジョブが完了したかどうかに応じて、**アクティブな実行**または**完了済みの実行アイテム**で最新の実行を選択します。
# MAGIC 
# MAGIC タスクのビジュアライゼーションは、アクティブに実行されているタスクを反映するためにリアルタイムで更新され、タスクに失敗すると色が変わります。
# MAGIC 
# MAGIC タスクボックスをクリックすると、スケジュールされたノートブックがUIに表示されます。
# MAGIC 
# MAGIC これは、以前のDatabricksジョブUIの上の単なるオーケストレーションの追加レイヤーだと考えれば分かりやすいかと思います。CLIまたはREST APIを使用してジョブをスケジュールするワークロードがある場合、<a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">ジョブの構成と結果の取得に使用されるJSON構造には、UIと同様の更新が行われることに注意してください</a>。
# MAGIC 
# MAGIC **注**：現時点では、タスクとしてスケジュールされたDLTパイプラインは、Runs GUIで結果を直接レンダリングしません。代わりに、スケジュールされたパイプラインのDLTパイプラインGUIに移動します。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
