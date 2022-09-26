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
# MAGIC 
# MAGIC ## レイクハウスでのエンドツーエンドETL（End-to-End ETL in the Lakehouse）
# MAGIC 
# MAGIC このノートブックでは、コース全体で学習した概念をまとめて、データパイプラインの例を完成させます。
# MAGIC 
# MAGIC 以下は、この演習を正常に完了するために必要なスキルとタスクの（包括的でない）リストです。
# MAGIC * Databricksノートブックを使用してSQLとPythonでクエリを作成する
# MAGIC * データベース、テーブル、およびビューの作成と変更
# MAGIC * マルチホップアーキテクチャでの増分データ処理にAuto LoaderとSpark構造化ストリーミングを使用する
# MAGIC * Delta Live TablesのSQL構文を使用する
# MAGIC * 継続的な処理のためにDelta Live Tablesのパイプラインを設定する
# MAGIC * Databricksジョブsを使用して、Reposに保存されているノートブックからタスクに対してオーケストレーションを実行する
# MAGIC * Databricksジョブの時系列スケジュールを設定する
# MAGIC * Databricks SQLでクエリを定義する
# MAGIC * Databricks SQLでビジュアライゼーションを作成する
# MAGIC * Databricks SQLダッシュボードを定義してメトリックと結果を確認する

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## セットアップを実行する（Run Setup）
# MAGIC 次のセルを実行して、このラボに関連しているすべてのデータベースとディレクトリをリセットします。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 初期データの配置（Land Initial Data）
# MAGIC 先に進む前に、データを用いてランディングゾーンをシードします。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## DLTパイプラインを作成し構成する（Create and Configure a DLT Pipeline）
# MAGIC **注**：ここでの手順とDLTを使用した以前のラボでの手順の主な違いは、この場合、**プロダクション**モードで**連続**に実行するためにパイプラインを設定することです。

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. サイドバーの**ジョブ**ボタンをクリックします
# MAGIC 1. **Delta Live Tables**タブを選択します。
# MAGIC 1. **パイプラインを作成**をクリックします。
# MAGIC 1. **パイプライン名**を入力します。これらの名前は一意である必要があるため、上記のセルに記載されている**Pipeline Name**使用することをおすすめします。
# MAGIC 1. **ノートブックライブラリ**では、ナビゲーターを使って**DE 12.2.2L - DLTタスク**という付録のノートブックを探して選択します。
# MAGIC     * または、上記のセルに記載されている**Notebook Path**をコピーして、用意されているフィールドに貼り付けることもできます。
# MAGIC 1. ソースの構成
# MAGIC     *  **`構成を追加`** をクリックします
# MAGIC     * **Key**フィールドに **`source`** という単語を入力します
# MAGIC     * 上で指定されている**Source**値を **`Value`** フィールドに入力します
# MAGIC 1. **ターゲット**フィールドに、上記のセルで**Target**の隣に表示されているデータベースの名前を指定します。<br/> データベースの名前は **`dbacademy_<username>_dewd_cap_12`** というパターンに従っているはずです。
# MAGIC 1. **ストレージの場所**フィールドに、上記で出力されている通りディレクトリをコピーします
# MAGIC 1. **Pipeline Mode**では、**連続**を選択します。
# MAGIC 1. **オートパイロットオプション**ボックスのチェックを外します
# MAGIC 1. ワーカーの数を **`1`** （1つ）に設定します。
# MAGIC 1. **作成**をクリックします
# MAGIC 1. UIが更新されたら、**開発**モードから**プロダクション**モードに変更します
# MAGIC 
# MAGIC これにより、インフラストラクチャの展開が開始されます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ノートブックジョブをスケジュールする（Schedule a Notebook Job）
# MAGIC 
# MAGIC DLTパイプラインは、データが到着するとすぐに処理するように設定されています。
# MAGIC 
# MAGIC この機能が実際に動作していることを確認できるように、毎分新しいデータのバッチを配置するようにノートブックをスケジュールします。
# MAGIC 
# MAGIC 開始する前に、次のセルを実行して、このステップで使用される値を取得します。

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. Databricksの左側のナビゲーションバーを使って、Jobs UIに移動します。
# MAGIC 1. 青色の**ジョブ作成**ボタンをクリックします
# MAGIC 1. タスクを構成します：
# MAGIC     1. タスク名として**Land-Data**と入力します
# MAGIC     1. ノートブックピッカーを使って**DE 12.2.3L - 新しいデータの配置**のノートブックを選択します
# MAGIC     1. **Cluster**のドロップダウンから**既存の多目的クラスター**の下にあるクラスタを選択します
# MAGIC     1. **作成**をクリックします
# MAGIC 1. 画面の左上でジョブ（タスクではなく）を **`Land-Data`** （デフォルトの値）から前のセルに記載されている**ジョブの名前**に変更します。
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png" /> **注**：汎用クラスタを選択する際、汎用コンピュートとして請求されるという警告が表示されます。 本番環境のジョブは常に、ワークロードにサイズを合わせた新しいジョブクラスタに対してスケジュールしたほうが良いです。こうしたほうが、費用を抑えられます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ジョブの時系列のスケジュールを設定する（Set a Chronological Schedule for your Job）
# MAGIC 手順は、次の通りです。
# MAGIC 1. **Jobs UI**に移動し、先ほど作成したジョブをクリックします。
# MAGIC 1. 右側のサイドパネルで**スケジュール**セクションを見つけます。
# MAGIC 1. **スケジュールを編集**ボタンをクリックして、スケジュールオプションを確認します。
# MAGIC 1. **スケジュールのタイプ**フィールドを**手動**から**スケジュール済み**に変更すると、cronスケジューリングUIが表示されます。
# MAGIC 1. スケジュールの更新間隔を**00**から**毎2分**に設定します
# MAGIC 1. **保存**をクリックします
# MAGIC 
# MAGIC **注**：必要に応じて、**今すぐ実行**をクリックして最初の実行をトリガーするか、次の1分が経過するまで待って、スケジュールが正常に機能することを確認します。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## DBSQLを使用して照会するためのDLTイベントメトリックを登録する（Register DLT Event Metrics for Querying with DBSQL）
# MAGIC 
# MAGIC 次のセルは、DBSQLでクエリを実行するためにDLTイベントログをターゲットデータベースに登録するSQL文を出力します。
# MAGIC 
# MAGIC DBSQLクエリエディタで出力コードを実行して、これらのテーブルとビューを登録します。
# MAGIC 
# MAGIC それぞれを調べて、ログに記録されたイベントメトリックをメモします。

# COMMAND ----------

DA.generate_register_dlt_event_metrics_sql()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ゴールドテーブルでクエリを定義する（Define a Query on the Gold Table）
# MAGIC 
# MAGIC **daily_patient_avg**テーブルは、新しいデータのバッチがDLTパイプラインを介して処理されるたびに自動的に更新されます。 このテーブルに対してクエリが実行されるたびに、DBSQLは新しいバージョンがあるかどうかを確認し、利用可能な最新バージョンから結果を取得します。
# MAGIC 
# MAGIC 次のセルを実行して、データベース名でクエリを出力します。 これをDBSQLクエリとして保存します。

# COMMAND ----------

DA.generate_daily_patient_avg()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 折れ線グラフのビジュアライゼーションを追加する（Add a Line Plot Visualization）
# MAGIC 
# MAGIC 時間の経過に伴う患者の平均の傾向を追跡するには、折れ線グラフを作成して新しいダッシュボードに追加します。
# MAGIC 
# MAGIC 次の設定で折れ線グラフを作成します。
# MAGIC * **X列**:  **`date`** 
# MAGIC * **Y列**:  **`avg_heartrate`** 
# MAGIC * **Group By**:  **`name`** 
# MAGIC 
# MAGIC このビジュアライゼーションをダッシュボードに追加します。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## データ処理の進捗状況を追跡する（Track Data Processing Progress）
# MAGIC 
# MAGIC 以下のコードは、DLTイベントログから **`flow_name`** 、 **`timestamp`** 、 **`num_output_rows`**  を抽出します。
# MAGIC 
# MAGIC このクエリをDBSQLに保存してから、次を示す棒グラフのビジュアライゼーションを定義します。
# MAGIC * **X列**:  **`timestamp`** 
# MAGIC * **Y列**:  **`num_output_rows`** 
# MAGIC * **Group By**:  **`flow_name`** 
# MAGIC 
# MAGIC ダッシュボードにビジュアライゼーションを追加します。

# COMMAND ----------

DA.generate_visualization_query()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ダッシュボードを更新して結果を追跡する（Refresh your Dashboard and Track Results）
# MAGIC 
# MAGIC 上記のジョブでスケジュールされた**Land-Data**ノートブックには、12バッチのデータがあり、それぞれが患者の少量のサンプルの1ヶ月分の記録を表しています。 手順に従って設定されているとすれば、これらのデータのバッチがすべてトリガーされて処理されるまでに20分強かかります（Databricksジョブが2分ごとに実行されるようにスケジュールされ、データのバッチは最初の取り込み後パイプラインを介して非常に迅速に処理されます）。
# MAGIC 
# MAGIC ダッシュボードを更新し、ビジュアライゼーションを確認して、処理されたデータのバッチ数を確認します。 （ここで概説されている手順に従った場合、DLTメトリックによって追跡されている異なるフロー更新が12件あるはずです。） すべてのソースデータがまだ処理されていない場合は、Databricks Jobs UIに戻って、追加のバッチを手動でトリガーできます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC すべてを設定したら、ノートブックでラボの最後の部分 \[DE 12.2.4L - 最終ステップ\]($./DE 12.2.4L - 最終ステップ）に進むことができます

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
