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
# MAGIC # Delta Live TablesUIの使用（Using the Delta Live Tables UI）
# MAGIC 
# MAGIC このデモではDLT UIについて見ていきます。
# MAGIC 
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * DLTパイプラインをデプロイする
# MAGIC * 結果DAGを調べる
# MAGIC * パイプラインの更新を実行する
# MAGIC * メトリックを見る

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## セットアップを実行する（Run Setup）
# MAGIC 
# MAGIC 以下のセルは、このデモをリセットするために構成されています。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 以下のセルを実行して、次の構成段階で使用する値を出力します。

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## パイプラインを作成し構成する（Create and Configure a Pipeline）
# MAGIC 
# MAGIC このセクションでは、コースウェアに付属しているノートブックを使ってパイプラインを構築します。 次のレッスンでは、ノートブックの内容について見ていきます。
# MAGIC 
# MAGIC 1. サイドバーの**ジョブ**ボタンをクリックします。
# MAGIC 1. **Delta Live Tables**タブを選択します。
# MAGIC 1. **パイプラインを作成**をクリックします。
# MAGIC 1. **製品エディション**は**Advanced**のままにします。
# MAGIC 1. **パイプライン名**を入力します。これらの名前は一意である必要があるため、上記のセルに記載されている **`Pipeline Name`** を使用することをおすすめします。
# MAGIC 1. **ノートブックライブラリ**では、ナビゲーターを使って**8.1.2 - Delta Live TablesのSQL**という付録のノートブックを探して選択します。
# MAGIC    * または、上記のセルに記載されている **`Notebook Path`** をコピーして、用意されているフィールドに貼り付けることもできます。
# MAGIC    * このドキュメントは標準のDatabricksノートブックですが、SQL構文はDLTテーブル宣言に特化しています。
# MAGIC    * 次のエクササイズでは、構文について見ていきます。
# MAGIC 1. **ターゲット**フィールドに、上記のセルで **`Target`** の隣に表示されているデータベースの名前を指定します。<br/> データベースの名前は、 **`dbacademy_<username>_dewd_dlt_demo_81`** というパターンに従っているはずです。
# MAGIC    * このフィールドは任意です。指定しなかった場合、テーブルはメタストアに登録されませんが、引き続きDBFSでは使用できます。 このオプションに関して詳しく知りたい場合は、こちらの<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">ドキュメント</a>を参考にしてください。
# MAGIC 1. **ストレージの場所**フィールドには、上記のセルの隣に表示されている **`Storage location`** をコピーしましょう。
# MAGIC    * この任意フィールドを使うことで、ユーザーはログ、テーブル、およびその他のパイプラインの実行に関連する情報を保管する場所が指定できます。
# MAGIC    * 指定しない場合、DLTが自動的にディレクトリを生成します。
# MAGIC 1. **パイプラインモード**では、**トリガー**を選択します。
# MAGIC    * このフィールドでは、パイプラインの実行方法を指定します。
# MAGIC    * **トリガー**パイプラインは一度だけ実行され、次の手動またはスケジュールされた更新まではシャットダウンします。
# MAGIC    * **連続**パイプラインは継続的に実行され、新しいデータが到着するとそのデータを取り込みます。 レイテンシとコスト要件に基づいてモードを選択してください。
# MAGIC 1. **オートスケールを有効化**ボックスのチェックを外し、ワーカーの数を **`1`** （1つ）に設定します。
# MAGIC    * **オートスケールを有効化**、**ワーカーの最小数**、**ワーカーの最大数**はパイプラインをクラスタ処理する際の基盤となるワーカー構成を制御します。 このDBU試算は、インタラクティブクラスタを構成した時に得られる試算と似ていることに注意してください。
# MAGIC 1. **作成**をクリックします。

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## パイプラインを実行する（Run a Pipeline）
# MAGIC 
# MAGIC パイプラインを構築したら、そのパイプラインを実行します。
# MAGIC 
# MAGIC 1. **開発**を選択し、開発モードでパイプラインを実行します。
# MAGIC   * 開発モードでは、（実行の度に新しいクラスタを作成するのではなく）クラスタを再利用し再試行を無効にすることで、より迅速なインタラクティブ開発を可能にします。
# MAGIC   * この機能に関して詳しく知りたい場合は、こちらの<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">ドキュメント</a>を参考にしてください。
# MAGIC 2. **開始**をクリックします。
# MAGIC 
# MAGIC クラスタが用意されている間、最初の実行には数分程度の時間が掛かります。
# MAGIC 
# MAGIC その後の実行では、速度が急激に速くなります。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## DAGを調べる（Exploring the DAG）
# MAGIC 
# MAGIC パイプラインが完了すると、実行フローがグラフ化されます。
# MAGIC 
# MAGIC テーブルを選択すると詳細を確認できます。
# MAGIC 
# MAGIC **sales_orders_cleaned**を選択します。 **データ品質**セクションで報告されている結果に注目してください。 このフローではデータの期待値が宣言されているため、それらのメトリックがここで追跡されます。 出力に違反しているレコードを含むように制限を宣言しているため、レコードが削除されることはありません。 この詳細は次のエクササイズで扱います。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
