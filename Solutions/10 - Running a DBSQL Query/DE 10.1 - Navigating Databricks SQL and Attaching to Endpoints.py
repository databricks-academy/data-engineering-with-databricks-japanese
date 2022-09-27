# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="cbd3eef7-fc50-4393-bed5-44d7711f8052"/>
# MAGIC 
# MAGIC # Databricks SQLの操作とSQLウェアハウスへのアタッチ（Navigating Databricks SQL and Attaching to SQL Warehouses）
# MAGIC 
# MAGIC * Databricks SQLに移動します
# MAGIC   * サイドバー（Databricksロゴのすぐ下）のワークスペースオプションからSQLが選択されていることを確認します
# MAGIC * SQLウェアハウスがオンでアクセス可能であることを確認します
# MAGIC   * サイドバーでSQLウェアハウスに移動します
# MAGIC   * SQLウェアハウスが存在し、状態が **`実行中`** の場合は、このSQLウェアハウスを使用します
# MAGIC   * SQLウェアハウスは存在するが **`停止`** である場合は、（このオプションが利用可能な場合） **`開始`** ボタンをクリックします（**注意**：利用できる最も小さいウェアハウスを開始してください）
# MAGIC   * SQLウェアハウスが存在せず、(権限がある場合は)**`SQLウェアハウスを作成`** をクリックします。SQLウェアハウスにわかりやすい名前を付け、クラスタサイズを2X-Smallに設定します。 他のすべてのオプションはデフォルトのままにします。
# MAGIC   * SQLウェアハウスを作成または接続する方法がない場合、続行するには、ワークスペース管理者にお問い合わせして、Databricks SQLのコンピュートリソースへのアクセスを要求する必要があります。
# MAGIC * Databricks SQLのホームページに移動します
# MAGIC   * サイドナビゲーションバーの上部にあるDatabricksのロゴをクリックします
# MAGIC * **サンプルダッシュボード**を見つけて、 **`ギャラリーに移動`** をクリックします。
# MAGIC * **Retail Revenue & Supply Chain**オプションの横にある **`インポート`** をクリックします
# MAGIC   * SQLウェアハウスが利用可能な場合は、ダッシュボードが読み込まれ、すぐに結果が表示されます。
# MAGIC   * 右上の**更新**をクリックします（元になっているデータは変更されていませんが、変更を取得するために使用するのはこのボタンです）
# MAGIC 
# MAGIC # DBSQLダッシュボードの更新（Updating a DBSQL Dashboard）
# MAGIC 
# MAGIC * サイドバーナビゲーターを使用して、**ダッシュボード**を見つけます
# MAGIC   * 先ほどロードしたサンプルダッシュボードを見つけます。**Retail Revenue & Supply Chain**というダッシュボードで、 **`作成者`** フィールドにあなたのユーザー名が記載されているはず。 **注**：右側の**マイダッシュボード**オプションは、ワークスペース内の他のダッシュボードを除外するためのショートカットとして使用できます。
# MAGIC   * ダッシュボード名をクリックして表示します
# MAGIC * **Shifts in Pricing Priorities**プロットの背後にあるクエリを表示します
# MAGIC   * プロットにカーソルを合わせます。すると3つの垂直ドットが表示されます。 ドットをクリックします
# MAGIC   * 表示されるメニューから**クエリを表示**を選択します
# MAGIC * このプロットの作成に使用されたSQLコードを確認します
# MAGIC   * ソーステーブルを識別するために3層の名前空間が使用されていることに注意してください。これは、今後Unity Catalogでサポートされる新機能のプレビューです。
# MAGIC   * 画面右上の **`実行`** をクリックして、クエリの結果をプレビューします
# MAGIC * ビジュアライゼーションを確認します
# MAGIC   * クエリの下で、**テーブル**というタブが選択されているはずです。**Price by Priority over Time**をクリックして、プロットのプレビューに切り替えます
# MAGIC   * 画面下部の**ビジュアライゼーションを編集**をクリックして設定を確認します
# MAGIC   * 設定の変更がビジュアライゼーションにどのように影響するかを探索します
# MAGIC   * 変更を適用する場合は、**保存**をクリックします。それ以外の場合は、**キャンセル**をクリックします
# MAGIC * クエリエディタに戻り、ビジュアライゼーション名の右側にある**ビジュアライゼーションを追加**ボタンをクリックします
# MAGIC   * 棒グラフを作成します
# MAGIC   * **X列**を **`Date`** として設定します
# MAGIC   * **Y列**を **`Total Price`** として設定します
# MAGIC   *  **`Priority`** で**Group by**します
# MAGIC   * **Stacking**を **`Stack`** に設定します
# MAGIC   * 他のすべての設定はデフォルトのままにします
# MAGIC   * **保存**をクリックします
# MAGIC * クエリエディタに戻り、このビジュアライゼーションのデフォルト名をクリックして編集します。 ビジュアライゼーション名を **`Stacked Price`** に変更します
# MAGIC * 変更したビジュアライゼーションの名前の右側にある3つの縦のドットをクリックします
# MAGIC   * メニューから**ダッシュボードに追加**を選択します
# MAGIC   *  **`Retail Revenue & Supply Chain`** ダッシュボードを選択します
# MAGIC * ダッシュボードに戻って、この変更を表示します
# MAGIC 
# MAGIC # 新しいクエリの作成（Create a New Query）
# MAGIC 
# MAGIC * サイドバーを使用して、**クエリ**に移動します
# MAGIC *  **`クエリを作成`** ボタンをクリックします
# MAGIC * SQLウェアハウスにつながっていることを確認してください。 **スキーマブラウザ**で現在のメタストアをクリックして、 **`samples`** を選択します。
# MAGIC   *  **`tpch`** データベースを選択します
# MAGIC   *  **`partsupp`** テーブルをクリックしてスキーマのプレビューを確認します
# MAGIC   *  **`partsupp`** テーブル名にカーソルを合わせ、 **>>** ボタンをクリックして、テーブル名を照会テキストに挿入します
# MAGIC * 最初のクエリを書きます：
# MAGIC   * 最後のステップでインポートしたフルネームを使用して **`partsupp`** から **`SELECT * FROM`** します。**実行**をクリックして結果のプレビューを表示します
# MAGIC   * このクエリを、 **`GROUP BY ps_partkey`** をし、 **`ps_partkey`** と **`sum(ps_availqty)`** を戻すように変更します。**実行**をクリックして結果のプレビューを表示します
# MAGIC   * クエリを更新して、2番目の列に **`total_availqty`** という名前を付け、クエリを再実行します
# MAGIC * クエリを保存します
# MAGIC   * 画面右上の**実行**の横にある**保存**ボタンをクリックします
# MAGIC   * クエリに覚えやすい名前を付けます
# MAGIC * クエリをダッシュボードに追加します
# MAGIC   * 画面の下部にある3つの垂直ボタンをクリックします
# MAGIC   * **ダッシュボードに追加だ**をクリックします
# MAGIC   *  **`Retail Revenue & Supply Chain`** ダッシュボードを選択します
# MAGIC * ダッシュボードに戻って、この変更を表示します
# MAGIC   * ビジュアライゼーションの配置を変更する場合は、画面の右上にある3つの垂直ボタンをクリックします。表示されるメニューで**編集**をクリックすると、ビジュアライゼーションをドラッグしてサイズ変更できます

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
