# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="8c3bda3d-9889-40cc-9b21-f42cab247b73"/>
# MAGIC 
# MAGIC # 本番データおよび派生テーブルの権限を構成する（Configuring Privileges for Production Data and Derived Tables）
# MAGIC 
# MAGIC DatabricksのテーブルACLがどのように機能するかを説明するために、ユーザーのペアを対象とした詳細な解説を以下に提供しています。 Databricks SQLとData Explorerを使用してこれらのタスクを達成します。また、どちらのユーザーもワークスペースの管理者権限を持っていないことを想定しています。 Databricks SQLでデータベースを作成するためには、管理者が事前に **`CREATE`** および **`USAGE`** 権限をカタログ上でユーザーに付与しておく必要があります。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC 
# MAGIC このラボでは、以下のことが学べます。
# MAGIC * Data Explorerを使用してリレーショナルエンティティを操作する
# MAGIC * Data Explorerでテーブルとビューの権限を構成する
# MAGIC * テーブルの検出とクエリを可能にする最小限の権限を構成する
# MAGIC * DBSQLで作成されたデータベース、テーブル、ビューの所有権を変更する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.2L

# COMMAND ----------

# MAGIC %md <i18n value="ed6459eb-0045-4614-9639-d3e3ff6dff2a"/>
# MAGIC 
# MAGIC ## パートナーとユーザー名を交換する（Exchange User Names with your Partner）
# MAGIC ユーザー名とメールアドレスが一致するワークスペースを使用していない場合には、パートナーがあなたのユーザー名を把握していることを確認してください。
# MAGIC 
# MAGIC これは、後の手順で権限を付与したり、データベースを検索したりする際に必要となります。
# MAGIC 
# MAGIC 次のセルでは、ユーザー名を表示します。

# COMMAND ----------

print(f"Your username: {DA.username}")

# COMMAND ----------

# MAGIC %md <i18n value="6541f150-07b2-4acb-931b-3c33e66cdce0"/>
# MAGIC 
# MAGIC ## セットアップ文の生成（Generate Setup Statements）
# MAGIC 
# MAGIC 次のセルは、Pythonを使用して現在のユーザーのユーザー名を抽出し、これをデータベース、テーブル、ビューの作成に使用されるいくつかの文へとフォーマットしています。
# MAGIC 
# MAGIC 両学生ともに次のセルを実行する必要があります。
# MAGIC 
# MAGIC 実行に成功すると、一連のフォーマットされたSQLクエリが出力されます。それらはDBSQLクエリエディターへとコピーして実行できます。

# COMMAND ----------

DA.generate_query()

# COMMAND ----------

# MAGIC %md <i18n value="3c5a67e7-b871-49be-8097-998c2e698ee3"/>
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 上のセルを実行します
# MAGIC 1. 出力されたものをすべてクリップボードにコピーします
# MAGIC 1. Databricks SQLワークスペースへと移動します
# MAGIC 1. SQLウェアハウスが起動していることを確認します
# MAGIC 1. 左側のサイドバーを使用して、**SQLエディタ**を選択します
# MAGIC 1. 上のクエリを貼り付け、右上にある青色の**実行**をクリックします
# MAGIC 
# MAGIC **注**：これらのクエリを正常に実行するためには、SQLウェアハウスに接続する必要があります。 SQLウェアハウスに接続できない場合には、管理者に連絡してアクセス権を付与してもらう必要があります。

# COMMAND ----------

# MAGIC %md <i18n value="b2ddfe82-c24e-474a-b3ec-0786e095dc69"/>
# MAGIC 
# MAGIC ## データベースを見つける（Find Your Database）
# MAGIC Data Explorerで、以前作成したデータベース（これは **`dbacademy_<username>_dewd_acls_lab`** というパターンに従っているはずです）を見つけます。
# MAGIC 
# MAGIC データベース名をクリックすると、含まれているテーブルとビューのリストが左側に表示されます。
# MAGIC 
# MAGIC 右側には**所有者**や**場所**などのデータベースに関する詳細情報がいくつか表示されます。
# MAGIC 
# MAGIC **権限**タブをクリックし、現在の権限所有者を確認します（ワークスペースの構成によっては、一部の権限がカタログ上の設定から継承されている場合があります）。

# COMMAND ----------

# MAGIC %md <i18n value="8a89aab0-2d1e-45e9-a24b-44e1106ebc39"/>
# MAGIC 
# MAGIC ## データベース権限の変更（Change Database Permissions）
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. データベースの**権限**タブが選択されていることを確認します
# MAGIC 1. 青色の**付与**ボタンをクリックします
# MAGIC 1. **USAGE**、**SELECT**、**READ_METADATA**オプションを選択します
# MAGIC 1. 上にあるフィールドにパートナーのユーザー名を入力します。
# MAGIC 1. **OK**をクリックします
# MAGIC 
# MAGIC お互いのデータベースとテーブルが確認可能であることをパートナー同士で確認します。

# COMMAND ----------

# MAGIC %md <i18n value="b5803c49-52ce-4155-a448-10fc98a589fc"/>
# MAGIC 
# MAGIC ## クエリを実行して確認する（Run a Query to Confirm）
# MAGIC 
# MAGIC あなたのデータベースに対して **`USAGE`** 、 **`SELECT`** 、 **`READ_METADATA`** を付与すると、パートナーはこのデータベースのテーブルとビューを自由にクエリできるようになりますが、新しいテーブルを作成したり、あなたのデータを修正したりすることはできないはずです。
# MAGIC 
# MAGIC SQL Editorでは、各ユーザーは追加されたばかりのデータベースでこの動作を確認するために、一連のクエリを実行する必要があります。
# MAGIC 
# MAGIC **以下のクエリを実行する際に、パートナーのデータベースを指定していることを確認します。**
# MAGIC 
# MAGIC **注**：最初の3つのクエリは成功し、最後のクエリは失敗するはずです。

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_confirmation_query("FILL_IN")

# COMMAND ----------

# MAGIC %md <i18n value="3c23d5fc-0400-49fb-9e1e-fabd00f875f2"/>
# MAGIC 
# MAGIC ## Beansの結合を生成するクエリの実行（Execute a Query to Generate the Union of Your Beans）
# MAGIC 
# MAGIC 以下のクエリを、自分のデータベースに実行してみてください。
# MAGIC 
# MAGIC **注**： **`grams`** と **`delicious`** 列にランダムな値が挿入されているため、 **`name`** 、 **`color`** のペアのそれぞれに異なる2行が表示されているはずです。

# COMMAND ----------

DA.generate_union_query()

# COMMAND ----------

# MAGIC %md <i18n value="de0c1434-fa02-4296-aae0-f253e006e9f5"/>
# MAGIC 
# MAGIC ## 派生ビューのデータベースへの登録（Register a Derivative View to Your Database）
# MAGIC 
# MAGIC 以下のクエリを実行し、先ほどのクエリの結果をデータベースに登録します。

# COMMAND ----------

DA.generate_derivative_view()

# COMMAND ----------

# MAGIC %md <i18n value="0f32f538-b1dc-4d47-9ba9-4f1ccddf5e6f"/>
# MAGIC 
# MAGIC ## パートナーのビューを照会する（Query Your Partner's View）
# MAGIC 
# MAGIC パートナーが前の手順を正常に完了したら、それぞれのテーブルに対して以下のクエリを実行します。同じ結果が得られるはずです。

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_partner_view("FILL_IN")

# COMMAND ----------

# MAGIC %md <i18n value="c06b7d01-464e-4265-b0a3-0fc5691e4073"/>
# MAGIC 
# MAGIC ## 変更権限の追加（Add Modify Permissions）
# MAGIC 
# MAGIC それでは、お互いの **`beans`** テーブルを削除してみましょう。
# MAGIC 
# MAGIC 現時点では、上手くいかないはずです。
# MAGIC 
# MAGIC Data Explorerを使用して、あなたの **`beans`** テーブルの **`MODIFY`** 権限をあなたのパートナーに追加します。
# MAGIC 
# MAGIC もう一度、パートナーの **`beans`** テーブルを削除してみてください。
# MAGIC 
# MAGIC また失敗するはずです。
# MAGIC 
# MAGIC **テーブルの所有者のみがこの文を実行できるはず**。<br/> （必要に応じて、所有権を個人からグループに譲渡できることに注意してください）。
# MAGIC 
# MAGIC 代わりに、レコードを削除するクエリをパートナーのテーブルから実行します。

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_delete_query("FILL_IN")

# COMMAND ----------

# MAGIC %md <i18n value="1b4b0745-e3d2-4024-ab59-8afd2f5b803f"/>
# MAGIC 
# MAGIC このクエリによって、目的のテーブルからすべてのレコードが正常に削除されたはずです。
# MAGIC 
# MAGIC このラボで以前にクエリを実行したテーブルのビューのいずれかに対して、クエリを再実行してみてください。
# MAGIC 
# MAGIC **注**：手順が正しく完了されていれば、ビューが参照するデータが削除されたため、以前のクエリはどれも結果を返さないはずです。 これは、プロダクションアプリケーションやダッシュボードで使用されるデータを扱っているユーザーに **`MODIFY`** 権限を付与することのリスクを示しています。
# MAGIC 
# MAGIC もし時間があれば、Deltaメソッド **`DESCRIBE HISTORY`** と **`RESTORE`** を使用してテーブル内のレコードを元に戻せるかどうかを確認してみてください。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
