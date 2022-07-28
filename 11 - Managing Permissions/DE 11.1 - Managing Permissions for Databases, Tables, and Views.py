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
# MAGIC # データベース、テーブル、ビューの権限管理（Managing Permissions for Databases, Tables, and Views）
# MAGIC 
# MAGIC DatabricksのテーブルACLがどのように機能するかを説明するために、ユーザーのグループを対象とした詳細な解説を以下に提供しています。 Databricks SQLとData Explorerを活用してこれらのタスクを実行し、グループ内の少なくとも1人のユーザーが管理者ステータスを持っていることを前提としています（または、ユーザーがデータベース、テーブル、ビューを作成するための適切な権限を許可する権限を管理者が以前に構成していることを前提としています） 。
# MAGIC 
# MAGIC これらの手順は管理者ユーザーが完了するためのものです。 次のノートブックには、ユーザーがペアで完了するための同様の演習があります。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * DBSQLでのユーザーと管理者のデフォルトの権限を説明する
# MAGIC * DBSQLで作成されたデータベース、テーブル、およびビューのデフォルトの所有者を特定し、所有権を変更する
# MAGIC * Data Explorerを使用してリレーショナルエンティティを操作する
# MAGIC * Data Explorerでテーブルとビューの権限を構成する
# MAGIC * テーブルの検出とクエリを可能にする最小限の権限を構成する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## セットアップ文の生成（Generate Setup Statements）
# MAGIC 
# MAGIC 次のセルは、Pythonを使用して現在のユーザーのユーザー名を抽出し、これをデータベース、テーブル、ビューの作成に使用されるいくつかの文へとフォーマットしています。
# MAGIC 
# MAGIC 管理者のみが次のセルを実行する必要があります。 実行に成功すると、一連のフォーマットされたSQLクエリが出力されます。それらはDBSQLクエリエディターへとコピーして実行できます。

# COMMAND ----------

DA.generate_users_table()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 上のセルを実行します
# MAGIC 1. 出力されたものをすべてクリップボードにコピーします
# MAGIC 1. Databricks SQLワークスペースへと移動します
# MAGIC 1. DBSQLエンドポイントが起動していることを確認します
# MAGIC 1. 左側のサイドバーを使用して、**SQLエディタ**を選択します
# MAGIC 1. 上のクエリを貼り付け、右上にある青色の**実行**をクリックします
# MAGIC 
# MAGIC **注**：これらのクエリを正常に実行するためには、DBSQLエンドポイントに接続する必要があります。 DBSQLエンドポイントに接続できない場合には、管理者に連絡してアクセス権を付与してもらう必要があります。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Data Explorerを使用する（Using Data Explorer）
# MAGIC 
# MAGIC * 左側のサイドバーナビゲーターを使用して、**データ**タブを選択します。これにより、**データエクスプローラ**が表示されます
# MAGIC 
# MAGIC ## データエクスプローラとは？（What is the Data Explorer?）
# MAGIC 
# MAGIC データエクスプローラを使用するとユーザーと管理者は次のことができます。
# MAGIC * データベース、テーブル、ビューを移動する
# MAGIC * データスキーマ、メタデータ、履歴を探る
# MAGIC * リレーショナルエンティティの権限を設定したり変更したりする
# MAGIC 
# MAGIC これらの解説が書かれている現在、Unity Catalogはまだ一般に利用可能ではないことに注意してください。 追加された3層の名前空間機能は、デフォルトの **`hive_metastore`** とダッシュボードやクエリなどに使用される **`サンプル`** カタログを切り替えることである程度下見できます。 Unity Catalogがワークスペースに追加されるにつれて、Data ExplorerのUIと機能が進化する予定です。
# MAGIC 
# MAGIC ## 権限を設定する（Configuring Permissions）
# MAGIC 
# MAGIC デフォルトでは、管理者はメタストアに登録されているすべてのオブジェクトを表示でき、ワークスペース内の他のユーザーの権限を制御できます。 ユーザーは、DBSQLで作成するオブジェクトを除き、メタストアに登録されているものに対してデフォルトでは権限を**一切与えられません**。ユーザーがデータベース、テーブル、またはビューを作成する前に、ユーザーに特別に付与された作成および使用の権限が必要であることに注意してください。
# MAGIC 
# MAGIC 通常、権限は、管理者によって構成されたグループを使用して設定されます。この操作は、多くの場合、SCIM統合から別のIDプロバイダーとの組織構造をインポートすることで行われます。 このレッスンでは、権限の制御に使用されるアクセス制御リスト（ACL）について説明しますが、説明はグループではなく個人を対象とします。
# MAGIC 
# MAGIC ## テーブルACL（Table ACLs）
# MAGIC 
# MAGIC Databricksでは、次のオブジェクトに対する権限を設定できます。
# MAGIC 
# MAGIC | オブジェクト   | 範囲                                                                                                               |
# MAGIC | -------- | ---------------------------------------------------------------------------------------------------------------- |
# MAGIC | CATALOG  | データカタログ全体へのアクセスを制御します。                                                                                           |
# MAGIC | DATABASE | データベースへのアクセスを制御します。                                                                                              |
# MAGIC | TABLE    | マネージドテーブルまたは外部テーブルへのアクセスを制御します。                                                                                  |
# MAGIC | VIEW     | SQLビューへのアクセスを制御します。                                                                                              |
# MAGIC | FUNCTION | 名前付き関数へのアクセスを制御します。                                                                                              |
# MAGIC | ANY FILE | 元となっているファイルシステムへのアクセスを制御します。 ANY FILE権限を付与されたユーザーは、ファイルシステムから直接読み取ることにより、カタログ、データベース、テーブル、およびビューに課せられた制限を回避できます。 |
# MAGIC 
# MAGIC **注**：現在、 **`ANY FILE`** オブジェクトを Data Explorerから設定することはできます。
# MAGIC 
# MAGIC ## 権限の付与（Granting Privileges）
# MAGIC 
# MAGIC Databricksの管理者とオブジェクトの所有者は、次のルールに従って権限を付与できます。
# MAGIC 
# MAGIC | 役割                                      | アクセス権限を次のものに対して付与可                |
# MAGIC | --------------------------------------- | --------------------------------- |
# MAGIC | Databricks administrator（Databricks管理者） | カタログ内のすべてのオブジェクトと元となっているファイルシステム。 |
# MAGIC | Catalog owner（カタログの所有者）                 | カタログ内のすべてのオブジェクト。                 |
# MAGIC | Database owner（データベースの所有者）              | データベース内のすべてのオブジェクト。               |
# MAGIC | Table owner（テーブルの所有者）                   | テーブルのみ（ビューと関数に対するオプションも似ています）。    |
# MAGIC 
# MAGIC **注**：現在、Data Explorerは、データベース、テーブル、およびビューの所有権を変更するためにのみ使用できます。 カタログの権限は、SQLクエリエディタを使用してインタラクティブに設定できます。
# MAGIC 
# MAGIC ## 権限（Privileges）
# MAGIC 
# MAGIC Data Explorerでは次の権限を設定できます。
# MAGIC 
# MAGIC | 権限の名前          | 権限の範囲                                                  |
# MAGIC | -------------- | ------------------------------------------------------ |
# MAGIC | ALL PRIVILEGES | すべての特権を付与します（以下のすべての権限を含みます）。                          |
# MAGIC | SELECT         | オブジェクトへの読み取りアクセスを許可します。                                |
# MAGIC | MODIFY         | オブジェクトとの間でデータを追加、削除、および変更できるようになります。                   |
# MAGIC | READ_METADATA  | オブジェクトとそのメタデータを表示できるようになります。                           |
# MAGIC | USAGE          | 権限の範囲は広くなりませんが、データベースオブジェクトに対してアクションを実行するための追加要件となります。 |
# MAGIC | CREATE         | オブジェクトを作成できるようになります（例えば、データベース内のテーブルなど）。               |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## デフォルトの権限を確認する（Review the Default Permissions）
# MAGIC データエクスプローラで、以前作成したデータベース（これは **`dbacademy_<username>_acls_demo`** というパターンに従っているはずです）を見つけます。
# MAGIC 
# MAGIC データベース名をクリックすると、含まれているテーブルとビューのリストが左側に表示されます。 右側には**所有者**や**場所**などのデータベースに関する詳細情報がいくつか表示されます。
# MAGIC 
# MAGIC **権限**タブをクリックし、現在の権限所有者を確認します（ワークスペースの構成によっては、一部の権限がカタログ上の設定から継承されている場合があります）。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 所有権の割り当て（Assigning Ownership）
# MAGIC 
# MAGIC **所有者**フィールドの横にある青色の鉛筆をクリックします。 所有者は個人またはグループとして設定できることに注意してください。 ほとんどの実装では、信頼できるパワーユーザーの1つまたは複数の小さなグループを所有者に指定することで、重要なデータセットへの管理者アクセスを制限し、1人のユーザーが生産性を妨げないようにできます。
# MAGIC 
# MAGIC ここでは、所有者を**Admins**に設定します。これは、すべてのワークスペース管理者を含むデフォルトのグループです。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## データベース権限の変更（Change Database Permissions）
# MAGIC 
# MAGIC まず、すべてのユーザーがデータベースに関するメタデータを確認できるようにします。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. データベースの**権限**タブが選択されていることを確認します
# MAGIC 1. 青色の**付与**ボタンをクリックします
# MAGIC 1. **USAGE**、**READ_METADATA**オプションを選択します
# MAGIC 1. 上部のドロップダウンメニューから**All Users**グループを選択します
# MAGIC 1. **OK**をクリックします
# MAGIC 
# MAGIC これらの権限が更新されたことを確認するには、ユーザーがビューを更新する必要がある場合があることに注意してください。 更新は、Data ExplorerとSQL Editorの両方でほぼリアルタイムでユーザーに反映されるはずです。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ビュー権限の変更（Change View Permissions）
# MAGIC 
# MAGIC ユーザーはこのデータベースに関する情報を表示できるようになりましたが、上記で宣言されたビューもしくはテーブルを操作することはできません。
# MAGIC 
# MAGIC まず、ユーザーがビューを照会できるようにしましょう。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1.  **`ny_users_vw`** を選択します
# MAGIC 1. **権限**タブを選択します
# MAGIC    * ユーザーは、データベースレベルで付与された権限を継承しているはずです。ユーザーがアセットに対して現在持っている権限と、その権限がどこから継承されているかを確認できます
# MAGIC 1. 青色の**付与**ボタンをクリックします
# MAGIC 1. **SELECT**、**READ_METADATA**オプションを選択します
# MAGIC    * 厳密に言うと、**READ_METADATA**は、ユーザーがすでにデータベースから継承しているため、冗長です。 ただし、ビューレベルで付与すると、データベースの権限が取り消された場合でも、ユーザーがこの権限を保持できるようになります。
# MAGIC 1. 上部のドロップダウンメニューから**All Users**グループを選択します
# MAGIC 1. **OK**をクリックします

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## クエリを実行して確認する（Run a Query to Confirm）
# MAGIC 
# MAGIC **SQLエディタ**で、ユーザーは左側の**スキーマブラウザ**を使用して、管理者が制御しているデータベースに移動します。
# MAGIC 
# MAGIC ユーザーは、 **`SELECT * FROM`** と入力してクエリを開始し、ビューの名前にカーソルを合わせると表示される**>>**をクリックして、クエリに挿入します。
# MAGIC 
# MAGIC このクエリでは2つの結果が返されます。
# MAGIC 
# MAGIC **注**：このビューは、まだ権限が設定されていない **`users`** テーブルに対して定義されています。 ユーザーは、ビューで定義されたフィルタを通過するデータの部分にのみアクセスできることに注意してください。このパターンは、元になっている単一のテーブルを使用して、関連する利害関係者のデータへの制御されたアクセスを実現する方法を示しています。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## テーブル権限の変更（Change Table Permissions）
# MAGIC 
# MAGIC 上記と同じ手順を、今度は **`ユーザー`** テーブルに対して実行します。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1.  **`users`** テーブルを選択します
# MAGIC 1. **権限**タブを選択します
# MAGIC 1. 青色の**付与**ボタンをクリックします
# MAGIC 1. **SELECT**、**READ_METADATA**オプションを選択します
# MAGIC 1. 上部のドロップダウンメニューから**All Users**グループを選択します
# MAGIC 1. **OK**をクリックします

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ユーザーに **`DROP TABLE`** の実行を試してもらう（Have Users Attempt to **`DROP TABLE`** ）
# MAGIC 
# MAGIC **SQLエディタ**で、にユーザーにこのテーブルのデータを調べてもらいます。
# MAGIC 
# MAGIC ここでデータを変更するようにユーザーに促します。権限が正しく設定されていれば、これらのコマンドを実行するとエラーが発生します。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 派生データセット用のデータベースを作成する（Create a Database for Derivative Datasets）
# MAGIC 
# MAGIC ほとんどの場合、ユーザーは派生データセットを保存する場所が必要になります。 現在、ユーザーはどの場所にも新しいテーブルを作成できない場合があります（ワークスペース内の既存のACLと、学生が完了した前のレッスンで作成されたデータベースによって異なります）。
# MAGIC 
# MAGIC 以下のセルでは、新しいデータベースを生成し、すべてのユーザーに権限を付与するためのコードを出力します。
# MAGIC 
# MAGIC **注**：ここでは、Data ExplorerではなくSQL Editorを使用して権限を設定します。 クエリ履歴を確認すると、以前Data Explorerから行った権限の変更がすべてSQLクエリとして実行され、ここに記録されていることがわかります（さらに、Data Explorerのほとんどのアクションは、UIフィールドへの入力に使用される対応するSQLクエリでログに記録されます）。

# COMMAND ----------

DA.generate_create_database_with_grants()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## ユーザーに新しいテーブルまたはビューを作成してもらう（Have Users Create New Tables or Views）
# MAGIC 
# MAGIC ユーザーに、この新しいデータベースにテーブルとビューを作成できることを試す時間を与えましょう。
# MAGIC 
# MAGIC **注**：ユーザーには**MODIFY**および**SELECT**権限も付与されているため、すべてのユーザーは、そのピアによって作成されたエンティティをすぐに照会および変更できます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 管理者設定（Admin Configuration）
# MAGIC 
# MAGIC 現在、ユーザーには、デフォルトのカタログ **`hive_metastore`** に対するテーブルACL権限がデフォルトで付与されていません。 次のラボでは、ユーザーがデータベースを作成できることを前提としています。
# MAGIC 
# MAGIC Databricks SQLを使用してデフォルトのカタログにデータベースとテーブルを作成する機能を有効にするには、ワークスペース管理者にDBSQLクエリエディタで次のコマンドを実行してもらいます。
# MAGIC 
# MAGIC <strong><code>GRANT usage, create ON CATALOG &#x60;hive_metastore&#x60; TO &#x60;users&#x60;</code></strong>
# MAGIC 
# MAGIC これが正常に実行されたことを確認するには、次のクエリを実行します。
# MAGIC 
# MAGIC <strong><code>SHOW GRANT ON CATALOG &#x60;hive_metastore&#x60;</code></strong>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
