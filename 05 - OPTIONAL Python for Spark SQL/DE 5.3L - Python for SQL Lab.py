# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="358d2c22-9d78-4888-a7ec-54b7d5f3db64"/>
# MAGIC 
# MAGIC # Databricks SQLに最低限必要なPythonのラボ（Just Enough Python for Databricks SQL Lab）
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このラボでは、以下のことが学べます。
# MAGIC * 基本的なPythonコードを確認し、コードを実行したときに期待すべき結果を説明する
# MAGIC * Python関数の制御流れ文について考えて説明する
# MAGIC * SQLクエリをPython関数で囲むことでパラメーターを追加する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.3L

# COMMAND ----------

# MAGIC %md <i18n value="97cba873-1459-478f-831b-b52fc54265eb"/>
# MAGIC 
# MAGIC # Python基本の復習（Reviewing Python Basics）
# MAGIC 
# MAGIC 前のノートブックでは、 **`spark.sql()`** を使用してPythonから任意のSQLコマンドを実行する方法について簡単に説明しました。
# MAGIC 
# MAGIC 以下の3つのセルを見てみましょう。 各セルを実行する前に次のことを明らかにしましょう：
# MAGIC 1. セル実行の期待される出力
# MAGIC 1. どのようなロジックが実行されているか
# MAGIC 1. 結果として生じる環境の状態の変更
# MAGIC 
# MAGIC 次に、セルを実行し、結果を期待していた結果と比較して、以下の説明を参照してください。

# COMMAND ----------

course = "dewd"

# COMMAND ----------

spark.sql(f"SELECT '{course}' AS course_name")

# COMMAND ----------

df = spark.sql(f"SELECT '{course}' AS course_name")
display(df)

# COMMAND ----------

# MAGIC %md <i18n value="bc8fda28-92ad-4cd5-aa24-34022269698a"/>
# MAGIC 
# MAGIC 1. **コマンド5**は変数に文字列を割り当てます。 変数の割り当てが成功すると、ノートブックに出力は表示されません。 新しい変数が現在の実行環境に追加されます。
# MAGIC 1. **コマンド6**はSQLクエリを実行し、DataFrameのスキーマを **`DataFrame`** という単語と一緒に表示します。 この場合、SQLクエリは文字列を選択するためだけのものなので、環境に変更はありません。
# MAGIC 1. **コマンド7**は同じSQLクエリを実行し、DataFrameの出力を表示します。 この **`display()`** と **`spark.sql()`** の組み合わせは、 **`%sql`** セルでロジックを実行することを最もよく反映しています。結果がクエリによって返される場合、結果は常にフォーマットされたテーブルで出力されます。その一方で一部のクエリはテーブルまたはデータベースを操作します。その場合、 **`OK`** という単語が出力されて正常に実行されたことを示します。 この場合、このコードを実行しても環境は変更されません。

# COMMAND ----------

# MAGIC %md <i18n value="ef0b350e-c470-4e89-9617-948e49dd1710"/>
# MAGIC 
# MAGIC ## 開発環境のセットアップ（Setting Up a Development Environment）
# MAGIC 
# MAGIC このコースを通して、次のセルに似ているロジックを使用して、現在ノートブックを実行しているユーザーに関する情報を取得し、分離された開発データベースを作成します。
# MAGIC 
# MAGIC  **`re`**  ライブラリは、Python<a href="https://docs.python.org/3/library/re.html" target="_blank">標準の正規表現用ライブラリ</a>です。
# MAGIC 
# MAGIC Databricks SQLには、 **`current_user()`** という現在のユーザー名を取得するための特別なメソッドがあります。 **`.first()[0]`** コードは、 **`spark.sql()`** で実行されたクエリの最初の列の最初の行をキャプチャするための簡単な裏技です（この場合、行と列が1つしかないとわかっている上で、これを安全に実行します）。
# MAGIC 
# MAGIC 以下の他のすべてのロジックは、単なる文字列のフォーマットです。

# COMMAND ----------

import re

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
schema_name = f"dbacademy_{clean_username}_{course}_5_3l"
working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

print(f"username:    {username}")
print(f"schema_name:     {schema_name}")
print(f"working_dir: {working_dir}")

# COMMAND ----------

# MAGIC %md <i18n value="1273f7a3-823a-4b1f-914a-ce6eaaa867b3"/>
# MAGIC 
# MAGIC 以下では、このロジックに単純な制御流れ文を追加して、このユーザー固有のデータベースを作成および使用します。
# MAGIC 
# MAGIC 任意ですが、繰り返し実行し、このデータベースをリセットしてすべてのコンテンツを削除します。 （パラメーター **`reset`** のデフォルト値は **`True`** であることに注意してください）。

# COMMAND ----------

def create_database(course, reset=True):
    import re

    username = spark.sql("SELECT current_user()").first()[0]
    clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
    schema_name = f"dbacademy_{clean_username}_{course}_5_3l"
    working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

    print(f"username:    {username}")
    print(f"schema_name: {schema_name}")
    print(f"working_dir: {working_dir}")

    if reset:
        spark.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        dbutils.fs.rm(working_dir, True)
        
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name} LOCATION '{working_dir}/{schema_name}.db'")
    spark.sql(f"USE {schema_name}")
    
create_database(course)

# COMMAND ----------

# MAGIC %md <i18n value="cfa0adf3-cc23-4ba1-8daf-2c70af7fa079"/>
# MAGIC 
# MAGIC 定義されているこのロジックは、教育目的で共有ワークスペース内の学生を分離することを目的としていますが、同じ基本設計を利用して、本番環境にプッシュする前に分離された環境で新しいロジックをテストできます。

# COMMAND ----------

# MAGIC %md <i18n value="1c994e19-2b72-45c3-a174-8a7e21701688"/>
# MAGIC 
# MAGIC ## エラーの適切な処理（Handling Errors Gracefully）
# MAGIC 
# MAGIC 以下の関数のロジックを確認しましょう。
# MAGIC 
# MAGIC 新しいデータベースを宣言したばかりで、現在はテーブルが含まれていないことに注意してください。

# COMMAND ----------

def query_or_make_demo_table(table_name):
    try:
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Displayed results for the table {table_name}")
        
    except:
        spark.sql(f"CREATE TABLE {table_name} (id INT, name STRING, value DOUBLE, state STRING)")
        spark.sql(f"""INSERT INTO {table_name}
                      VALUES (1, "Yve", 1.0, "CA"),
                             (2, "Omar", 2.5, "NY"),
                             (3, "Elia", 3.3, "OH"),
                             (4, "Rebecca", 4.7, "TX"),
                             (5, "Ameena", 5.3, "CA"),
                             (6, "Ling", 6.6, "NY"),
                             (7, "Pedro", 7.1, "KY")""")
        
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Created the table {table_name}")

# COMMAND ----------

# MAGIC %md <i18n value="5a449d08-9811-4b0d-9004-74b8bb04eef5"/>
# MAGIC 
# MAGIC 次のセルを実行する前に、次のことを確認してください：
# MAGIC 1. セル実行の期待される出力
# MAGIC 1. どのようなロジックが実行されているか
# MAGIC 1. 結果として生じる環境の状態の変更

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md <i18n value="8ddb2ea1-9e4e-4ac7-a369-ff984114653f"/>
# MAGIC 
# MAGIC 次に、以下の同じクエリを実行する前に、同じ3つの質問に答えてください。

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md <i18n value="6efbda51-9c51-440a-aaaf-7276ad175398"/>
# MAGIC 
# MAGIC - 最初の実行では、テーブル **`demo_table`**  はまだ存在していませんでした。 そのため、テーブルの内容を返そうとするとエラーが発生し、その結果、 **`except`** ブロックのロジックが実行されました。 このブロックは：
# MAGIC   1. テーブルを作成した
# MAGIC   1. 値を挿入した
# MAGIC   1. テーブルのコンテンツを表示した
# MAGIC - 2回目の実行では、テーブル **`demo_table`** がすでに存在するため、 **`try`** ブロックの最初のクエリはエラーなしで実行されます。 その結果、環境内で何も変更せずに、クエリの結果を表示しただけです。

# COMMAND ----------

# MAGIC %md <i18n value="a0f957ea-7604-46b9-9b06-d672b73efcec"/>
# MAGIC 
# MAGIC ## SQLをPythonに適応させる（Adapting SQL to Python）
# MAGIC 上で作成したデモテーブルに対する次のSQLクエリについて考えてみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, value 
# MAGIC FROM demo_table
# MAGIC WHERE state = "CA"

# COMMAND ----------

# MAGIC %md <i18n value="c4abcb35-3733-4565-8f8c-0df4b23f1e71"/>
# MAGIC 
# MAGIC これは、PySparkAPIと **`display`**  関数を使用して次のように表現することもできます。

# COMMAND ----------

results = spark.sql("SELECT id, value FROM demo_table WHERE state = 'CA'")
display(results)

# COMMAND ----------

# MAGIC %md <i18n value="6a4e7e96-c53a-4b8e-abf5-412fe4170c27"/>
# MAGIC 
# MAGIC この簡単な例を使用して、任意の機能を追加するPython関数の作成を練習してみましょう。
# MAGIC 
# MAGIC 次のような関数を目指します：
# MAGIC *  **` demo_table `** というのテーブルの **`id `** 列と **`value`** 列のみを含むクエリに基づく
# MAGIC *  **`state`** によるクエリのフィルタリングが可能。デフォルトの動作では、すべての状態が含まれる
# MAGIC * オプションで **`display`** 関数を使用してクエリの結果をレンダリングする。デフォルトの動作ではレンダリングしない
# MAGIC * 次のものを返します：
# MAGIC   *  **`render_results`** がFalseの場合、クエリ結果オブジェクト（PySpark DataFrame）
# MAGIC   *  **`render_results`** がTrueの場合、 **`None`** の値
# MAGIC 
# MAGIC ストレッチ目標：
# MAGIC *  **`state`** パラメーターに渡された値に2つの大文字が含まれていることを確認するためのassert文を追加する
# MAGIC 
# MAGIC 基礎となるロジックを以下に記載しています：

# COMMAND ----------

# TODO
def preview_values(state=<FILL-IN>, render_results=<FILL-IN>):
    query = <FILL-IN>

    if state is not None:
        <FILL-IN>

    if render_results
        <FILL-IN>

# COMMAND ----------

# MAGIC %md <i18n value="060207a1-a34c-4817-abee-f6e0b9c3b48a"/>
# MAGIC 
# MAGIC 以下のassert文を使用して、関数が意図したとおりに機能するかどうかを確認できます。

# COMMAND ----------

import pyspark.sql.dataframe

assert type(preview_values()) == pyspark.sql.dataframe.DataFrame, "Function should return the results as a DataFrame"
assert preview_values().columns == ["id", "value"], "Query should only return **`id`** and **`value`** columns"

assert preview_values(render_results=True) is None, "Function should not return None when rendering"
assert preview_values(render_results=False) is not None, "Function should return DataFrame when not rendering"

assert preview_values(state=None).count() == 7, "Function should allow no state"
assert preview_values(state="NY").count() == 2, "Function should allow filtering by state"
assert preview_values(state="CA").count() == 2, "Function should allow filtering by state"
assert preview_values(state="OH").count() == 1, "Function should allow filtering by state"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
