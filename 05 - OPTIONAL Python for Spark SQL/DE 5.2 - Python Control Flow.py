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
# MAGIC # Databricks SQLに最低限必要なPython（Just Enough Python for Databricks SQL）
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC *  **`if`**  /  **`else`** を活用する
# MAGIC * エラーがノートブックの実行にどのような影響を与えるかを説明する
# MAGIC *  **`assert`** で簡単なテストを書く
# MAGIC * エラーを処理するのに **`try`**  /  **`except`** を使用する

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## if/else
# MAGIC 
# MAGIC  **`if`**  /  **`else`** は多くのプログラミング言語で一般的です。
# MAGIC 
# MAGIC SQLには **`CASE WHEN ... ELSE`** という構造体があってif/elseに似ていることにご注意ください。
# MAGIC 
# MAGIC <strong>テーブルまたはクエリ内の条件を評価する場合は、 **`CASE WHEN`** を使用します。</strong>
# MAGIC 
# MAGIC Python制御流れは、クエリ以外の条件を評価するために使用する必要があります。
# MAGIC 
# MAGIC 詳細は後ほど説明します。 まずは、 **`"beans"`** を使用した一例を示します。

# COMMAND ----------

food = "beans"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC  **`if`** および **`else`** の操作というのは、要するに実行環境で特定の条件が真であるかどうかを評価することです。
# MAGIC 
# MAGIC Pythonには、次の比較演算子があることに注意してください：
# MAGIC 
# MAGIC | 構文          | 操作    |
# MAGIC | ----------- | ----- |
# MAGIC |  **`==`**     | 等価    |
# MAGIC |  **`>`**   | より大きい |
# MAGIC |  **`<`**   | より小さい |
# MAGIC |  **`>=`**  | 以上    |
# MAGIC |  **`<=`**  | 以下    |
# MAGIC |  **`!=`**     | 不等価   |
# MAGIC 
# MAGIC 以下の文章を読み上げることで、プログラムの制御流れを説明できます。

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 期待どおり、 **`food`** 変数は文字列リテラル **`"beans"`** であるため、 **`if`** 文が **`True`** と評価され、最初のprint文が評価されました。
# MAGIC 
# MAGIC 変数に別の値を割り当てましょう。

# COMMAND ----------

food = "beef"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これで、最初の条件は **`False`** と評価されます。
# MAGIC 
# MAGIC 次のセルを実行するとどうなると思いますか？

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 変数に新しい値を割り当てるたびに、これにより古い変数が完全に消去されることに注意してください。

# COMMAND ----------

food = "potatoes"
print(food)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Pythonキーワード **`elif`** （ **`else`**  +  **`if`** の省略）を使用すると複数の条件を評価できます。
# MAGIC 
# MAGIC 条件は上から下に評価されることに注意してください。 条件がtrueと評価されると、他の条件は評価されなくなります。
# MAGIC 
# MAGIC  **`if`**  /  **`else`** 制御流れのパターン：
# MAGIC 1.  **`if`** 句が必要
# MAGIC 1. 任意の数の **`elif`** 句を含められる
# MAGIC 1. 1つの **`else`** 句のみを含められる

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
elif food == "potatoes":
    print(f"My favorite vegetable is {food}")
elif food != "beef":
    print(f"Do you have any good recipes for {food}?")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 上記のロジックを関数にカプセル化することで、グローバルに定義された変数を参照するのではなく、このロジックとフォーマットを任意の引数で再利用できます。

# COMMAND ----------

def foods_i_like(food):
    if food == "beans":
        print(f"I love {food}")
    elif food == "potatoes":
        print(f"My favorite vegetable is {food}")
    elif food != "beef":
        print(f"Do you have any good recipes for {food}?")
    else:
        print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ここでは、文字列 **`"bread"`** を関数に渡します。

# COMMAND ----------

foods_i_like("bread")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 関数を評価するときに、文字列 **`"bread"`** をローカルに **`food`** 変数に割り当てると、ロジックは期待どおりに動作します。
# MAGIC 
# MAGIC ノートブックで以前に定義された  **`food`** 変数の値は上書きされないことに注意してください。

# COMMAND ----------

food

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## try/except
# MAGIC 
# MAGIC  **`if`**  /  **`else`** 句は、条件文の評価に基づいて条件付きロジックを定義できます。その一方で **`try`**  /  **`except`** は、堅牢なエラー処理に重点を置いています。
# MAGIC 
# MAGIC まずは、簡単な関数を見てみましょう。

# COMMAND ----------

def three_times(number):
    return number * 3

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC この関数では、整数値に3を掛けたいとします。
# MAGIC 
# MAGIC 以下のセルはこの動作を示しています。

# COMMAND ----------

three_times(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 文字列が関数に渡された場合に何が起こるかに注意してください。

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC この場合、エラーは発生しませんが、目的の結果も得られません。
# MAGIC 
# MAGIC  **`assert`** 文を使用すると、Pythonコードの簡単なテストを実行できます。  **`assert`** 文がtrueと評価された場合、何も起こりません。
# MAGIC 
# MAGIC falseと評価された場合、エラーが発生します。
# MAGIC 
# MAGIC 次のセルを実行して、数値 **`2`** が整数であることをアサートします

# COMMAND ----------

assert type(2) == int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 次のセルからコメントアウトを外して実行し、文字列 **`"2"`** が整数であることをアサートします。
# MAGIC 
# MAGIC  **`AssertionError`** が投げられるはずです。

# COMMAND ----------

# assert type("2") == int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 期待どおり、文字列 **`"2"`** は整数ではありません。
# MAGIC 
# MAGIC Python文字列には、以下のとおり、数値として安全にキャストできるかどうかを報告するプロパティがあります。

# COMMAND ----------

assert "2".isnumeric()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 文字列の数値は、APIクエリの結果で使用されたり、JSONまたはCSVファイルの未加工のレコードで使用されたり、またはSQLクエリによって返されたりして、一般的です。
# MAGIC 
# MAGIC  **`int()`**  and  **`float()`** は、値を数値型にキャストするための2つの一般的なメソッドです。
# MAGIC 
# MAGIC  **`int`** は常に整数になり、 **`float`** は常に小数の値を持ちます。

# COMMAND ----------

int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Pythonは、数値を含む文字列を数値型にはキャストしますが、他の文字列を数値に変更することはできません。
# MAGIC 
# MAGIC 次のセルからコメントアウトを外して試してみましょう：

# COMMAND ----------

# int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC エラーが発生すると、ノートブックスクリプトの実行が停止することに注意してください。ノートブックが本番ジョブとしてスケジュールされている場合、エラー後のすべてのセルはスキップされます。
# MAGIC 
# MAGIC エラーを投げる可能性のあるコードを **`try`** 文で囲むと、エラーが発生したときに代替ロジックを定義できます。
# MAGIC 
# MAGIC 以下は、これを示す簡単な関数です。

# COMMAND ----------

def try_int(num_string):
    try:
        int(num_string)
        result = f"{num_string} is a number."
    except:
        result = f"{num_string} is not a number!"
        
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 数値文字列が渡されると、関数は結果を整数として返します。

# COMMAND ----------

try_int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 数値以外の文字列が渡されると、有益なメッセージが表示されます。
# MAGIC 
# MAGIC **注**：エラーが発生し値は返されなかったにもかかわらず、エラーは**投げられませんでした**。 エラーを抑制するロジックを実装すると、ロジックが静かに失敗するおそれがります。

# COMMAND ----------

try_int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 以下では、以前の関数が更新され、エラーを処理して有益なメッセージを返すためのロジックが含まれています。

# COMMAND ----------

def three_times(number):
    try:
        return int(number) * 3
    except ValueError as e:
        print(f"You passed the string variable '{number}'.\n")
        print(f"Try passing an integer instead.")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これで、関数は文字列として渡された数値を処理できます。

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC また、文字列が渡されると、有益なメッセージを表示します。

# COMMAND ----------

three_times("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 現在の実装だと、このロジックはこのロジックのインタラクティブな実行にのみ役立つことに注意してください（メッセージは現在どこのログにも記録されておらず、コードは目的の形式でデータを返しません。表示されたメッセージに対応するにはユーザーの介入が必要になります）。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## SQLクエリにPythonの制御流れを適用する（Applying Python Control Flow for SQL Queries）
# MAGIC 
# MAGIC 上記の例は、Pythonでこのようなデザインを使用する基本原則を示していますが、このレッスンの目的は、これらの概念をDatabricksでSQLロジックを実行するために適用する方法を学ぶことです。
# MAGIC 
# MAGIC Pythonで実行するためにSQLセルを変換する方法をもう一度見てみましょう。
# MAGIC 
# MAGIC **注**：次のセットアップスクリプトは、分離された実行環境を保証します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_tmp_vw(id, name, value) AS VALUES
# MAGIC   (1, "Yve", 1.0),
# MAGIC   (2, "Omar", 2.5),
# MAGIC   (3, "Elia", 3.3);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 以下のSQLセルを実行して、このテンポラリビューの内容をプレビューします。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC PythonセルでSQLを実行するには、文字列クエリを  **`spark.sql()`** に渡します。

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ただし、  **`spark.sql()`** を使用してクエリを実行すると、結果が表示されるのではなく、DataFrameとして返されます。 以下のコードは、結果をキャプチャして表示するために拡張されています。

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
result = spark.sql(query)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 関数で単純な **`if`** 句を使用すると、任意のSQLクエリを実行し、オプションで結果を表示し、常に結果のDataFrameを返すことができます。

# COMMAND ----------

def simple_query_function(query, preview=True):
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

result = simple_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC クエリの目的がデータのプレビューを返すのではなく、テンポラリビューを作成することであるため、以下では、別のクエリを実行しプレビューを **`False`** に設定します。

# COMMAND ----------

new_query = "CREATE OR REPLACE TEMP VIEW id_name_tmp_vw AS SELECT id, name FROM demo_tmp_vw"

simple_query_function(new_query, preview=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これで、組織のニーズに応じてさらにパラメーター化できる単純な拡張可能な関数ができました。
# MAGIC 
# MAGIC 例えば、以下のクエリのとおり、悪意のあるSQLから自社を保護したいとします。

# COMMAND ----------

injection_query = "SELECT * FROM demo_tmp_vw; DROP DATABASE prod_db CASCADE; SELECT * FROM demo_tmp_vw"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC  **`find()`** メソッドを使用してセミコロンを探すことで、複数のSQL文が含まれていないかをテストできます。

# COMMAND ----------

injection_query.find(";")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC セミコロンが見つからなかった場合、メソッドは **`-1`** を返します

# COMMAND ----------

injection_query.find("x")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC その知識を活かして、クエリ文字列でセミコロンの簡単な検索を定義し、見つかった場合（  **`-1`** じゃない場合）はカスタムエラーメッセージを表示できます

# COMMAND ----------

def injection_check(query):
    semicolon_index = query.find(";")
    if semicolon_index >= 0:
        raise ValueError(f"Query contains semi-colon at index {semicolon_index}\nBlocking execution to avoid SQL injection attack")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **注**：ここに示す例は素朴ですが、一般的な原則を示すことを目的としています。
# MAGIC 
# MAGIC SQLクエリに渡されるテキストを信頼できないユーザーが渡すときは、警戒したほうが良いです。
# MAGIC 
# MAGIC また、 **`spark.sql()`** を使用して実行できるクエリは1つだけであるため、セミコロンを含むテキストは常にエラーを投げることに注意してください。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 次のセルからコメントアウトを外して試してみましょう：

# COMMAND ----------

# injection_check(injection_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC このメソッドを以前のクエリ関数に追加すれば、実行前に潜在的な脅威がないか各クエリを評価する、より堅牢な関数ができます。

# COMMAND ----------

def secure_query_function(query, preview=True):
    injection_check(query)
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 予想どおり、安全なクエリで通常のパフォーマンスが見られます。

# COMMAND ----------

secure_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ただし、不正なロジックが実行された場合は実行を防ぎます。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
