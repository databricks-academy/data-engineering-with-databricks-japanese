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
# MAGIC Databricks SQLでは、多くの追加カスタムメソッド（Delta Lake SQLの構文全体も含めて）を備えたANSIに準拠したSQLを使用できますが、一部のシステムから移行してきたユーザーは、機能（特に制御流れやエラーハンドリングの機能）が足りないと感じるかもしれません。
# MAGIC 
# MAGIC Databricksノートブックを使用すると、ユーザーはSQLとPythonを書いてロジックをセルごとに実行できます。 PySparkは、SQLクエリの実行を幅広くサポートしており、テーブルとテンポラリビューと簡単にデータを交換できます。
# MAGIC 
# MAGIC 一部のPythonの概念をマスターするだけで、SQLに精通しているエンジニアやアナリストは新しい強力なデザイン方法を身に着けられます。 このレッスンでは、言語全体を教えるのではなく、Databricksでより拡張可能なSQLプログラムを即座に書くのに活用できる機能に焦点を当てています。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * 複数行のPython文字列の表示と操作
# MAGIC * 変数と関数の定義
# MAGIC * 変数置き換えのためののf文字列の使用方法

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 文字列（Strings）
# MAGIC 一重引用符（ **`'`** ）または二重引用符（ **`"`** ）で囲われている文字は文字列です。

# COMMAND ----------

"This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 文字列がどのように表示されるかを確認するには **`print()`** を呼び出します。

# COMMAND ----------

print("This is a string")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 文字列を三重引用符（ **`"""`** ）で囲むことで、複数行での使用が可能になります。

# COMMAND ----------

print("""
This 
is 
a 
multi-line 
string
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これのおかがで、SQLクエリをPython文字列に変えるのは簡単です。

# COMMAND ----------

print("""
SELECT *
FROM test_table
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC SQLをPythonセルから実行する際、 **`spark.sql()`** に引数として文字列を渡します。

# COMMAND ----------

spark.sql("SELECT 1 AS test")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC クエリを通常のSQLノートブックと同じ方法で表示するには、この関数に **`display()`** を呼び出します。

# COMMAND ----------

display(spark.sql("SELECT 1 AS test"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC **注**：Pythonの文字列しか載っていないセルを実行しても、文字列が表示されるだけです。 文字列で **`print()`**  を使用してもノートブックにレンダリングされるだけです。
# MAGIC 
# MAGIC Pythonを使用してSQLが含まれている文字列を実行するには、 **`spark.sql()`** を呼び出す際、文字列を渡す必要があります。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 変数（Variables）
# MAGIC Pythonの変数は **`=`** を使用して割り当てできます。
# MAGIC 
# MAGIC Python変数は英字で始まる必要があり、英数字とアンダースコアのみを含められます。 （アンダースコアで始まる変数名は有効ですが、通常は特別な場面で使用されます。）
# MAGIC 
# MAGIC 多くのPythonプログラマーは、すべての変数に小文字とアンダースコアのみを使用するスネークケースを好みます。
# MAGIC 
# MAGIC 以下のセルでは **`my_string`** という変数を作成します。

# COMMAND ----------

my_string = "This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC この変数があるセルを実行するとその値が返されます。

# COMMAND ----------

my_string

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ここの出力は、セルに **`"This is a string"`** と書いて実行した場合のと同じ出力になります。
# MAGIC 
# MAGIC 表示時に表示されるとおり、引用符は文字列の一部ではないことに注意してください。

# COMMAND ----------

print(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC この変数は、文字列と同じように使用できます。
# MAGIC 
# MAGIC 文字列の連結（2つの文字列を結合すること）は、 **`+`** を使用して実行できます。

# COMMAND ----------

print("This is a new string and " + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 文字列変数を他の文字列変数と結合できます。

# COMMAND ----------

new_string = "This is a new string and "
print(new_string + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 関数（Functions）
# MAGIC 関数を使用すると、ローカル変数を引数として指定して、カスタムロジックを適用できます。 キーワード **`def `** に続いて関数名、および括弧で囲んで関数に渡したい変数引数を記載して関数を定義します。 そして、関数ヘッダの最後に **`:`** があります。
# MAGIC 
# MAGIC 注：Pythonでは、インデントが重要です。 以下のセルで、関数のロジックが左マージンからインデントされていることがわかります。 このレベルにインデントされたコードはすべて関数の一部です。
# MAGIC 
# MAGIC 以下の関数は、1つの引数（ **`arg`** ）を取り、それを表示します。

# COMMAND ----------

def print_string(arg):
    print(arg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 引数として文字列を渡すと、それが表示されます。

# COMMAND ----------

print_string("foo")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 引数として変数を渡すこともできます。

# COMMAND ----------

print_string(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 多くの場合、他の場所で使用するために関数の結果を返したいことがあります。 そのために、 **`return`** キーワードを使用します。
# MAGIC 
# MAGIC 以下の関数は、引数を連結して新しい文字列を作成します。 関数と引数の両方に、変数と同じように任意の名前を付けられることに注意してください（同じルールに従います）。

# COMMAND ----------

def return_new_string(string_arg):
    return "The string passed to this function was " + string_arg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC この関数を実行すると、出力が返されます。

# COMMAND ----------

return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC それを変数に割り当てると、他の場所で再利用するために出力をキャプチャできます。

# COMMAND ----------

function_output = return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC この変数には関数は含まれていません。関数の結果（文字列）のみが含まれています。

# COMMAND ----------

function_output

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## f-文字列（F-strings）
# MAGIC Python文字列の前に文字 **`f`** を追加することで、中括弧（ **`{}`** ）内に変数や評価するPythonコードを挿入することで、それらをPython文字列に挿入できます。
# MAGIC 
# MAGIC 以下のセルを評価して、文字列変数の置換を確認しましょう。

# COMMAND ----------

f"I can substitute {my_string} here"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 次のセルは、関数によって返された文字列を挿入します。

# COMMAND ----------

f"I can substitute functions like {return_new_string('foobar')} here"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC これを三重引用符と組み合わせると、以下のようにパラグラフまたはリストをフォーマットできます。

# COMMAND ----------

multi_line_string = f"""
I can have many lines of text with variable substitution:
  - A variable: {my_string}
  - A function output: {return_new_string('foobar')}
"""

print(multi_line_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC もしくは、SQLクエリをフォーマットすることもできます。

# COMMAND ----------

table_name = "users"
filter_clause = "WHERE state = 'CA'"

query = f"""
SELECT *
FROM {table_name}
{filter_clause}
"""

print(query)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
