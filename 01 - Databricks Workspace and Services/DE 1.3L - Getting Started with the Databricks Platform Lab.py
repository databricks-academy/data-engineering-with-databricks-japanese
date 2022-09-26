# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="5f2cfc0b-1998-4182-966d-8efed6020eb2"/>
# Databricksプラットフォーム入門（Getting Started with the Databricks Platform）

このノートブックでは、Databricksデータサイエンスおよびエンジニアリングのワークスペースの基本機能の一部を実践的に説明します。

## 学習目標（Learning Objectives）
このラボでは、以下のことが学べます。
- ノートブックの名前を変更してデフォルト言語を変更する
- クラスタをアタッチする
- MAGICコマンド  **`%run`**  を使う
- PythonセルとSQLセルを実行する
- Markdownセルを作成する

# COMMAND ----------

# MAGIC %md <i18n value="05dca5e4-6c50-4b39-a497-a35cd6d99434"/>
# ノートブックの名称変更（Renaming a Notebook）

ノートブックの名前の変更は簡単です。 このページの上部にある名前をクリックしてから、名前を変更します。 後で必要になったときにこのノートブックに簡単に戻れるように、既存の名前の末尾に短いテスト文字列を追加します。

# COMMAND ----------

# MAGIC %md <i18n value="f07b8dd7-436d-4719-9c17-18cd47f493fe"/>
# クラスタのアタッチ（Attaching a cluster）

ノートブックでセルを実行するには、クラスタによって提供されるコンピュートリソースが必要です。 ノートブックでセルを初めて実行するときに、まだクラスタがアタッチされていない場合、クラスタにアタッチするように指示されます。

このページの左上隅付近にあるドロップダウンをクリックして、このノートブックにクラスタをアタッチします。 以前に作成したクラスタを選択します。 これにより、ノートブックの実行状態がクリアされ、選択したクラスタにノートブックが接続されます。

ドロップダウンメニューには、必要に応じてクラスタを起動または再起動するオプションがあることにご注意ください。 また、1回の動作でクラスタをデタッチして再アタッチすることもできます。 これは、必要なときに実行状態をクリアする場合に便利です。

# COMMAND ----------

# MAGIC %md <i18n value="68805a5e-3b2c-4f79-819f-273d4ca95137"/>
# %runを使う（Using %run）

どのような種類の複雑なプロジェクトでも、よりシンプルで再利用可能なコンポーネントに分解する機能があれば便利です。

Databricksノートブックのコンテキストでは、この機能は  **`%run`**  MAGICコマンドによって提供されます。

このように使用すると、変数、関数、コードブロックが現在のプログラミングコンテキストの一部になります。

次の例を考えてみましょう：

 **`Notebook_A`** には4つのコマンドがあります：
  1.   **`name = "John"`**  
  2.   **`print(f"Hello {name}")`**   
  3.   **`%run ./Notebook_B`**  
  4.   **`print(f"Welcome back {full_name}")`**

 **`Notebook_B`**  にはコマンドが1つしかありません：
  1.   **`full_name = f"{name} Doe"`**  

 **`Notebook_B`**  を実行すると、変数  **`name`**  が  **`Notebook_B`**  では定義されていないため、実行に失敗します

同様に、  **`Notebook_A`**  は、  **`Notebook_A`**  で同じく定義されていない変数  **`full_name`**  を使用しているため、失敗すると思うかもしれませんが、そうではありません！

実際に起きるのは、2つのノートブックが以下のようにマージされ、 **それから**実行されるのです：
1.  **`name = "John"`** 
2.  **`print(f"Hello {name}")`** 
3.  **`full_name = f"{name} Doe"`** 
4.  **`print(f"Welcome back {full_name}")`**

そしてこの結果、期待通りに動作します：
*  **`Hello John`** 
*  **`Welcome back John Doe`**

# COMMAND ----------

# MAGIC %md <i18n value="260e99b3-4126-41b7-8210-b6ff01b98790"/>
このノートブックを含むフォルダには、 **`ExampleSetupFolder`** というのサブフォルダが含まれています。このサブフォルダには同様に、 **`example-setup`** というのノートブックが含まれています。

この単純なノートブックは変数 **`my_name`** を宣言し、それを **`None`** に設定してから、 **`example_df`** というのデータフレームを作成します。

example-setupノートブックを開き、名前が **`None`** ではなく、自分の名前 （または誰かの名前）を引用符で囲むように変更します。次の2つのセルは、 **`AssertionError`** を出さずに実行されます。

<img src="https://files.training.databricks.com/images/icon_note_24.png"> **`_utility-methods`**  と **`DBAcademyHelper`** もコースの設定に含まれているがこの練習には不要なので無視してください。

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# MAGIC %md <i18n value="ece094f7-d013-4b24-aa54-e934f4ab7dbd"/>
## Pythonセルを実行する（Run a Python cell）

次のセルを実行して、 **`example_df`** データフレームを表示することにより、  **`example-setup`** ノートブックが実行されたことを確認してください。 このテーブルは、値が増加する16行で構成されています。

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %md <i18n value="ce392afd-2e73-4a51-adc4-7d654dad6215"/>
# 言語の変更（Change Language）

このノートブックのデフォルト言語が、Pythonに設定されていることに注意してください。 これを変更するには、ノートブック名の右にある**Python**ボタンをクリックします。 デフォルト言語をSQLに変更します。

セルの有効性を維持するために、Pythonセルには<strong><code>&#37;python</code></strong>MAGICコマンドが先頭に自動的に追加されていることに注意してください。 この操作によって実行状態もクリアされることに注意してください。

# COMMAND ----------

# MAGIC %md <i18n value="dfce7fd1-08e8-4cc3-92ac-a2eb74f804ef"/>
# Markdownセルを作成する（Create a Markdown Cell）

このセルの下に新しいセルを追加します。 少なくとも次の要素を含むMarkdownをいくつか追加します：
* ヘッダ
* 箇条書き
* リンク（HTMLまたはMarkdown記法で好みのものを使用）

# COMMAND ----------

# MAGIC %md <i18n value="a54470bc-2a69-4a34-acbb-fe28c4dee284"/>
## SQLセルを実行する（Run a SQL cell）

SQLを使用してDeltaテーブルを照会するには、次のセルを実行します。 これは、すべてのDBFSインストールに含まれる、DataBricks提供のサンプルデータセットに基づいているテーブルに対し、単純なクエリを実行します。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.datasets}/nyctaxi-with-zipcodes/data`

# COMMAND ----------

# MAGIC %md <i18n value="7499c6b6-b3f3-4641-88d9-5a260d3c11f8"/>
次のセルを実行して、このテーブルが基づいている基本ファイルを表示します。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="a17b5667-53bc-4f8a-8601-5599f4ebb819"/>
# ノートブックの状態をクリアする (Clearing notebook state)
ノートブックで定義されているすべての変数をクリアして、最初からやり直す必要な場合があります。例えば、セルを単独でテストしたり、実行状態をリセットしたりする場合にノートブックの状態をクリアします。
**消去** メニューにアクセスし、**状態とセルの出力をクリア** を選択します。
下のセルを実行してみてください。上のセルを再実行するまで、以前に定義した変数が定義されていないことに注目してください。

# COMMAND ----------

print(my_name)

# COMMAND ----------

# MAGIC %md <i18n value="8bff18c2-3ecf-484a-9a8c-dadab7eaf0a1"/>
# 変更を確認する（Review Changes）

Databricks Repoを使用してこのデータをワークスペースにインポートした場合、このページの左上隅にある **`公開`** ブランチボタンをクリックして、Repoダイアログを開いてください。 変更が3つあります：
1. **削除**項目で以前のノートブック名前
1. **追加**項目で新しいノートブックの名前
1. **変更**項目で上記のMarkdownセルの作成

ダイアログを使用して変更を元に戻し、このノートブックを元の状態に復元します。

# COMMAND ----------

# MAGIC %md <i18n value="cb3c335a-dd4c-4620-9f10-6946250f2e02"/>
## まとめ（Wrapping Up）

このラボでは、ノートブックの操作、新しいセルの作成、ノートブック内でのノートブックの実行を学びました。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
