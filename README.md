
<img src="./img/banner.png" width=800>

ここは、2023/07/29「[Data Engneering Geeks #3 Snowflake hands-on ー Snowpark for Python、dbt、AirflowでつくるMLパイプライン ー](https://datumstudio.jp/information/0729_snowflake_hands-on_seminar/)」にて使用するソースコードを配布するためのリポジトリです。

# 構成

|フォルダ名|説明|
|:-|:-|
|handson|ソースファイル置き場|
|handson/notebook|「Notebook から 使ってみよう」「ML を動かしてみよう」パートで使用する Notebook |
|handson/dbt|「dbt から 使ってみよう」パートで使用するソースコード|
|handson/airflow|「Airflow から 使ってみよう」パートで使用するソースコード|
|handson_guide|ハンズオンガイドの現行と画像置き場|

# 実行環境について
## 環境作成
実施環境は Docker コンテナで用意しています。

**起動コマンド例**

```
$ docker compose up -d && docker compose exec handson bash
```

## 操作
実行環境にて、[ハンズオンガイド](./handson_guide/handson_guide.md)に沿って、操作していただきます。
