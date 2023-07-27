connection_parameters = {
    'account': '<org_name>-<account_name>',  # お使いの Snowflake アカウントの識別子
    'user': '<your_username>',  # Snowflake アカウントにサインインするユーザー名
    'password': '<your_password>',  # ユーザーのパスワード
    'role': 'SYSADMIN',  # ハンズオンで使用するロール。変更は不要です
    'database': 'DEGEEKS_HO_DB',  # ハンズオンで使用するデータベース。存在しなければ新規作成します（「gegeeks_ho_notebook.ipynb」をご参照ください）
    'schema': 'PUBLIC',  # ハンズオンで使用するスキーマ
    'warehouse': 'DEGEEKS_HO_WH'  # ハンズオンで使用するウェアハウス。存在しなければ新規作成します（「gegeeks_ho_notebook.ipynb」をご参照ください）
}
