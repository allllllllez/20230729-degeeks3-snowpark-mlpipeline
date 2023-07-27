def model(dbt, session):
    # Must be either table or incremental (view is not currently supported)
    dbt.config(
        materialized = "table",
        packages=["xgboost", "pandas", "numpy"]
    )
    from xgboost import XGBRegressor
    import pandas as pd
    import numpy as np


    # DataFrame representing an upstream model
    df_train = dbt.ref("train")
    df_train = df_train.to_pandas()
    df_test = dbt.ref("test")
    df_test = df_test.to_pandas()

    feature_cols = list(df_train.columns)
    # target_col = 'SUM_BASE_PRICE'  # 当たりすぎる
    target_col = 'COUNT_ORDER'
    feature_cols.remove(target_col)
    feature_cols.remove('ORDER_DATE')

    X = df_train[feature_cols].values
    y = df_train[target_col].values
    
    # TypeError('Not supported type for data.' + str(type(data)))
    
    # 学習
    xgbmodel = XGBRegressor(random_state=123)
    xgbmodel.fit(X,y)
 
    y_pred = xgbmodel.predict(df_test[feature_cols].values)
    
    # Add a new column containing the id incremented by one
    df = df_test[['ORDER_DATE', target_col]]
    df["COUNT_ORDER_PRED"] = y_pred

    return df