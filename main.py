from flow import *
import polars as pl
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from catboost import CatBoostClassifier, Pool
from catboost import utils
from typing import TypedDict, List
from numpy.typing import NDArray
import matplotlib.pyplot as plt

LearningData = TypedDict('LearningData', {'x_train': NDArray,
                                          'x_test': NDArray,
                                          'y_train': NDArray,
                                          'y_test': NDArray,
                                          'cat_features': List})


def execute_data() -> pl.DataFrame:
    data = execute_flow.start_flow()
    print('data executed')
    return data


def transform_data(data: pl.DataFrame) -> LearningData:
    data = data.to_pandas()
    data = data.astype('string')
    print(data.dtypes)
    # data =data.fill_null('')
    data =data.fillna('')
    x = data.loc[:, data.columns!='SALES']
    # x = data.select(pl.exclude(columns=['SALES']))
    y = data['SALES']
    # y = data.select('SALES')

    # with pl.Config(tbl_cols=x.width):
    #     print(x)
    pd.set_option('display.max_columns', None)

    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, random_state=1234)

    cat_features = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66]
    # x_train = x_train.to_numpy()
    # x_test = x_test.to_numpy()
    # y_train = y_train.to_numpy()
    # y_test = y_test.to_numpy()
    print('data transformed')
    return LearningData(x_train=x_train,
                        x_test=x_test,
                        y_train=y_train,
                        y_test=y_test,
                        cat_features=cat_features)


def learning(learn_data: LearningData) -> None:
    model = CatBoostClassifier(
        bagging_temperature=1,
        random_strength=1,
        thread_count=3,
        iterations=500,
        l2_leaf_reg=4.0,
        learning_rate=0.07521709965938336,
        random_seed=63,
        od_type='Iter',
        od_wait=20,
        custom_loss=['AUC', 'Accuracy'],
        use_best_model=True
    )

    model.fit(
        learn_data['x_train'], learn_data['y_train'],
        cat_features=learn_data['cat_features'],
        eval_set=(learn_data['x_test'], learn_data['y_test']),
        logging_level='Info',
        plot=True
    )
    print('Resulting tree count:', model.tree_count_)
    # train_pool = Pool(learn_data['x_test'], learn_data['y_test'])
    # (fpr, tpr, thresholds) = utils.get_roc_curve(model=model, data=train_pool, plot=True)
    print(model.get_best_score())

    feature_importance = model.feature_importances_
    sorted_idx = np.argsort(feature_importance)
    fig = plt.figure(figsize=(12, 6))
    plt.barh(range(len(sorted_idx)), feature_importance[sorted_idx], align='center')
    plt.yticks(range(len(sorted_idx)), np.array(learn_data['x_test'].columns)[sorted_idx])
    plt.title('Feature Importance')
    plt.show()


def flow():
    data = execute_data()
    transformed_data = transform_data(data=data)
    learning(learn_data=transformed_data)

flow()
