from flow import *
import polars as pl
from sklearn.model_selection import train_test_split
from catboost import CatBoostClassifier
from typing import TypedDict
from numpy.typing import NDArray

LearningData = TypedDict('LearningData', {'x_train': NDArray,
                          'x_test': NDArray,
                          'y_train': NDArray,
                          'y_test': NDArray})


def execute_data() -> pl.DataFrame:
    data = execute_flow.start_flow()
    return data


def transform_data(data: pl.DataFrame) -> LearningData:
    x = data.select(pl.exclude('SALES', 'DEAL_STATUS'))
    y = data.select('SALES')

    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, random_state=1234)

    x_train = x_train.to_numpy()
    x_test = x_test.to_numpy()
    y_train = y_train.to_numpy()
    y_test = y_test.to_numpy()
    return LearningData(x_train=x_train,
                        x_test= x_test,
                        y_train=y_train,
                        y_test=y_test)


def learning(learn_data: LearningData) -> None:
    model = CatBoostClassifier(
        bagging_temperature=1,
        random_strength=1,
        thread_count=3,
        iterations=500,
        l2_leaf_reg=4.0,
        learning_rate=0.07521709965938336,
        save_snapshot=True,
        snapshot_file='snapshot_best.bkp',
        random_seed=63,
        od_type='Iter',
        od_wait=20,
        custom_loss=['AUC', 'Accuracy'],
        use_best_model=True
    )

    model.fit(
        learn_data['x_train'], learn_data['y_train'],
        eval_set=(learn_data['x_test'], learn_data['y_test']),
        logging_level='Info',
        plot=True
    )
    print('Resulting tree count:', model.tree_count_)

def flow():
    data = execute_data()
    transformed_data = transform_data(data=data)
    learning(learn_data=transformed_data)

