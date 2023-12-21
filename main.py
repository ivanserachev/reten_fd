from flow import *
import polars as pl
import numpy as np
from sklearn.model_selection import train_test_split
from catboost import CatBoostClassifier
from typing import TypedDict
from numpy.typing import NDArray

LearningData = TypedDict('LearningData', {'x_train': NDArray,
                                          'x_test': NDArray,
                                          'y_train': NDArray,
                                          'y_test': NDArray,
                                          'cat_features': NDArray})


def execute_data() -> pl.DataFrame:
    data = execute_flow.start_flow()
    print('data executed')
    return data


def transform_data(data: pl.DataFrame) -> LearningData:
    x = data.select(pl.exclude(columns=['SALES']))
    y = data.select('SALES')

    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, random_state=1234)

    cat_features = x_train.select(pl.col(['GENDER',
                                          'MARITAL_STATUS',
                                          'EMPLOYMENT_TYPE',
                                          'DEPENDENTS',
                                          'EDUCATION',
                                          'EMAIL_HOST',
                                          'NAME',
                                          'MIDDLENAME',
                                          'WORK_PLACE_FEDERAL_DISTRICT',
                                          'WORK_PLACE_REGION',
                                          'WORK_PLACE_REGION_TYPE_FULL',
                                          'WORK_PLACE_CITY',
                                          'WORK_PLACE_CITY_TYPE_FULL',
                                          'WORK_PLACE_CITY_DISTRICT_WITH_TYPE',
                                          'WORK_PLACE_STEAD_TYPE_FULL',
                                          'WORK_PLACE_HOUSE_TYPE_FULL',
                                          'WORK_PLACE_CAPITAL_MARKER',
                                          'WORK_PLACE_FIAS_LEVEL',
                                          'WORK_PLACE_TIMEZONE',
                                          'WORK_PLACE_OKVED',
                                          'WORK_PLACE_OKVED_TYPE',
                                          'WORK_PLACE_OKATO',
                                          'WORK_PLACE_OKFS',
                                          'WORK_PLACE_OKOGU',
                                          'WORK_PLACE_OKPO',
                                          'WORK_PLACE_OKTMO',
                                          'WORK_PLACE_NAME',
                                          'WORK_PLACE_OPF_FULL',
                                          'WORK_PLACE_FINANCE_YEAR',
                                          'PASSPORTISSUERCODE',
                                          'PASSPORTSERIES',
                                          'REGISTRATIONADDRESS_FEDERAL_DISTRICT',
                                          'REGISTRATIONADDRESS_REGION',
                                          'REGISTRATIONADDRESS_REGION_TYPE_FULL',
                                          'REGISTRATIONADDRESS_CITY',
                                          'REGISTRATIONADDRESS_CITY_TYPE_FULL',
                                          'REGISTRATIONADDRESS_CITY_DISTRICT_WITH_TYPE',
                                          'REGISTRATIONADDRESS_STEAD_TYPE_FULL',
                                          'REGISTRATIONADDRESS_HOUSE_TYPE_FULL',
                                          'REGISTRATIONADDRESS_FIAS_LEVEL',
                                          'REGISTRATIONADDRESS_TIMEZONE',
                                          'RESIDENTIALADDRESS_FEDERAL_DISTRICT',
                                          'RESIDENTIALADDRESS_REGION',
                                          'RESIDENTIALADDRESS_REGION_TYPE_FULL',
                                          'RESIDENTIALADDRESS_CITY',
                                          'RESIDENTIALADDRESS_CITY_TYPE_FULL',
                                          'RESIDENTIALADDRESS_CITY_DISTRICT_WITH_TYPE',
                                          'RESIDENTIALADDRESS_STEAD_TYPE_FULL',
                                          'RESIDENTIALADDRESS_HOUSE_TYPE_FULL',
                                          'RESIDENTIALADDRESS_CAPITAL_MARKER',
                                          'RESIDENTIALADDRESS_FIAS_LEVEL',
                                          'RESIDENTIALADDRESS_TIMEZONE'])
                                  )
    cat_features = cat_features.to_numpy()
    x_train = x_train.to_numpy()
    x_test = x_test.to_numpy()
    y_train = y_train.to_numpy()
    y_test = y_test.to_numpy()
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
        cat_features=learn_data['cat_features'],
        eval_set=(learn_data['x_test'], learn_data['y_test']),
        logging_level='Info',
        plot=True
    )
    print('Resulting tree count:', model.tree_count_)


def flow():
    data = execute_data()
    transformed_data = transform_data(data=data)
    learning(learn_data=transformed_data)

flow()
