from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from microtc.textmodel import TextModel
from sklearn.svm import LinearSVC
import pickle


args = {
    'owner': 'enrique',
    'start_date': '2021-10-08',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id="add_libraries_and_plugins_and_save_load_model",
    default_args=args,
    tags=["firstVersion"],
    schedule_interval=None,                                 # '@daily',   None
    catchup=False,                                          # True->Run the days since 'start_date', False->only once
)



with dag:
    def test_microtc():
        corpus = [['Jinx Lux Ashe Yasuo Shyvana', 'LoL'], ['lapiz goma sacapuntas libreta pluma', 'papeleria']]
        textmodel_vectorizer = TextModel().fit([i[0] for i in corpus])
        model = LinearSVC().fit(textmodel_vectorizer.transform([i[0] for i in corpus]), [i[1] for i in corpus])
        clf_ = model.predict(textmodel_vectorizer.transform([['Jinx goma Ashe Yasuo Shyvana']]))
        print("predicciones: ", clf_)
        with open("model.pkl", "wb") as f:
            pickle.dump((textmodel_vectorizer, model), f)

    def load_and_apply_model():
        with open("model.pkl", "rb") as f:
            textmodel_vectorizer, model = pickle.load(f)
        predicts = model.predict(textmodel_vectorizer.transform([['lapiz goma Ashe pluma libreta']]))
        print("predicts: ", predicts)

    def test_plugins_path():
        words_ok = []
        words = open("./plugins/examples/example1.txt")
        for i in words:
            words_ok.append(i.replace("\n", ""))
        print("Ejemplo realizado: ", words_ok)
        words.close()


    t1 = PythonOperator(
        task_id="testing_microtc",
        python_callable=test_microtc,
    )

    t2 = PythonOperator(
        task_id="load_microtc_model_apply",
        python_callable=load_and_apply_model,
    )

    t3 = PythonOperator(
        task_id="testing_plugins_path",
        python_callable=test_plugins_path,
    )


    t1 >> t2 >> t3