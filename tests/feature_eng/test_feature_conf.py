import pytest
from pyspark.sql import SparkSession
from src.feature_eng.feature_conf import compute_service_features, clean_churn_features

@pytest.fixture(scope="module")
def spark():
    # Créer la session Spark sans spécifier le master
    return SparkSession.builder.appName("test").getOrCreate()

    
def test_compute_service_features(spark):
    # Crée un DataFrame de test
    data = [("Yes", "No", "Yes", "Yes", "No", "Yes"),
            ("No", "No", "No", "Yes", "Yes", "No"),
            ("Yes", "Yes", "Yes", "No", "Yes", "Yes")]
    columns = ["online_security", "online_backup", "device_protection", "tech_support", "streaming_tv", "streaming_movies"]
    
    inputDF = spark.createDataFrame(data, columns)
    
    # Applique la fonction compute_service_features
    resultDF = compute_service_features(inputDF)
    
    # Collecte les résultats
    result = resultDF.select("num_optional_services").rdd.flatMap(lambda x: x).collect()

    # Vérifie les résultats attendus (le nombre de "Yes" dans chaque ligne)
    assert result == [4.0, 2.0, 5.0]

# def test_clean_churn_features(spark):
#     # Crée un DataFrame de test
#     data = [("1", "29", "25.0", "100.0"),
#             ("0", "35", "30.0", "200.0"),
#             ("1", "40", "", "")]
#     columns = ["senior_citizen", "tenure", "monthly_charges", "total_charges"]
    
#     inputDF = spark.createDataFrame(data, columns)
    
#     # Applique la fonction clean_churn_features
#     resultDF = clean_churn_features(inputDF)
    
#     # Collecte les résultats
#     result = resultDF.select("senior_citizen", "tenure", "monthly_charges", "total_charges").rdd.collect()

#     # Vérifie les résultats attendus
#     assert result[0]["senior_citizen"] == "Yes"
#     assert result[1]["senior_citizen"] == "No"
#     assert result[2]["total_charges"] == 0.0  # total_charges était vide, donc 0
#     assert result[0]["tenure"] == 29.0  # La valeur de tenure reste inchangée
#     assert result[1]["monthly_charges"] == 30.0  # monthly_charges reste inchangé



# import pytest
# from pyspark.sql import SparkSession


# # La fixture pour créer une session Spark
# @pytest.fixture(scope="module")
# def spark_session():
#     try:
#         from databricks.connect import DatabricksSession
#         print("Using Databricks Connect")
#         return DatabricksSession.builder.profile("unit_tests").getOrCreate()
#     except ValueError:
#         print("Profile `unit_tests` not found, trying default SparkSession getOrCreate")
#         from pyspark.sql import SparkSession
#         return SparkSession.builder.getOrCreate()
#     except ImportError:
#         print("No Databricks Connect, building and returning local SparkSession")
#         from pyspark.sql import SparkSession
#         return SparkSession.builder.getOrCreate()

# # Exemple de test utilisant la fixture `spark_session`
# def test_example(spark_session):
#     # Utilise la session Spark pour créer un DataFrame
#     data = [("Alice", 29), ("Bob", 30), ("Charlie", 35)]
#     columns = ["name", "age"]
#     df = spark_session.createDataFrame(data, columns)

#     # Effectue une opération de test
#     assert df.count() == 3
#     assert df.filter(df.age > 30).count() == 1


