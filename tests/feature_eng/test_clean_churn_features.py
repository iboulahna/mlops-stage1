import pytest
from pyspark.sql import SparkSession
from src.feature_eng.clean_churn_features import clean_churn_features  # Adapte le chemin d'importation

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_clean_churn_features(spark):
    # Crée un DataFrame de test
    data = [("1", "29", "25.0", "100.0"),
            ("0", "35", "30.0", "200.0"),
            ("1", "40", "", "")]
    columns = ["senior_citizen", "tenure", "monthly_charges", "total_charges"]
    
    inputDF = spark.createDataFrame(data, columns)
    
    # Applique la fonction clean_churn_features
    resultDF = clean_churn_features(inputDF)
    
    # Collecte les résultats
    result = resultDF.select("senior_citizen", "tenure", "monthly_charges", "total_charges").rdd.collect()

    # Vérifie les résultats attendus
    assert result[0]["senior_citizen"] == "Yes"
    assert result[1]["senior_citizen"] == "No"
    assert result[2]["total_charges"] == 0.0  # total_charges était vide, donc 0
    assert result[0]["tenure"] == 29.0  # La valeur de tenure reste inchangée
    assert result[1]["monthly_charges"] == 30.0  # monthly_charges reste inchangé
