import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def create_spark_session():
    """Créer une session Spark avec des configurations optimisées"""
    return SparkSession.builder \
        .appName("Process Hourly Data") \
        .config("spark.sql.files.maxPartitionBytes", 128 * 1024 * 1024) \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def process_hourly_data(spark, hour_to_process):
  
    input_dir = f"./logs/{hour_to_process}"
    output_dir = f"./output/{hour_to_process}"

    if not os.path.exists(input_dir):
        print(f"Erreur : Le répertoire {input_dir} n'existe pas.")
        return

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("article", StringType(), True),
        StructField("price", DoubleType(), True)  
    ])

    try:
        df = spark.read \
            .option("delimiter", "|") \
            .option("mode", "DROPMALFORMED") \
            .schema(schema) \
            .csv(input_dir)

        if df.count() == 0:
            print(f"Aucune donnée trouvée dans {input_dir}")
            return

        aggregated_df = df.groupBy(
            date_format(col("date"), "yyyy/MM/dd HH").alias("formatted_date"),
            "article"
        ).agg(
            _sum("price").alias("total_sales")
        )

        os.makedirs("./output", exist_ok=True)

        aggregated_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(output_dir)

        output_files = [f for f in os.listdir(output_dir) if f.startswith("part-")]
        if output_files:
            os.rename(
                os.path.join(output_dir, output_files[0]), 
                f"./output/{hour_to_process}.txt"
            )
            os.system(f"rm -rf {output_dir}")

        print(f"Données traitées sauvegardées dans ./output/{hour_to_process}.txt")

    except Exception as e:
        print(f"Erreur lors du traitement : {str(e)}")
        with open("./output/error.log", "a") as log_file:
            log_file.write(f"{hour_to_process}: {str(e)}\n")

def main():
    hour_to_process = sys.argv[1] if len(sys.argv) > 1 else "2024112014"
    
    spark = create_spark_session()
    
    try:
        process_hourly_data(spark, hour_to_process)
    except Exception as e:
        print(f"Erreur globale : {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
