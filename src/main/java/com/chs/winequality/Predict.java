package com.chs.winequality;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import static com.chs.winequality.Constants.*;

public class Predict {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .config("spark.executor.memory", "2147480000")
                .config("spark.driver.memory", "2147480000")
                .config("spark.testing.memory", "2147480000")
                .getOrCreate();

        if (StringUtils.isNotEmpty(ACCESS_KEY_ID) && StringUtils.isNotEmpty(SECRET_KEY)) {

            spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY_ID);
            spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY);

        }

        File tempFile = new File(TESTING_DATASET);
        boolean exists = tempFile.exists();
        if(exists){
            Predict parser = new Predict();
            parser.logisticRegression(spark);
        }else{
            System.out.print("Sorry, can't find TestDataset.csv! Please try again \n");
        }



    }


    public void logisticRegression(SparkSession spark) {
        System.out.println("Metrics for the testing dataset\n");
        PipelineModel pipelineModel = PipelineModel.load(MODEL_PATH);
        Dataset<Row> testDf = getDataFrame(spark, true, TESTING_DATASET).cache();
        Dataset<Row> predictionDF = pipelineModel.transform(testDf).cache();
        predictionDF.select("features", "label", "prediction").show(5, false);
        printResults(predictionDF);

    }

    public Dataset<Row> getDataFrame(SparkSession spark, boolean transform, String name) {

        Dataset<Row> validationDataframe = spark.read().format("csv").option("header", "true")
                .option("multiline", true).option("sep", ";").option("quote", "\"")
                .option("dateFormat", "M/d/y").option("inferSchema", true).load(name);

        Dataset<Row> lblFeatureDf = validationDataframe.withColumnRenamed("quality", "label").select("label",
                "alcohol", "sulphates", "pH", "density", "free sulfur dioxide", "total sulfur dioxide",
                "chlorides", "residual sugar", "citric acid", "volatile acidity", "fixed acidity");

        lblFeatureDf = lblFeatureDf.na().drop().cache();

        VectorAssembler assembler =
                new VectorAssembler().setInputCols(new String[]{"alcohol", "sulphates", "pH", "density",
                        "free sulfur dioxide", "total sulfur dioxide", "chlorides", "residual sugar",
                        "citric acid", "volatile acidity", "fixed acidity"}).setOutputCol("features");

        if (transform)
            lblFeatureDf = assembler.transform(lblFeatureDf).select("label", "features");


        return lblFeatureDf;
    }


    public void printResults(Dataset<Row> predictions) {
        System.out.println();
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        evaluator.setMetricName("accuracy");
        System.out.println("The accuracy of the model is " + evaluator.evaluate(predictions));

        evaluator.setMetricName("f1");
        double f1 = evaluator.evaluate(predictions);

        System.out.println("F1: " + f1);
    }
}
