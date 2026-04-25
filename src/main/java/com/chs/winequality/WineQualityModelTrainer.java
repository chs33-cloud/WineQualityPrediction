/* An attempt at Assignment 2 for NJIT's Cloud Computing course
Colin Sherman - 4/21/2026
 */

package com.chs.winequality;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import static com.chs.winequality.Constants.*;

public class WineQualityModelTrainer {
       /* Hey grader - here's a coding duck! Hope your day is going well


                         o8888o
                        o888888o
                        88888888o          Quack! Quack!
                        "8888""88o
              .o8888o.   888(   `"
            .o88888888o  "888
           o888888888888. 888o
      8o  o8888888888888888888
       888888888888888888888888
        88888888888888888888888
         88888888888888888DSI8"

        */
       private static final String TRAINING_DATASET = "/data/TestDataset.csv";
    public static void main(String[] args) {
        System.out.println("Starting WineQualityModelTrainer...");
        SparkSession sparkSetup;
        try {
            // Starting up Spark, diving into the program
            /* Commenting out the node builder for local testing
            sparkSetup = SparkSession.builder()
                    .appName("WineQualityPrediction")
                    .master("local[*]")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.memory", "2g")
                    .getOrCreate();

             */

            sparkSetup = SparkSession.builder()
                    .appName("WineQualityPrediction")
                    .master("local[*]")
                    // Disable Hadoop security to prevent getSubject error
                    .config("spark.hadoop.security.authentication", "simple")
                    .config("spark.hadoop.security.authorization", "false")
                    .getOrCreate();

            System.out.println("Spark master: " + sparkSetup.sparkContext().getConf().get("spark.master"));

        } catch (Exception e) {
            System.err.println("Uh oh, something went wrong starting Spark: " + e.getMessage());
            return;
        }

        // Set up the program with your AWS credentials
        if (StringUtils.isNotEmpty(ACCESS_KEY_ID) && StringUtils.isNotEmpty(SECRET_KEY)) {
            try {
                sparkSetup.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY_ID);
                sparkSetup.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY);
            } catch (Exception e) {
                System.err.println("Couldn't set AWS credentials: " + e.getMessage());
            }
        }

        // Checking if the dataset is there. If not, we'll print an error message
        // Make sure this path matches where the dataset is mounted inside the container

        File trainFile = new File(TRAINING_DATASET);
        if (trainFile.exists()) {
            System.out.println("Found your dataset! Let's get this show on the road...");
            if (trainFile.getName().toLowerCase().endsWith(".csv")) {
                new WineQualityModelTrainer().trainingWithSparkSesh(sparkSetup);
            } else {
                System.out.println("Error: The file " + TRAINING_DATASET + " is not a CSV file.");
            }
        } else {
            System.out.println("Hmm, I looked everywhere but couldn't find the dataset: " + TRAINING_DATASET);
            System.out.println("Make sure it's in the right place and try again later.");
        }
    }

    public void trainingWithSparkSesh(SparkSession spark) {
        // Debug: print the dataset path being used
        System.out.println("Loading data from: " + TRAINING_DATASET);

        System.out.println("\nLoading data, please wait...");
        Dataset<Row> trainingData = datasetAssembly(spark, true, TRAINING_DATASET);
        if (trainingData == null || trainingData.isEmpty()) {
            System.out.println("Oops, something went wrong loading the training data.");
            return;
        }
        trainingData = trainingData.cache();

        // Setting up the logistic regression model
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(100)
                .setRegParam(0.0);

        // Putting everything into a pipeline resulting from the training stages
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{lr});
        PipelineModel model;
        try {model = pipeline.fit(trainingData);
        } catch (Exception e) {
            System.err.println("Uh-oh, training the model didn't work: " + e.getMessage());
            return;
        }

        // Checkin to see how the model did during the training
        LogisticRegressionModel lrModel = (LogisticRegressionModel) (model.stages()[0]);
        LogisticRegressionTrainingSummary summary = lrModel.summary();

        System.out.println("\n-- Here's how I performed on the training data --");
        System.out.println("Accuracy: " + summary.accuracy());
        System.out.println("F-measure: " + summary.weightedFMeasure());

        Dataset<Row> validationData = datasetAssembly(spark, true, VALIDATION_DATASET);
        if (validationData == null || validationData.isEmpty()) {
            System.out.println("Couldn't load validation data. Skipping validation step.");
            return;
        }
        validationData = validationData.cache();
        Dataset<Row> predictions;
        try {
            predictions = model.transform(validationData);
        } catch (Exception e) {
            System.err.println("Something went wrong during prediction: " + e.getMessage());
            return;
        }

        System.out.println("\nHere's what the model predicted:");
        predictions.select("features", "label", "prediction").show(5, false);
        evaluateResults(predictions);

        // An attempt to save the model for later use, if needed
        try {
            model.write().overwrite().save(MODEL_PATH);
            System.out.println("All done! Your model is saved at: " + MODEL_PATH);
        } catch (Exception e) {
            System.err.println("We couldn't save the model: " + e.getMessage());
        }
    }

    private void evaluateResults(Dataset<Row> predictions) {
        MulticlassClassificationEvaluator eval = new MulticlassClassificationEvaluator();
        eval.setMetricName("accuracy");
        System.out.println("How accurate is my prediction? " + eval.evaluate(predictions));
        eval.setMetricName("f1");
        System.out.println("And the F1 score? " + eval.evaluate(predictions));
    }

    public Dataset<Row> datasetAssembly(SparkSession spark, boolean doTransform, String datasetPath) {
        System.out.println("Loading data from: " + datasetPath);
        Dataset<Row> csvData;
        try {
            csvData = spark.read().format("csv")
                    .option("header", "true")
                    .option("multiline", true)
                    .option("sep", ";")
                    .option("quote", "\"")
                    .option("dateFormat", "M/d/y")
                    .option("inferSchema", true)
                    .load(datasetPath);
        } catch (Exception e) {
            System.err.println("Couldn't load the dataset: " + e.getMessage());
            return null;
        }

        // Making the column names friendlier
        csvData = csvData.withColumnRenamed("fixed acidity", "fixed_acidity")
                .withColumnRenamed("volatile acidity", "volatile_acidity")
                .withColumnRenamed("citric acid", "citric_acid")
                .withColumnRenamed("residual sugar", "residual_sugar")
                .withColumnRenamed("chlorides", "chlorides")
                .withColumnRenamed("free sulfur dioxide", "free_sulfur_dioxide")
                .withColumnRenamed("total sulfur dioxide", "total_sulfur_dioxide")
                .withColumnRenamed("density", "density")
                .withColumnRenamed("pH", "pH")
                .withColumnRenamed("sulphates", "sulphates")
                .withColumnRenamed("alcohol", "alcohol")
                .withColumnRenamed("quality", "label");

        // Show some sample data
        csvData.show(5);

        // Pick features and labels based on csv keywords
        Dataset<Row> featureData = csvData.select(
                "label", "alcohol", "sulphates", "pH", "density",
                "free_sulfur_dioxide", "total_sulfur_dioxide", "chlorides",
                "residual_sugar", "citric_acid", "volatile_acidity", "fixed_acidity"
        );

        // Drop rows with missing info
        featureData = featureData.na().drop();

        // Pack features into a vector for the training model
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"alcohol", "sulphates", "pH", "density",
                        "free_sulfur_dioxide", "total_sulfur_dioxide", "chlorides",
                        "residual_sugar", "citric_acid", "volatile_acidity", "fixed_acidity"})
                .setOutputCol("features");

        if (doTransform) {
            featureData = assembler.transform(featureData).select("label", "features");
        }

        return featureData;
    }
}


