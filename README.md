# WineQualityPrediction
My attempt at the Wine Quality Prediction Lab for Cloud Computing

ReadMe: Assignment #2 Steps

Github link:

Docker link:

Optional Step 0: Early setup

While you don’t need to do these steps necessarily at the beginning, they will save some time and hassle when it comes to working with the rest of the lab.

I started with these precursor steps:
Update Java to the newest version
Install Maven if you don’t have it already, and update to the newest version. Make sure to set up path and environment variables for it on your computer.
Install sbt and Scala. Similarly to Maven, make sure to configure your path and environment variables properly on your home computer.
Install Docker for Windows. It is possible that MacOS may require some configuration for variables, but Docker for Windows seems to do it automatically.
Install Git. I also didn’t have a Github account prior to this, so I used this as an opportunity to create one.
Install Apache Spark, and winutils.exe - and configure them for your computer. Assuming you have set spark up successfully, you should see “Welcome to Spark” written in ASCII art upon running spark in the command prompt.

There may be other downloads/setups necessary (this has been a very, very long project in the making) - but this should cover the bulk of what you will need to set up your device.

Step 1: Create an EMR cluster

Log into the AWS Console.
Head over to the EMR Service and click "Create Cluster."
Give your cluster a name.
Some people may choose to toggle “Use High Availability” for the nodes - which allows for more fault tolerance, but is more expensive. For the purpose of this lab, I left it on normal settings.
Set the vendor to Amazon.
For hardware:
Choose your instance type. Because I am a beginner - and because we are working with multiple datasets in this lab - I opted to make all the instance types m5.xlarge. It is possible that other ones will be more effective for the lab, but these are still (relatively) cheap and convenient.
Set the number of instances to 4 (one master and three worker nodes).
Generate or select an EC2 key pair to access the master node. Make sure to save it somewhere on your device!

Click "Create Cluster" and wait for it to be ready.


Step 2: Uploading Files to the EMR Master Node:

Once your cluster is in the "waiting" state, copy the master node's DNS address and open your command prompt. It may take a bit for your cluster to reach the “waiting” state, so be patient with it! The address is labelled on AWS EMR as “Primary node public DNS”.
Start an SFTP connection:          sftp -i your-key.pem hadoop@your-master-dns
Note that if this isn’t working, you likely need to change your security group settings for the cluster - to allow SSH connections with 0.0.0.0/0
Upload your datasets and the jar file (TrainingDataset.csv, ValidationDataset.csv, and Assignment2.jar)
Exit sftp once you are done, since we will need to ssh to the same node (done by just typing “bye”



Step 3: SSH to the Master Node
SSH into the master node using:   ssh -i ~/your-key.pem hadoop@your-master-dns
Assuming you have set up your cluster properly, you should see this ASCII art for Hadoop:

Move your files into HDFS so worker nodes have access, using:
hadoop fs -put TrainingDataset.csv TrainingDataset.csv
hadoop fs -put ValidationDataset.csv ValidationDataset.csv
	Note that your path may be different for your files. For mine, TrainingDataset and ValidationDataset were already in my directory, so i didn’t need to write in a path to them.
Optionally, you can check to see if the files have been placed successfully, using:
hdfs dfs -ls -t -R

Step 4: Model training
 Execute the Spark job:
 spark-submit Assignment2.jar
This should create a training model for you, and place it in a folder.
Download the model folder back to your local environment, and package it to ease download
Transfer it via SFTP

Step 5: Single EC2 Predictions
Log into AWS and launch a new EC2 instance
Select a key pair, configure settings, and start the instance
Connect to it using ssh -i ec2-A.pem ec2-user@your-ec2-dns
If you don’t already have Scala and Spark downloaded, you will need them here. I already downloaded them prior to this step, so I will skip explaining this process.
If you aren’t still connected, reconnect to your EC2 instance and upload your jar file, trainingdataset.csv, and your archive from the trained model earlier.
Extract the model, and run your program with: spark-submit wine-quality-predict-1.0.jar

You should see the results printed on your console.

Step 6: Predicting Wine Quality with Docker
If it’s not there already, make sure to move your test dataset to your local directory.
Pull your docker image with: docker pull your-docker-image-name
Run the container by mapping your local directory to it:
docker run -v /path/to/data:/data your-docker-image-name /data/TestDataset.csv
You should see your results printed out into the terminal!
