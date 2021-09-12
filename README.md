# Twitter-Trading-HashTags-Spark-Streaming-Flask
This project was created in order to implement twitter's most interesting feature. Twitter collects all the trading hashtags across the world. 

![spark-twitter](https://user-images.githubusercontent.com/25617530/132982067-2252a19a-66e5-4545-8bb1-ae10a6e84c83.jpg)

In recent years data are exponentially increased due to the high number of smart devices. The evolution of IoT and Hidh Speed Interenet (HSI) 
led companies to digitalize their products. For instance, tweeter is a social media app that receives and processes billion of tweets every day.
Processing of these data is becoming more and more difficult since massive data cannot be processed in a single machine, due to hardware limits.
For that reason, scientists invented Apache Spark, a framework which can handle peta-bytes of data in real-time. Apache spark is 100x times faster than 
Hadoop and is even more powerful when streaming data, like tweets. Apache Spark was built on devide and conquer principle. Data are devided
into partitions and processed by different machines. In this way, small batches of data are assigned to slaves machines of a cluster and the master node
orchestrates the necessary execution steps. In our case we leverage Apache Spark to count effectively the 10 more trading tweets in England. 
Another python script is responsive for getting the tweets from Twitter API and pass them via socket to Apache Spark (twitter_app.py).
Spark Streaming and Spark SQL are combined to extract information about the most famous tweets, which are sented via HTTP request to Flask.
Don't omit to explore our web UI at localhost:5001/.

Have a nice trip!

## Execute it !
```bat
#Receive tweets
python TwitterHttpClient\twitter_app.py
#Open DashBoard
python HashtagsDashboard\app.py
#Start spark streaming
java -jar twitter-spark.jar
```


