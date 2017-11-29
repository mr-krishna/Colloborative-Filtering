package CS185;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import au.com.bytecode.opencsv.CSVParser;

import scala.Tuple2;


import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class MovieRecommendation {
	static String result = "";

	public static void main(String[] args) {
		
		
		// Create Java spark context
		SparkConf conf = new SparkConf().setAppName("MOVIE RECOMMENDER").setMaster("local")
										.set("es.index.auto.create", "true")
										.set("es.nodes", "localhost:9200");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read ratins.csv file. format - userId,movieId,rating
		JavaRDD<String> userMovieRatingsFile = sc.textFile(args[0]);
		
		// Read item description file. format - itemId, itemName, Other Fields,..
		JavaRDD<String> itemDescritpionFile = sc.textFile(args[1]);
		
		// Map file to Ratings(userID,MovieID,rating) tuples
		JavaRDD<Rating> ratings = userMovieRatingsFile.map(new Function<String, Rating>() {
			
			private static final long serialVersionUID = 1L;

			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer
						.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
				
			}
		});
		
	
		
		// Create tuples(itemId,ItemDescription), will be used later to get names of item from itemId
		JavaPairRDD<Integer,String> itemDescritpion = itemDescritpionFile.mapToPair(
				new PairFunction<String, Integer, String>() {
		
					private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(String t) throws Exception {
				CSVParser parser = new CSVParser();
				
				String[] s = parser.parseLine(t);
			
				//String[] s = t.split(",(?=([^\"]\"[^\"]\")[^\"]$)");
				//System.out.println(Arrays.toString(s));
				return new Tuple2<Integer,String>(Integer.parseInt(s[0]), s[1]);
			}
		});
	
	/*	
		itemDescritpion.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<Integer, String> arg0) throws Exception {
				System.out.println(arg0._1 + " " + arg0._2 );
				
			}
		});*/
		// Build the recommendation model using ALS
		
		int rank = 10; // 10 latent factor\
		int numIterations = Integer.parseInt(args[2]); // number of iterations
		
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings),
				rank, numIterations);
		
		//ALS.trainImplicit(arg0, arg1, arg2)
		// Create user-item tuples from ratings
		JavaRDD<Tuple2<Object, Object>> userProducts = ratings
				.map(new Function<Rating, Tuple2<Object, Object>>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Object, Object> call(Rating r) {
						return new Tuple2<Object, Object>(r.user(), r.product());
					}
				});
	
		// Calculate the itemIds not rated by a particular user, say user with userId = 1
		JavaRDD<Integer> notRatedByUser = userProducts.filter(new Function<Tuple2<Object,Object>, Boolean>() {
		
		
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Object, Object> v1) throws Exception {
				if (((Integer) v1._1).intValue() != 0) {
					return true;
				}
				return false;
			}
		}).map(new Function<Tuple2<Object,Object>, Integer>() {
		
			
			private static final long serialVersionUID = 1L;

			public Integer call(Tuple2<Object, Object> v1) throws Exception {
				return (Integer) v1._2;
			}
		});
		
		// Create user-item tuples for the items that are not rated by user, with user id 1
		JavaRDD<Tuple2<Object, Object>> itemsNotRatedByUser = notRatedByUser
				.map(new Function<Integer, Tuple2<Object, Object>>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Object, Object> call(Integer r) {
						return new Tuple2<Object, Object>(0, r);
					}
		});
		
		
		// Predict the ratings of the items not rated by user for the user
		
		
		JavaRDD<Rating> recomondations = model.predict(itemsNotRatedByUser.rdd()).
				toJavaRDD().distinct();
				//model.p
		// Sort the recommendations by rating in descending order
		recomondations = recomondations.sortBy(new Function<Rating,Double>(){
	
		
			private static final long serialVersionUID = 1L;

			public Double call(Rating v1) throws Exception {
				return v1.rating();
			}
			
		}, false, 1);	
		
		// Get top 10 recommendations
		
		
		JavaRDD<Rating> top5Recomondations = sc.parallelize(recomondations.take(5));
		top5Recomondations.foreach(new VoidFunction<Rating>() {

			private static final long serialVersionUID = 1L;

			public void call(Rating arg0) throws Exception {
			 System.out.println(arg0.toString());
				
			}
		});
	
		// Join top 10 recommendations with item descriptions
		JavaRDD<Tuple2<Rating, String>> recommendedItems = top5Recomondations.mapToPair(
				new PairFunction<Rating, Integer, Rating>() {
		
					private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Rating> call(Rating t) throws Exception {
				return new Tuple2<Integer,Rating>(t.product(),t);
			}
		}).join(itemDescritpion).values();
		
		
		//Print the top recommendations for user 1.
		recommendedItems.foreach(new VoidFunction<Tuple2<Rating,String>>() {
			
			
			private static final long serialVersionUID = 1L;

			
			public void call(Tuple2<Rating, String> t) throws Exception {
				
				System.out.println(t._1.product() + "\t" + t._1.rating() + "\t" + t._2);
				result=result+t._1.product() + "," + t._1.rating() + "," + t._2+"\n";
			}
		});
		PrintWriter writer;
		try {
			writer = new PrintWriter("wow.txt", "UTF-8");
			writer.println(result);
			
			writer.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*Map<String, String> maa =  new HashMap<>();
		maa.put("es.nodes", "localhost");
		maa.put("es.port", "9200");*/
		//JavaEsSpark.saveToEs(recommendedItems, "test/movie",maa);
	//	recommendedItems.saveAsTextFile("wow");
	
	
	sc.close();
		
	}

}