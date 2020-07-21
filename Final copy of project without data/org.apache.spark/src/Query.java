

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.swing.JOptionPane;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.catalyst.*;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.analysis.*;
//import org.apache.spark.sql.catalyst.*;
import org.apache.spark.sql.execution.*;
import org.apache.spark.sql.types.util.*;




/**
 * Servlet implementation class Query
 */
@WebServlet("/Query")
public class Query extends HttpServlet {
	private static final long serialVersionUID = 1L;
	/**
     * Default constructor. 
     */
    public Query() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		
		  int requested_data = Integer.parseInt(request.getParameter("Query selected"));
	      System.out.println(requested_data);
	      
	      switch(requested_data)
			{
			case 1: 
		        CandidateQuery();
		       
		       response.sendRedirect("Candidate.html");
				break;
		case 2: 
			Top8UsersTweetsCount();    
			response.sendRedirect("FrequentTweetUsers.html");
			break;
		case 3:
			Top8UsersFollowers();
			response.sendRedirect("FamousPersons.html");
			break;
		case 4:
			TopLocations();
			response.sendRedirect("TopLocations.html");
			break;
		case 5:
			Friends();
			response.sendRedirect("MoreFriends.html");
			break;
		case 6:
			TimeQuery();
			response.sendRedirect("MostTweetTimes.html");
			break;
		case 7:
			SentimentAnalysisQuery();
			response.sendRedirect("SentimentAnalysis.html");
			break;
		case 8:
			SourceQuery();
			response.sendRedirect("Source.html");
			break;
		case 9:
			TweetStatsQuery();
			response.sendRedirect("tweet_stats.html");
				
				break;
			default: JOptionPane.showMessageDialog(null, "invalid option");
						break;
			}
		
		
	}
	
	
	  public void CandidateQuery()
			{
//		  

      	URL url=getClass().getResource("tweitCollectfinal.txt");
          String pathToFile = url.toString();
		    SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
		    
		    
		    JavaSparkContext sc = new JavaSparkContext(conf);
		   
		    JavaSQLContext sqlContext = new JavaSQLContext(sc);
		    
		   
		   
		    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);
		    tweets.registerAsTable("tweetTable");

		    tweets.printSchema();

		    nbTweetByCandidate(sqlContext);

		    sc.stop();
		    /*String htmlurl = "http://twitteranalysispbm.mybluemix.net/FrequentTweetUsers.html";
	        try {
				java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
			} catch (IOException e) {
				e.printStackTrace();
			}*/
		
			}

	  private void nbTweetByCandidate(JavaSQLContext sqlContext) 
		{		  
			 try
			 {
				  File outputFile = new File(getServletContext().getRealPath("/")
				            + "/query1.csv");
				
			 FileWriter fw= new FileWriter(outputFile);

		    JavaSchemaRDD count = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
		    										"WHERE text LIKE '%realDonaldTrump%'");
		    JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%HillaryClinton%'");
		    JavaSchemaRDD count2 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%JebBush%'");
		    JavaSchemaRDD count3 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%RealBenCarson%'");
		    JavaSchemaRDD count4 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%ChrisChristie%'");
		    JavaSchemaRDD count5 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%tedcruz%'");
		    JavaSchemaRDD count6 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%CarlyFiorina%'");
		    JavaSchemaRDD count7 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%gov_gilmore%'");
		    JavaSchemaRDD count8 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%JohnKasich%'");
		    JavaSchemaRDD count9 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%marcorubio%'");
		    JavaSchemaRDD count10 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%BernieSanders%'");
		 
		 List<Row> realDonaldTrump=count.collect();	 
		 String realDonaldTrump12=realDonaldTrump.toString();
		 String realDonaldTrump1 = realDonaldTrump12.substring(realDonaldTrump12.indexOf("[") + 2, realDonaldTrump12.indexOf("]"));
		 
		 List<Row> HillaryClinton=count1.collect();
		 String HillaryClinton12=HillaryClinton.toString();
		 String HillaryClinton1 = HillaryClinton12.substring(HillaryClinton12.indexOf("[") + 2, HillaryClinton12.indexOf("]"));
		 
		 List<Row> JebBush=count2.collect();
		 String JebBush12=JebBush.toString();
		 String JebBush1 = JebBush12.substring(JebBush12.indexOf("[") + 2, JebBush12.indexOf("]"));
		 
		 List<Row> RealBenCarson=count3.collect();
		 String RealBenCarson12=RealBenCarson.toString();
		 String RealBenCarson1 = RealBenCarson12.substring(RealBenCarson12.indexOf("[") + 2, RealBenCarson12.indexOf("]"));
		 
		 List<Row> ChrisChristie=count4.collect();
		 String ChrisChristie12=ChrisChristie.toString();
		 String ChrisChristie1 = ChrisChristie12.substring(ChrisChristie12.indexOf("[") + 2, ChrisChristie12.indexOf("]"));
		 
		 List<Row> tedcruz=count5.collect();
		 String tedcruz12=tedcruz.toString();
		 String tedcruz1 = tedcruz12.substring(tedcruz12.indexOf("[") + 2, tedcruz12.indexOf("]"));
		 
		 List<Row> CarlyFiorina=count6.collect();
		 String CarlyFiorina12=CarlyFiorina.toString();
		 String CarlyFiorina1 = CarlyFiorina12.substring(CarlyFiorina12.indexOf("[") + 2, CarlyFiorina12.indexOf("]"));
		 
		 List<Row> gov_gilmore=count7.collect();
		 String gov_gilmore12=gov_gilmore.toString();
		 String gov_gilmore1 = gov_gilmore12.substring(gov_gilmore12.indexOf("[") + 2, gov_gilmore12.indexOf("]"));
		 
		 List<Row> JohnKasich=count8.collect();
		 String JohnKasich12=JohnKasich.toString();
		 String JohnKasich1 = JohnKasich12.substring(JohnKasich12.indexOf("[") + 2, JohnKasich12.indexOf("]"));
		 
		 List<Row> marcorubio=count9.collect();
		 String marcorubio12=marcorubio.toString();
		 String marcorubio1 = marcorubio12.substring(marcorubio12.indexOf("[") + 2, marcorubio12.indexOf("]"));
		 
		 List<Row> BernieSanders=count10.collect();
		 String BernieSanders12=BernieSanders.toString();
		 String BernieSanders1 = BernieSanders12.substring(BernieSanders12.indexOf("[") + 2, BernieSanders12.indexOf("]"));
		
		    
		    fw.append("Language");
			fw.append(',');
			fw.append("Count");
			fw.append("\n");
			fw.append("Donald Trump");
			fw.append(',');
			fw.append(realDonaldTrump1);
			fw.append("\n");
			fw.append("Hillary Clinton");
			fw.append(',');
			fw.append(HillaryClinton1);
			fw.append("\n");
			fw.append("Jeb Bush");
			fw.append(',');
			fw.append(JebBush1);
			fw.append("\n");
			fw.append("Ben Carson");
			fw.append(',');
			fw.append(RealBenCarson1);
			fw.append("\n");
			fw.append("Chris Christie");
			fw.append(',');
			fw.append(ChrisChristie1);
			fw.append("\n");
			fw.append("Ted Cruz");
			fw.append(',');
			fw.append(tedcruz1);
			fw.append("\n");
			fw.append("Carly Fiorina");
			fw.append(',');
			fw.append(CarlyFiorina1);
			fw.append("\n");
			fw.append("gov_gilmore");
			fw.append(',');
			fw.append(gov_gilmore1);
			fw.append("\n");
			fw.append("John Kasich");
			fw.append(',');
			fw.append(JohnKasich1);
			fw.append("\n");
			fw.append("Marcorubio");
			fw.append(',');
			fw.append(marcorubio1);
			fw.append("\n");
			fw.append("Bernie Sanders");
			fw.append(',');
			fw.append(BernieSanders1);
			fw.append("\n");
			
			
			
			fw.close();
			
			
		 }
			  catch (Exception exp)
			  {
			  }

		  }

	  
//		  
	        public void Top8UsersTweetsCount()
			{
	        	URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
			    SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
			    
			    
			    JavaSparkContext sc = new JavaSparkContext(conf);
			   
			    JavaSQLContext sqlContext = new JavaSQLContext(sc);
			    
			   
			   
			    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);
			    tweets.registerAsTable("tweetTable");

			    tweets.printSchema();

			    nbTweetByUser(sqlContext);

			    sc.stop();
			    /*String htmlurl = "http://twitteranalysispbm.mybluemix.net/FrequentTweetUsers.html";
		        try {
					java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
				} catch (IOException e) {
					e.printStackTrace();
				}*/
			}
			
			private void nbTweetByUser(JavaSQLContext sqlContext) 
			{		  
				 try
				 {
					 
					 File outputFile = new File(getServletContext().getRealPath("/")
							 
					            + "/query2.csv");
					
				 FileWriter fw= new FileWriter(outputFile);
			   
			    JavaSchemaRDD count = sqlContext.sql("SELECT user.name,max(user.statuses_count) AS c FROM tweetTable " +
			    		                             "GROUP BY user.name ORDER BY c desc limit 8");
			    
			    
		       List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

		       //Collections.reverse(rows);
			    
			    String rows123=rows.toString();
			    
			   
			    
			   String[] array = rows123.split("],"); 
			    
			    System.out.println(rows123);
			    
			    fw.append("Name");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
				
				

				for(int i = 0; i < 8; i++)
				{
					if(i==0)
					{
						fw.append(array[0].substring(2));
						fw.append(',');
						fw.append("\n");
					}
					else {
					fw.append(array[i].substring(2));
					fw.append(',');
					fw.append("\n");
					}
				}
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }

			  }

			
			public void Top8UsersFollowers()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
				 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sc = new JavaSparkContext(conf);
		 
		         JavaSQLContext sqlContext = new JavaSQLContext(sc);

		         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		         tweets.registerAsTable("tweetTable");

		        tweets.printSchema();

		       nbTweetByFollower(sqlContext);

		        sc.stop();
		        
//		        String htmlurl = "http://twitteranalysispbm.mybluemix.net/FamousPersons.html";
//		        try {
//					java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//				} catch (IOException e) {
//					e.printStackTrace();
//				}

			}
			
			 private void nbTweetByFollower(JavaSQLContext sqlContext) {
				  
				  try
				  {
					  File outputFile = new File(getServletContext().getRealPath("/")
					            + "/query3.csv");
					
				 FileWriter fw= new FileWriter(outputFile);
			   
			    JavaSchemaRDD count = sqlContext.sql("SELECT user.name, max(user.followers_count) AS c FROM tweetTable " +
			                                         "GROUP BY user.name ORDER BY c DESC limit 8");
			    
			    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

			       //Collections.reverse(rows);
				    
				   String rows123=rows.toString();
			    
			       String[] array = rows123.split("],"); 
			    
			
			    
			    fw.append("Name");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
				
				

				for(int i = 0; i < 8; i++)
				{
					if(i==0)
					{
						fw.append(array[0].substring(2));
						fw.append(',');
						fw.append("\n");
					}
					else {
					fw.append(array[i].substring(2));
					fw.append(',');
				    fw.append("\n");
					}
				}
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }
			
			public void TopLocations()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
				SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

		        JavaSparkContext sc = new JavaSparkContext(conf);

		        JavaSQLContext sqlContext = new JavaSQLContext(sc);

		        JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		        tweets.registerAsTable("tweetTable");
		       

		        //tweets.printSchema();

		        nbTweetByLocation(sqlContext);

		        sc.stop();
		        
//		        String htmlurl = "http://twitteranalysispbm.mybluemix.net/BackgroundColors.html";
//		        try {
//					java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
			}
			
		  private void nbTweetByLocation(JavaSQLContext sqlContext) 
		  {
				  
			     try
				  {
			    	 File outputFile = new File(getServletContext().getRealPath("/")
					            + "/query4.csv");
					
				 FileWriter fw= new FileWriter(outputFile);

				 JavaSchemaRDD count = sqlContext.sql("SELECT user.location, COUNT(*) AS c FROM tweetTable " +
		                    							"order by c" );
			       
				  List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
				    
			       Collections.reverse(rows);
				    
				   String rows123=rows.toString();
				    
				   String[] array = rows123.split("],"); 
				    
				   System.out.println(rows123);
			    
		        fw.append("Location");
				fw.append(',');
				fw.append("second location");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
			
				for(int i = 0; i < 12; i++)
				{
					if((i==0)||(i==1)||(i==2))
					{
						continue;
					}
					
					else if(i == array.length-1)
					{
						fw.append(array[i].substring(2,array[i].length()-2));
						fw.append(',');
						fw.append("\n");
					}
					else {
					fw.append(array[i].substring(2));
					fw.append(',');
					fw.append("\n");
					}
				}
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }

			
			public  void Friends()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
				 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sc = new JavaSparkContext(conf);

		         JavaSQLContext sqlContext = new JavaSQLContext(sc);

		         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		         tweets.registerAsTable("tweetTable");

		         tweets.printSchema();

		          nbTweetByFriends(sqlContext);

		          sc.stop();
//		          String htmlurl = "http://twitteranalysispbm.mybluemix.net/MoreFreinds.html";
//		          try {
//		  			java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//		  		} catch (IOException e) {
//		  			e.printStackTrace();
//		  		}
			}
			
			 private void nbTweetByFriends(JavaSQLContext sqlContext) {
				  
				  try
				  {
					  File outputFile = new File(getServletContext().getRealPath("/")
					            + "/query5.csv");
					
				 FileWriter fw= new FileWriter(outputFile);
			   
			    JavaSchemaRDD count = sqlContext.sql("SELECT user.screen_name, max(user.friends_count) AS c FROM tweetTable " +
			    										"WHERE user.friends_count>'150000'" +
			    		                                  "group by user.screen_name order by c desc limit 20" ); 
			   
			    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
			    
		       //Collections.reverse(rows);
			    
			    String rows123=rows.toString();
			    
			   String[] array = rows123.split("],"); 
			    
			    System.out.println(rows123);
			    
			    fw.append("Name");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
				
				

				for(int i = 0; i < array.length; i++)
				{
					if(i==0)
					{
						fw.append(array[0].substring(2));
						fw.append(',');
						fw.append("\n");
					}
					else if(i == array.length-1)
					{
						fw.append(array[i].substring(2,array[i].length()-2));
						fw.append(',');
						fw.append("\n");
					}
					else {
					fw.append(array[i].substring(2));
					fw.append(',');
					fw.append("\n");
					}
				}
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }

			
			public void TimeQuery()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
				 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sc = new JavaSparkContext(conf);
		         JavaSQLContext sqlContext = new JavaSQLContext(sc);

		         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		        tweets.registerAsTable("tweetTable");

		       tweets.printSchema();

		       nbTweetByTime(sqlContext);

		       sc.stop();
		       
//		       String htmlurl = "http://twitteranalysispbm.mybluemix.net/MostTweetTimes.html";
//		       try {
//					java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
		       
		     
			}
			
			 private void nbTweetByTime(JavaSQLContext sqlContext) 
			 {		  
				  try
				  {
					  File outputFile = new File(getServletContext().getRealPath("/")
					            + "/query6.csv");
					
				 FileWriter fw= new FileWriter(outputFile);
			   
			    JavaSchemaRDD count = sqlContext.sql("SELECT created_at, COUNT(*) AS c FROM tweetTable " +
			    										"Group By created_at " +
			    		                                  "order by c desc limit 10" );
			    	   
			    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
			    
		        //Collections.reverse(rows);
			    
			    String rows123=rows.toString();
			    
			    String[] array = rows123.split("],"); 
			    
			    System.out.println(rows123);
			    
			    fw.append("Time");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
				
				

				for(int i = 0; i < 9; i++)
				{
					if(i==0)
					{
						continue;
					}
					else if(i == array.length-1)
					{
						fw.append(array[i].substring(2,array[i].length()-2));
						fw.append(',');
						fw.append("\n");
					}
					else {
					fw.append(array[i].substring(2));
					fw.append(',');
					fw.append("\n");
					}
				}
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }
			
			public void SentimentAnalysisQuery()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
		       SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

		       JavaSparkContext sc = new JavaSparkContext(conf);

		       JavaSQLContext sqlContext = new JavaSQLContext(sc);

		       JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		       tweets.registerAsTable("tweetTable");

		       tweets.printSchema();

		       nbTweetBySentiment(sqlContext);

		       sc.stop();
		       
//		       String htmlurl = "http://twitteranalysispbm.mybluemix.net/SentimentAnalysis.html";
//		       try {
//					java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
			}
			
			private void nbTweetBySentiment(JavaSQLContext sqlContext) 
			
			{
			    try
				{
			     
			    	 File outputFile = new File(getServletContext().getRealPath("/")
					            + "/Query7.csv");
					
				 FileWriter fw= new FileWriter(outputFile);
			   
			  JavaSchemaRDD count = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
			  										"WHERE text LIKE '%abundant%' OR text LIKE '%accessible%' OR text LIKE '%accurate%' OR text LIKE '%award%' OR text LIKE '%awesome%' OR text LIKE '%beautiful%' OR text LIKE '%affirmation%' OR text LIKE '%amicable%' OR text LIKE '%appreciate%' OR text LIKE '%approve%' OR text LIKE '%attractive%' OR text LIKE '%benefit%' OR text LIKE '%bless%' OR text LIKE '%bonus%' OR text LIKE '%brave%' OR text LIKE '%bright%' OR text LIKE '%brilliant%' OR text LIKE '%celebrate%' OR text LIKE '%champion%' OR text LIKE '%charm%' OR text LIKE '%cheer%' OR text LIKE '%clever%' OR text LIKE '%colorful%' OR text LIKE '%comfort%' OR text LIKE '%compliment%' OR text LIKE '%confidence%' OR text LIKE '%congratulation%' OR text LIKE '%cute%' OR text LIKE '%good%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%easy%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%fair%' OR text LIKE '%excite%' OR text LIKE '%fast%' OR text LIKE '%fine%' OR text LIKE '%fortunate%' OR text LIKE '%free%' OR text LIKE '%fresh%' OR text LIKE '%fun%' OR text LIKE '%gain%' OR text LIKE '%gem%' OR text LIKE '%gorgeous%' OR text LIKE '%grand%' OR text LIKE '%handsome%' OR text LIKE '%healthy%' OR text LIKE '%honest%' OR text LIKE '%humor%' OR text LIKE '%important%' OR text LIKE '%impress%' OR text LIKE '%improve%' OR text LIKE '%joy%' OR text LIKE '%love%' OR text LIKE '%perfect%' OR text LIKE '%pleasant%' OR text LIKE '%compliment%' OR text LIKE '%pleasure%' OR text LIKE '%precious%' OR text LIKE '%prolific%' OR text LIKE '%prudent%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%proven%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%restored%'  ");
			  JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE text LIKE '%abuse%' OR text LIKE '%abyss%' OR text LIKE '%absurd%' OR text LIKE '%akward%' OR text LIKE '%adverse%' OR text LIKE '%agony%' OR text LIKE '%annoying%' OR text LIKE '%anti%' OR text LIKE '%arrogant%' OR text LIKE '%assassinate%' OR text LIKE '%aversion%' OR text LIKE '%backward%' OR text LIKE '%bad%' OR text LIKE '%brutal%' OR text LIKE '%battered%' OR text LIKE '%berate%' OR text LIKE '%bewitch%' OR text LIKE '%berate%' OR text LIKE '%blunder%' OR text LIKE '%complain%' OR text LIKE '%conflict%' OR text LIKE '%confound%' OR text LIKE '%contagious%' OR text LIKE '%contaminated%' OR text LIKE '%contravene%' OR text LIKE '%corruption%' OR text LIKE '%corrupt%' OR text LIKE '%coward%' OR text LIKE '%cruel%' OR text LIKE '%sad%' OR text LIKE '%danger%' OR text LIKE '%debase%' OR text LIKE '%decline%' OR text LIKE '%deceive%' OR text LIKE '%defamation%' OR text LIKE '%demon%' OR text LIKE '%demolish%' OR text LIKE '%denied%' OR text LIKE '%demolish%' OR text LIKE '%depress%' OR text LIKE '%deny%' OR text LIKE '%destroy%' OR text LIKE '%devastation%' OR text LIKE '%disadvantage%' OR text LIKE '%disappointed%' OR text LIKE '%discord%' OR text LIKE '%evil%' OR text LIKE '%gossip%' OR text LIKE '%hard%' OR text LIKE '%gloom%' OR text LIKE '%hate%' OR text LIKE '%hazard%' OR text LIKE '%fuck%' OR text LIKE '%horrible%' OR text LIKE '%idiot%' OR text LIKE '%imperfect%' OR text LIKE '%inefficient%' OR text LIKE '%inflammation%' OR text LIKE '%ironic%' OR text LIKE '%irritate%' OR text LIKE '%jealous%' OR text LIKE '%lag%' OR text LIKE '%lie%' OR text LIKE '%malignant%' OR text LIKE '%malign%' OR text LIKE '%noisy%' OR text LIKE '%odd%' OR text LIKE '%offence%' OR text LIKE '%offend%' OR text LIKE '%offensive%' OR text LIKE '%bad%' OR text LIKE '%unhappy%' OR text LIKE '%weak%'");
			 
			 List<Row> positive=count.collect();	 
			 String positive12=positive.toString();
			 String positive1 = positive12.substring(positive12.indexOf("[") + 2, positive12.indexOf("]"));
			 
			 List<Row> negative=count1.collect();
			 String negative12=negative.toString();
			 String negative1 = negative12.substring(negative12.indexOf("[") + 2, negative12.indexOf("]"));
			 
			    
			    fw.append("Words");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
				fw.append("PositiveTweets");
				fw.append(',');
				fw.append(positive1);
				fw.append("\n");
				fw.append("NegativeTweets");
				fw.append(',');
				fw.append("-"+negative1);
				fw.append("\n");
				
				
				
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }
			
			public void SourceQuery()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String pathToFile = url.toString();
				 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sc = new JavaSparkContext(conf);

		         JavaSQLContext sqlContext = new JavaSQLContext(sc);

		         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

		         tweets.registerAsTable("tweetTable");

		         tweets.printSchema();

		         nbTweetBySourceQuery(sqlContext);

		        sc.stop();
		        
//		        String htmlurl = "http://twitteranalysispbm.mybluemix.net/TopGames.html";
//		        try {
//		 			java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//		 		} catch (IOException e) {
//		 			e.printStackTrace();
//		 		}

			}
			
			 private void nbTweetBySourceQuery(JavaSQLContext sqlContext) {
				  
				  try
				  {
					  File outputFile = new File(getServletContext().getRealPath("/")
					            + "/query8.csv");
					
				 FileWriter fw= new FileWriter(outputFile);

			    JavaSchemaRDD count = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
			    										"WHERE source LIKE '%Android%'");
			    JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%iPhone%'");
			    JavaSchemaRDD count2 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%iPad%'");
			    JavaSchemaRDD count3 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%Windows%'");
			    JavaSchemaRDD count4 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%Web Client%'");
			    //JavaSchemaRDD count5 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
				//		"WHERE source LIKE '%Golf%'");
			 
			 List<Row> Android=count.collect();	 
			 String Android12=Android.toString();
			 String Android1 = Android12.substring(Android12.indexOf("[") + 2, Android12.indexOf("]"));
			 
			 List<Row> iPhone=count1.collect();
			 String iPhone12=iPhone.toString();
			 String iPhone1 = iPhone12.substring(iPhone12.indexOf("[") + 2, iPhone12.indexOf("]"));
			 
			 List<Row> iPad=count2.collect();
			 String iPad12=iPad.toString();
			 String iPad1 = iPad12.substring(iPad12.indexOf("[") + 2, iPad12.indexOf("]"));
			 
			 List<Row> Windows=count3.collect();
			 String Windows12=Windows.toString();
			 String Windows1 = Windows12.substring(Windows12.indexOf("[") + 2, Windows12.indexOf("]"));
			 
			 List<Row> Web_Client=count4.collect();
			 String Web_Client12=Web_Client.toString();
			 String Web_Client1 = Web_Client12.substring(Web_Client12.indexOf("[") + 2, Web_Client12.indexOf("]"));
			 
			 //List<Row> Golf=count5.collect();
			 //String Golf12=Golf.toString();
			 //String Golf1 = Golf12.substring(Golf12.indexOf("[") + 2, Golf12.indexOf("]"));
			
			    
			    fw.append("DeviceName");
				fw.append(',');
				fw.append("Count");
				fw.append("\n");
				fw.append("Android");
				fw.append(',');
				fw.append(Android1);
				fw.append("\n");
				fw.append("iPhone");
				fw.append(',');
				fw.append(iPhone1);
				fw.append("\n");
				fw.append("iPad");
				fw.append(',');
				fw.append(iPad1);
				fw.append("\n");
				fw.append("Windows");
				fw.append(',');
				fw.append(Windows1);
				fw.append("\n");
				fw.append("Web Client");
				fw.append(',');
				fw.append(Web_Client1);
				fw.append("\n");
				//fw.append("Golf");
				//fw.append(',');
				//fw.append(Golf1);
				//fw.append("\n");
				
				
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }
			 
			 public void TweetStatsQuery()
			 {
				 URL url=getClass().getResource("tweitCollectfinal.txt");
		            String pathToFile = url.toString();
				 	
				    SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
				   
				    JavaSparkContext sc = new JavaSparkContext(conf);

				    JavaSQLContext sqlContext = new JavaSQLContext(sc);

				    JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

				    tweets.registerAsTable("tweetTable");

				    tweets.printSchema();

				    nbTweetByStats(sqlContext);

				    sc.stop();
				    
//				    String htmlurl = "http://twitteranalysispbm.mybluemix.net/tweet_status_analysis.html";
//			        try {
//			 			java.awt.Desktop.getDesktop().browse(java.net.URI.create(htmlurl));
//			 		} catch (IOException e) {
//			 			e.printStackTrace();
//			 		}
			 }
			 
			 private void nbTweetByStats(JavaSQLContext sqlContext) 
			 {
			 		  
				  try
				  {
					  File outputFile = new File(getServletContext().getRealPath("/")
					            + "/query9.csv");
					
				 FileWriter fw= new FileWriter(outputFile);
			   
			     JavaSchemaRDD totalcount = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable "+
			    		                                    "WHERE created_at is not null"                                );
			    
			     JavaSchemaRDD count = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
			  										  "WHERE retweeted='true' or retweet_count>'0' ");
			    
			     JavaSchemaRDD count1 = sqlContext.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						                               "WHERE retweeted='flase' or retweet_count=0 ");
			 
			 
			 List<Row> totalrows=totalcount.collect();	 
			 String totalrows12=totalrows.toString();
			 String totalrows1 = totalrows12.substring(totalrows12.indexOf("[") + 2, totalrows12.indexOf("]"));
			 
			 List<Row> retweetcount=count.collect();	 
			 String retweetcount12=retweetcount.toString();
			 String retweetcount1 = retweetcount12.substring(retweetcount12.indexOf("[") + 2, retweetcount12.indexOf("]"));
			 
			 List<Row> notretweetcount=count1.collect();	 
			 String notretweetcount12=notretweetcount.toString();
			 String notretweetcount1 = notretweetcount12.substring(notretweetcount12.indexOf("[") + 2, notretweetcount12.indexOf("]"));
			 
			 System.out.println(totalrows1);
			 System.out.println(retweetcount1);
			 
			int totalrows123=Integer.parseInt(totalrows1);
			
			int retweet123=Integer.parseInt(retweetcount1);
			
			int notretweet=Integer.parseInt(notretweetcount1);
			
			 int deletedtweet=totalrows123- notretweet;
			 
			 System.out.println(notretweet);
			 
			double retweetPercentage=((retweet123*100)/totalrows123);
			double notweetPercentage=((notretweet*100)/totalrows123);
			float deletedtweetPercentage=((deletedtweet*100)/totalrows123);
			
			System.out.println(retweetPercentage);
			System.out.println(notweetPercentage);
			System.out.println(deletedtweetPercentage);
			 
			String retweetPercentage1=Double.toString(retweetPercentage);
			String notweetPercentage1=Double.toString(notweetPercentage);
			String deletedtweetPercentage1=Float.toString(deletedtweetPercentage);
			
			
			    
			    fw.append("TweetStatus");
				fw.append(',');
				fw.append("Percentage");
				fw.append("\n");
				fw.append("Retweet%");
				fw.append(',');
				fw.append(retweetPercentage1);
				fw.append("\n");
				fw.append("Normal tweet%");
				fw.append(',');
				fw.append(notweetPercentage1);
				fw.append("\n");
				fw.append("deleted tweets%");
				fw.append(',');
				fw.append(deletedtweetPercentage1);
				fw.append("\n");
				
				
				
				
				
				
				fw.close();
				
				
			 }
				  catch (Exception exp)
				  {
				  }
				  

			  }
		
	
	

}
