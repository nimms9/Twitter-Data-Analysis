

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
    
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		doPost(request, response);
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		
	  int button_selected = Integer.parseInt(request.getParameter("Query selected"));
		  
      switch(button_selected)
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
			
		default:
			
				JOptionPane.showMessageDialog(null, "invalid option");
				break;
			}
		
		
	}
	
	
public void CandidateQuery()
	{
	   
	   URL url=getClass().getResource("tweitCollectfinal.txt");
       String source_data = url.toString();
	   SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");
	   JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);
	   JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);
	   JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);
	   tweet_data.registerAsTable("tweetTable");
	   tweet_data.printSchema();
	   nbTweetByCandidate(jasqlcon);
	   sparkcont.stop();

		
	}

private void nbTweetByCandidate(JavaSQLContext jasqlcon) 
		{		  
			 try
			 {
				  File out_put = new File(getServletContext().getRealPath("/")
				            + "/query1.csv");
				
			 FileWriter flle_add= new FileWriter(out_put);

		    JavaSchemaRDD candidate = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
		    										"WHERE text LIKE '%realDonaldTrump%'");
		    JavaSchemaRDD candidate1 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%HillaryClinton%'");
		    JavaSchemaRDD candidate2 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%JebBush%'");
		    JavaSchemaRDD candidate3 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%RealBenCarson%'");
		    JavaSchemaRDD candidate4 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%ChrisChristie%'");
		    JavaSchemaRDD candidate5 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%tedcruz%'");
		    JavaSchemaRDD candidate6 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%CarlyFiorina%'");
		    JavaSchemaRDD candidate7 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%gov_gilmore%'");
		    JavaSchemaRDD candidate8 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%JohnKasich%'");
		    JavaSchemaRDD candidate9 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%marcorubio%'");
		    JavaSchemaRDD candidate10 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
					"WHERE text LIKE '%BernieSanders%'");
		 
		 List<Row> realDonaldTrump=candidate.collect();	 
		 String realDonaldTrump12=realDonaldTrump.toString();
		 String realDonaldTrump1 = realDonaldTrump12.substring(realDonaldTrump12.indexOf("[") + 2, realDonaldTrump12.indexOf("]"));
		 
		 List<Row> HillaryClinton=candidate1.collect();
		 String HillaryClinton12=HillaryClinton.toString();
		 String HillaryClinton1 = HillaryClinton12.substring(HillaryClinton12.indexOf("[") + 2, HillaryClinton12.indexOf("]"));
		 
		 List<Row> JebBush=candidate2.collect();
		 String JebBush12=JebBush.toString();
		 String JebBush1 = JebBush12.substring(JebBush12.indexOf("[") + 2, JebBush12.indexOf("]"));
		 
		 List<Row> RealBenCarson=candidate3.collect();
		 String RealBenCarson12=RealBenCarson.toString();
		 String RealBenCarson1 = RealBenCarson12.substring(RealBenCarson12.indexOf("[") + 2, RealBenCarson12.indexOf("]"));
		 
		 List<Row> ChrisChristie=candidate4.collect();
		 String ChrisChristie12=ChrisChristie.toString();
		 String ChrisChristie1 = ChrisChristie12.substring(ChrisChristie12.indexOf("[") + 2, ChrisChristie12.indexOf("]"));
		 
		 List<Row> tedcruz=candidate5.collect();
		 String tedcruz12=tedcruz.toString();
		 String tedcruz1 = tedcruz12.substring(tedcruz12.indexOf("[") + 2, tedcruz12.indexOf("]"));
		 
		 List<Row> CarlyFiorina=candidate6.collect();
		 String CarlyFiorina12=CarlyFiorina.toString();
		 String CarlyFiorina1 = CarlyFiorina12.substring(CarlyFiorina12.indexOf("[") + 2, CarlyFiorina12.indexOf("]"));
		 
		 List<Row> gov_gilmore=candidate7.collect();
		 String gov_gilmore12=gov_gilmore.toString();
		 String gov_gilmore1 = gov_gilmore12.substring(gov_gilmore12.indexOf("[") + 2, gov_gilmore12.indexOf("]"));
		 
		 List<Row> JohnKasich=candidate8.collect();
		 String JohnKasich12=JohnKasich.toString();
		 String JohnKasich1 = JohnKasich12.substring(JohnKasich12.indexOf("[") + 2, JohnKasich12.indexOf("]"));
		 
		 List<Row> marcorubio=candidate9.collect();
		 String marcorubio12=marcorubio.toString();
		 String marcorubio1 = marcorubio12.substring(marcorubio12.indexOf("[") + 2, marcorubio12.indexOf("]"));
		 
		 List<Row> BernieSanders=candidate10.collect();
		 String BernieSanders12=BernieSanders.toString();
		 String BernieSanders1 = BernieSanders12.substring(BernieSanders12.indexOf("[") + 2, BernieSanders12.indexOf("]"));
		
		    
		    flle_add.append("Language");
			flle_add.append(',');
			flle_add.append("Count");
			flle_add.append("\n");
			flle_add.append("Donald Trump");
			flle_add.append(',');
			flle_add.append(realDonaldTrump1);
			flle_add.append("\n");
			flle_add.append("Hillary Clinton");
			flle_add.append(',');
			flle_add.append(HillaryClinton1);
			flle_add.append("\n");
			flle_add.append("Jeb Bush");
			flle_add.append(',');
			flle_add.append(JebBush1);
			flle_add.append("\n");
			flle_add.append("Ben Carson");
			flle_add.append(',');
			flle_add.append(RealBenCarson1);
			flle_add.append("\n");
			flle_add.append("Chris Christie");
			flle_add.append(',');
			flle_add.append(ChrisChristie1);
			flle_add.append("\n");
			flle_add.append("Ted Cruz");
			flle_add.append(',');
			flle_add.append(tedcruz1);
			flle_add.append("\n");
			flle_add.append("Carly Fiorina");
			flle_add.append(',');
			flle_add.append(CarlyFiorina1);
			flle_add.append("\n");
			flle_add.append("gov_gilmore");
			flle_add.append(',');
			flle_add.append(gov_gilmore1);
			flle_add.append("\n");
			flle_add.append("John Kasich");
			flle_add.append(',');
			flle_add.append(JohnKasich1);
			flle_add.append("\n");
			flle_add.append("Marcorubio");
			flle_add.append(',');
			flle_add.append(marcorubio1);
			flle_add.append("\n");
			flle_add.append("Bernie Sanders");
			flle_add.append(',');
			flle_add.append(BernieSanders1);
			flle_add.append("\n");
			flle_add.close();
		 }
		catch (Exception exp)
			  {
				  exp.printStackTrace();
				  
			  }

		  }

	  	  
	        public void Top8UsersTweetsCount()
			{
	        	URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
			    SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");
			    JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);
			   
			    JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);
			    JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);
			    tweet_data.registerAsTable("tweetTable");

			    tweet_data.printSchema();

			    nbTweetByUser(jasqlcon);

			    sparkcont.stop();
			  
			}
			
			private void nbTweetByUser(JavaSQLContext jasqlcon) 
			{		  
				 try
				 {
					 
					 File out_put = new File(getServletContext().getRealPath("/")
							 
					            + "/query2.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);
			   
			    JavaSchemaRDD count = jasqlcon.sql("SELECT user.name,max(user.statuses_count) AS c FROM tweetTable " +
			    		                             "GROUP BY user.name ORDER BY c");
		       List<org.apache.spark.sql.api.java.Row> row_data = count.collect(); 

		       Collections.reverse(row_data);
			    
			    String row_count=row_data.toString();
			   String[] data_array = row_count.split("],"); 
			    
			    System.out.println(row_count);
			    
			    flle_add.append("Name");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
				
				

				for(int i = 0; i < 8; i++)
				{
					if(i==0)
					{
						flle_add.append(data_array[0].substring(2));
						flle_add.append(',');
						flle_add.append("\n");
					}
					else {
					flle_add.append(data_array[i].substring(2));
					flle_add.append(',');
					flle_add.append("\n");
					}
				}
				
				flle_add.close();
				
				
			 }
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }

			  }

			
			public void Top8UsersFollowers()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
				 SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);
		 
		         JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

		         JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

		         tweet_data.registerAsTable("tweetTable");

		        tweet_data.printSchema();

		       nbTweetByFollower(jasqlcon);

		        sparkcont.stop();


			}
			
			 private void nbTweetByFollower(JavaSQLContext jasqlcon) {
				  
				  try
				  {
					  File out_put = new File(getServletContext().getRealPath("/")
					            + "/query3.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);
			   
			    JavaSchemaRDD count = jasqlcon.sql("SELECT user.name, max(user.followers_count) AS c FROM tweetTable " +
			                                         "GROUP BY user.name ORDER BY c");
			    
			    List<org.apache.spark.sql.api.java.Row> row_data = count.collect(); 

			       Collections.reverse(row_data);
				    
				   String row_count=row_data.toString();
			    
			       String[] data_array = row_count.split("],"); 
			    
			
			    
			    flle_add.append("Name");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
				
				

				for(int i = 0; i < 8; i++)
				{
					if(i==0)
					{
						flle_add.append(data_array[0].substring(2));
						flle_add.append(',');
						flle_add.append("\n");
					}
					else {
					flle_add.append(data_array[i].substring(2));
					flle_add.append(',');
				    flle_add.append("\n");
					}
				}
				
				flle_add.close();
				
				
			 }
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }
				  

			  }
			
			public void TopLocations()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
				SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");

		        JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);

		        JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

		        JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

		        tweet_data.registerAsTable("tweetTable");
		       

		        nbTweetByLocation(jasqlcon);

		        sparkcont.stop();

			}
			
		  private void nbTweetByLocation(JavaSQLContext jasqlcon) 
		  {
				  
			     try
				  {
			    	 File out_put = new File(getServletContext().getRealPath("/")
					            + "/query4.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);

				 JavaSchemaRDD count = jasqlcon.sql("SELECT user.location, COUNT(*) AS c FROM tweetTable " +
						  								"Group By user.location " +
		                    							"order by c" );
			       
				  List<org.apache.spark.sql.api.java.Row> row_data = count.collect(); 
				    
			       Collections.reverse(row_data);
				    
				   String row_count=row_data.toString();
				    
				   String[] data_array = row_count.split("],"); 
				    
				   System.out.println(row_count);
			    
		        flle_add.append("Location");
				flle_add.append(',');
				flle_add.append("second location");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
			
				for(int i = 0; i < 12; i++)
				{
					if((i==0)||(i==1)||(i==2))
					{
						continue;
					}
					
					else if(i == data_array.length-1)
					{
						flle_add.append(data_array[i].substring(2,data_array[i].length()-2));
						flle_add.append(',');
						flle_add.append("\n");
					}
					else {
					flle_add.append(data_array[i].substring(2));
					flle_add.append(',');
					flle_add.append("\n");
					}
				}
				
				flle_add.close();
				
				
			 }
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }
				  

			  }

			
			public  void Friends()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
				 SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);

		         JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

		         JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

		         tweet_data.registerAsTable("tweetTable");

		         tweet_data.printSchema();

		          nbTweetByFriends(jasqlcon);

		          sparkcont.stop();

			}
			
			 private void nbTweetByFriends(JavaSQLContext jasqlcon) {
				  
				  try
				  {
					  File out_put = new File(getServletContext().getRealPath("/")
					            + "/query5.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);
			   
			    JavaSchemaRDD count = jasqlcon.sql("SELECT user.screen_name, max(user.friends_count) AS c FROM tweetTable " +
			    										"WHERE user.friends_count>'150000'" +
			    		                                  "group by user.screen_name order by c" ); 
			   
			    List<org.apache.spark.sql.api.java.Row> row_data = count.collect(); 
			    
		       Collections.reverse(row_data);
			    
			    String row_count=row_data.toString();
			    
			   String[] data_array = row_count.split("],"); 
			    
			    System.out.println(row_count);
			    
			    flle_add.append("Name");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
				
				

				for(int i = 0; i < data_array.length; i++)
				{
					if(i==0)
					{
						flle_add.append(data_array[0].substring(2));
						flle_add.append(',');
						flle_add.append("\n");
					}
					else if(i == data_array.length-1)
					{
						flle_add.append(data_array[i].substring(2,data_array[i].length()-2));
						flle_add.append(',');
						flle_add.append("\n");
					}
					else {
					flle_add.append(data_array[i].substring(2));
					flle_add.append(',');
					flle_add.append("\n");
					}
				}
				
				flle_add.close();
				
				
			 }
				  
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }

			  }

			
			public void TimeQuery()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
				 SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);
		         JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

		         JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

		        tweet_data.registerAsTable("tweetTable");

		       tweet_data.printSchema();

		       nbTweetByTime(jasqlcon);

		       sparkcont.stop();

		     
			}
			
			 private void nbTweetByTime(JavaSQLContext jasqlcon) 
			 {		  
				  try
				  {
					  File out_put = new File(getServletContext().getRealPath("/")
					            + "/query6.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);
			   
			    JavaSchemaRDD count = jasqlcon.sql("SELECT created_at, COUNT(*) AS c FROM tweetTable " +
			    										"Group By created_at " +
			    		                                  "order by c" );
			    	   
			    List<org.apache.spark.sql.api.java.Row> row_data = count.collect(); 
			    
		        Collections.reverse(row_data);
			    
			    String row_count=row_data.toString();
			    
			    String[] data_array = row_count.split("],"); 
			    
			    System.out.println(row_count);
			    
			    flle_add.append("Time");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
				
				

				for(int i = 0; i < 9; i++)
				{
					if(i==0)
					{
						continue;
					}
					else if(i == data_array.length-1)
					{
						flle_add.append(data_array[i].substring(2,data_array[i].length()-2));
						flle_add.append(',');
						flle_add.append("\n");
					}
					else {
					flle_add.append(data_array[i].substring(2));
					flle_add.append(',');
					flle_add.append("\n");
					}
				}
				
				flle_add.close();
				
				
			 }
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }
			  }
			
			public void SentimentAnalysisQuery()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
		       SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");

		       JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);

		       JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

		       JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

		       tweet_data.registerAsTable("tweetTable");

		       tweet_data.printSchema();

		       nbTweetBySentiment(jasqlcon);

		       sparkcont.stop();
		       

			}
			
			private void nbTweetBySentiment(JavaSQLContext jasqlcon) 
			
			{
			    try
				{
			     
			    	 File out_put = new File(getServletContext().getRealPath("/")
					            + "/Query7.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);
			   
			  JavaSchemaRDD count = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
			  										"WHERE text LIKE '%abundant%' OR text LIKE '%accessible%' OR text LIKE '%accurate%' OR text LIKE '%award%' OR text LIKE '%awesome%' OR text LIKE '%beautiful%' OR text LIKE '%affirmation%' OR text LIKE '%amicable%' OR text LIKE '%appreciate%' OR text LIKE '%approve%' OR text LIKE '%attractive%' OR text LIKE '%benefit%' OR text LIKE '%bless%' OR text LIKE '%bonus%' OR text LIKE '%brave%' OR text LIKE '%bright%' OR text LIKE '%brilliant%' OR text LIKE '%celebrate%' OR text LIKE '%champion%' OR text LIKE '%charm%' OR text LIKE '%cheer%' OR text LIKE '%clever%' OR text LIKE '%colorful%' OR text LIKE '%comfort%' OR text LIKE '%compliment%' OR text LIKE '%confidence%' OR text LIKE '%congratulation%' OR text LIKE '%cute%' OR text LIKE '%good%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%easy%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%fair%' OR text LIKE '%excite%' OR text LIKE '%fast%' OR text LIKE '%fine%' OR text LIKE '%fortunate%' OR text LIKE '%free%' OR text LIKE '%fresh%' OR text LIKE '%fun%' OR text LIKE '%gain%' OR text LIKE '%gem%' OR text LIKE '%gorgeous%' OR text LIKE '%grand%' OR text LIKE '%handsome%' OR text LIKE '%healthy%' OR text LIKE '%honest%' OR text LIKE '%humor%' OR text LIKE '%important%' OR text LIKE '%impress%' OR text LIKE '%improve%' OR text LIKE '%joy%' OR text LIKE '%love%' OR text LIKE '%perfect%' OR text LIKE '%pleasant%' OR text LIKE '%compliment%' OR text LIKE '%pleasure%' OR text LIKE '%precious%' OR text LIKE '%prolific%' OR text LIKE '%prudent%' OR text LIKE '%happy%' OR text LIKE '%cool%' OR text LIKE '%proven%' OR text LIKE '%effective%' OR text LIKE '%efficient%' OR text LIKE '%restored%'  ");
			  JavaSchemaRDD count1 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE text LIKE '%abuse%' OR text LIKE '%abyss%' OR text LIKE '%absurd%' OR text LIKE '%akward%' OR text LIKE '%adverse%' OR text LIKE '%agony%' OR text LIKE '%annoying%' OR text LIKE '%anti%' OR text LIKE '%arrogant%' OR text LIKE '%assassinate%' OR text LIKE '%aversion%' OR text LIKE '%backward%' OR text LIKE '%bad%' OR text LIKE '%brutal%' OR text LIKE '%battered%' OR text LIKE '%berate%' OR text LIKE '%bewitch%' OR text LIKE '%berate%' OR text LIKE '%blunder%' OR text LIKE '%complain%' OR text LIKE '%conflict%' OR text LIKE '%confound%' OR text LIKE '%contagious%' OR text LIKE '%contaminated%' OR text LIKE '%contravene%' OR text LIKE '%corruption%' OR text LIKE '%corrupt%' OR text LIKE '%coward%' OR text LIKE '%cruel%' OR text LIKE '%sad%' OR text LIKE '%danger%' OR text LIKE '%debase%' OR text LIKE '%decline%' OR text LIKE '%deceive%' OR text LIKE '%defamation%' OR text LIKE '%demon%' OR text LIKE '%demolish%' OR text LIKE '%denied%' OR text LIKE '%demolish%' OR text LIKE '%depress%' OR text LIKE '%deny%' OR text LIKE '%destroy%' OR text LIKE '%devastation%' OR text LIKE '%disadvantage%' OR text LIKE '%disappointed%' OR text LIKE '%discord%' OR text LIKE '%evil%' OR text LIKE '%gossip%' OR text LIKE '%hard%' OR text LIKE '%gloom%' OR text LIKE '%hate%' OR text LIKE '%hazard%' OR text LIKE '%fuck%' OR text LIKE '%horrible%' OR text LIKE '%idiot%' OR text LIKE '%imperfect%' OR text LIKE '%inefficient%' OR text LIKE '%inflammation%' OR text LIKE '%ironic%' OR text LIKE '%irritate%' OR text LIKE '%jealous%' OR text LIKE '%lag%' OR text LIKE '%lie%' OR text LIKE '%malignant%' OR text LIKE '%malign%' OR text LIKE '%noisy%' OR text LIKE '%odd%' OR text LIKE '%offence%' OR text LIKE '%offend%' OR text LIKE '%offensive%' OR text LIKE '%bad%' OR text LIKE '%unhappy%' OR text LIKE '%weak%'");
			 
			 List<Row> positive=count.collect();	 
			 String positive12=positive.toString();
			 String positive1 = positive12.substring(positive12.indexOf("[") + 2, positive12.indexOf("]"));
			 
			 List<Row> negative=count1.collect();
			 String negative12=negative.toString();
			 String negative1 = negative12.substring(negative12.indexOf("[") + 2, negative12.indexOf("]"));
			 
			    
			    flle_add.append("Words");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
				flle_add.append("PositiveTweets");
				flle_add.append(',');
				flle_add.append(positive1);
				flle_add.append("\n");
				flle_add.append("NegativeTweets");
				flle_add.append(',');
				flle_add.append("-"+negative1);
				flle_add.append("\n");
				flle_add.close();
				
				
			 }
			    catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }
			  }
			
			public void SourceQuery()
			{
				URL url=getClass().getResource("tweitCollectfinal.txt");
	            String source_data = url.toString();
				 SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");

		         JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);

		         JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

		         JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

		         tweet_data.registerAsTable("tweetTable");

		         tweet_data.printSchema();

		         nbTweetBySourceQuery(jasqlcon);

		        sparkcont.stop();

			}
			
			 private void nbTweetBySourceQuery(JavaSQLContext jasqlcon) {
				  
				  try
				  {
					  File out_put = new File(getServletContext().getRealPath("/")
					            + "/query8.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);

			    JavaSchemaRDD count = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
			    										"WHERE source LIKE '%Android%'");
			    JavaSchemaRDD count1 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%iPhone%'");
			    JavaSchemaRDD count2 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%iPad%'");
			    JavaSchemaRDD count3 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%Windows%'");
			    JavaSchemaRDD count4 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
						"WHERE source LIKE '%Web Client%'");
			   
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
			 
			
			    
			    flle_add.append("DeviceName");
				flle_add.append(',');
				flle_add.append("Count");
				flle_add.append("\n");
				flle_add.append("Android");
				flle_add.append(',');
				flle_add.append(Android1);
				flle_add.append("\n");
				flle_add.append("iPhone");
				flle_add.append(',');
				flle_add.append(iPhone1);
				flle_add.append("\n");
				flle_add.append("iPad");
				flle_add.append(',');
				flle_add.append(iPad1);
				flle_add.append("\n");
				flle_add.append("Windows");
				flle_add.append(',');
				flle_add.append(Windows1);
				flle_add.append("\n");
				flle_add.append("Web Client");
				flle_add.append(',');
				flle_add.append(Web_Client1);
				flle_add.append("\n");
			
				
				flle_add.close();
				
				
			 }
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }
			  }
			 
			 public void TweetStatsQuery()
			 {
				 URL url=getClass().getResource("tweitCollectfinal.txt");
		            String source_data = url.toString();
				 	
				    SparkConf sparkobj = new SparkConf().setAppName("User mining").setMaster("local[*]");
				   
				    JavaSparkContext sparkcont = new JavaSparkContext(sparkobj);

				    JavaSQLContext jasqlcon = new JavaSQLContext(sparkcont);

				    JavaSchemaRDD tweet_data = jasqlcon.jsonFile(source_data);

				    tweet_data.registerAsTable("tweetTable");

				    tweet_data.printSchema();

				    nbTweetByStats(jasqlcon);

				    sparkcont.stop();

			 }
			 
			 private void nbTweetByStats(JavaSQLContext jasqlcon) 
			 {
			 		  
				  try
				  {
					  File out_put = new File(getServletContext().getRealPath("/")
					            + "/query9.csv");
					
				 FileWriter flle_add= new FileWriter(out_put);
			   
			     JavaSchemaRDD totalcount = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable "+
			    		                                    "WHERE created_at is not null"                                );
			    
			     JavaSchemaRDD count = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
			  										  "WHERE retweeted='true' or retweet_count>'0' ");
			    
			     JavaSchemaRDD count1 = jasqlcon.sql("SELECT  COUNT(*) AS c FROM tweetTable " +
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
			 
			
			int total_row=Integer.parseInt(totalrows1);
			
			int re_tweet=Integer.parseInt(retweetcount1);
			
			int no_re_tweet=Integer.parseInt(notretweetcount1);
			
			 int tweet_deleted=total_row- no_re_tweet;
		
			double re_tweet_percentage=((re_tweet*100)/total_row);
			double no_re_tweet_percentage=((no_re_tweet*100)/total_row);
			float tweet_deleted_percentage=((tweet_deleted*100)/total_row);
			
		
			String re_tweet_percentage_count=Double.toString(re_tweet_percentage);
			String no_re_tweet_percentage_count=Double.toString(no_re_tweet_percentage);
			String tweet_deleted_percentage_count=Float.toString(tweet_deleted_percentage);
			
			    flle_add.append("TweetStatus");
				flle_add.append(',');
				flle_add.append("Percentage");
				flle_add.append("\n");
				flle_add.append("Retweet%");
				flle_add.append(',');
				flle_add.append(re_tweet_percentage_count);
				flle_add.append("\n");
				flle_add.append("Normal tweet%");
				flle_add.append(',');
				flle_add.append(no_re_tweet_percentage_count);
				flle_add.append("\n");
				flle_add.append("deleted tweet_data%");
				flle_add.append(',');
				flle_add.append(tweet_deleted_percentage_count);
				flle_add.append("\n");
				flle_add.close();
			 }
				  catch (Exception exp)
				  {
					  exp.printStackTrace();
				  }

			  }
		
	
	

}
