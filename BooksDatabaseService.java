/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <YOUR STUDENT ID HERE>
 *
 */

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;
import java.io.BufferedReader;
import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
    //Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.

// Student ID: 2422336

public class BooksDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public BooksDatabaseService(Socket aSocket){
        
		//TO BE COMPLETED
        this.serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library
		
		String tmp = "";
        try {
			//TO BE COMPLETED
            //Use InputStreamReader and StringBuffer
            InputStreamReader isr = new InputStreamReader(this.serviceSocket.getInputStream());
            StringBuffer br = new StringBuffer();

            //Wait for the "#" to end the while loop
            char notEndChar;
            while (true) {
                notEndChar = (char) isr.read();
                if (notEndChar == '#') break;
                br.append(notEndChar);
            }
            tmp=br.toString();

            //Use the StringTokenizer class to split the tmp string into two parts
            StringTokenizer st = new StringTokenizer(tmp, ";");
            this.requestStr[0] = st.nextToken();
            this.requestStr[1] = st.nextToken();

        }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }

    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		this.outcome = null;
		
		String sql = "SELECT book.title, book.publisher, genre.name AS genre, book.rrp, COUNT(bookcopy.copyID) AS num_copies\n" +
                    "FROM book\n" +
                    "JOIN author ON book.authorID = author.authorID\n" +
                    "JOIN genre ON book.genre = genre.name\n" +
                    "JOIN bookcopy ON book.bookID = bookcopy.bookID\n" +
                    "JOIN library ON bookcopy.libraryID = library.libraryID\n" +
                    "WHERE author.familyname = '" + this.requestStr[0] + "' " + "AND library.city = '" + this.requestStr[1] + "'\n" +
                    "GROUP BY book.bookID, genre.name\n" +
                    "Order BY book.title ASC";

		try {
			//Connet to the database
			//TO BE COMPLETED
            Connection con = DriverManager.getConnection(Credentials.URL, Credentials.USERNAME, Credentials.PASSWORD);
			
			//Make the query
			//TO BE COMPLETED
            PreparedStatement stmt = con.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery();
			
			//Process query
			//TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            RowSetFactory afactory = RowSetProvider.newFactory();
            CachedRowSet crs = afactory.createCachedRowSet();

            //Reset the iterator of the row set
            this.outcome = crs;
            rs.beforeFirst();
            crs.populate(rs);

			//Clean up
            //TO BE COMPLETED
            rs.close();
            stmt.close();
            con.close();
			
		} catch (Exception e){
            System.out.println(e); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(this.serviceSocket.getOutputStream());
            outcomeStreamWriter.writeObject(this.outcome);

            // Traverse the CachedRowSet and output the data to console
            while (outcome.next()) {
                String title = outcome.getString("title");
                String publisher = outcome.getString("publisher");
                String genre = outcome.getString("genre");
                String rrp = outcome.getString("rrp");
                String numCopies = outcome.getString("num_copies");
                System.out.println(title + " | " + publisher + " | " + genre + " | " + rrp + " | " + numCopies);
            }
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);
            
			//Terminating connection of the service socket
			//TO BE COMPLETED
            this.serviceSocket.close();

        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        } catch (SQLException e) {
            System.out.println("Service thread " + this.getId() + ": Failed to traverse CachedRowSet. " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");

            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
