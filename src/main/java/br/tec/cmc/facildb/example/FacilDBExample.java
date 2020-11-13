package br.tec.cmc.facildb.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.h2.tools.Server;
import org.json.JSONArray;
import org.json.JSONObject;

import br.tec.cmc.facildb.FacilDB;
import br.tec.cmc.facildb.FacilH2;

/**
 * Example using FacilDB with H2 database
 */
public class FacilDBExample {

    private FacilDB db = null;
    private static final String DB_DIR = System.getProperty("java.io.tmpdir");
    
    public static void main(String[] args) {
        (new FacilDBExample()).run();
    }

    public void run() {

        try {
            initH2Server();

            // Open connection to a H2 Database database called 'testdb'
            db = new FacilH2("localhost", "9092", "", DB_DIR + "/testdb", "root", "1234");

            // Create Publisher Table
            db.sql(sqlCreatePublisherTable()).execute();
            insertPublisher(1000L, "Wiley");
            insertPublisher(1001L, "Addison-Wesley");
            insertPublisher(1002L, "Acme Books");

            // Create Book Table
            db.sql(sqlCreateBookTable()).execute();
            insertBook(2000L, "Linux Bible", "Christopher Negus", "978-1119578888", 1000L);
            insertBook(2001L, "Effective Java 3rd Edition", "Joshua Bloch", "978-0134685991", 1001L);
            insertBook(2002L, "Refactoring: Improving the Design of Existing Code (2nd Edition) ", "Martin Fowler", "978-0134757599", 1001L);
            insertBook(2003L, "TCP/IP Illustrated, Volume 1: The Protocols", "Kevin Fall and W. Stevens", "978-0321336316", 1001L);
            insertBook(2004L, "Drawing Cartoons", "John Silver", "965-33245667", 1002L);

            // Select book by author
            JSONArray list = db.select("title, author, isbn")
                               .from("book")
                               .where("publisher_id=?")
                               .orderBy("author")
                               .param(1001L)
                               .query();

            for (int i=0; i<list.length(); i++) {
                JSONObject rec = list.getJSONObject(i);
                System.out.println("\n>>> " + rec.getString("title") + ", " + 
                                   rec.getString("isbn") + ", " +
                                   rec.getString("author"));
            }

            // Update a book
            String newTitle = "Drawing Cartoons the Easy Way";

            db.update("book")
              .fields("title")
              .where("id=?")
              .param(newTitle)
              .param(2004L)
              .execute();

            JSONObject rec = db.select("id, title")
                               .from("book")
                               .where("id=?")
                               .param(2004L)
                               .queryUnique();

            System.out.println("\n" + rec.toString(3));

            // Count books
            long total = db.sql("select count(*) from book").queryCount();
            System.out.println("\n>> Total Books: " + total);
                           
        } catch (SQLException sqe) {
            sqe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                if (db != null) {
                    db.closeConnection();
                }
                Server.shutdownTcpServer("tcp://localhost:9092", "1234", true, true);
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    private void initH2Server() throws SQLException, IOException {
        // Delete database file if exists to start a new one
        File file = new File(DB_DIR + "/testdb.mv.db");
        if (file.exists()) {
            Files.delete(Paths.get(DB_DIR + "/testdb.mv.db"));
        }

        // Start H2 Server
        Server.createTcpServer("-tcpPort", "9092", "-tcpAllowOthers", "-tcpPassword", "1234", "-ifNotExists").start();
    }

    private String sqlCreatePublisherTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("create table publisher (");
        sb.append("   id bigint not null,");
        sb.append("   pub_name varchar(80) not null,");
        sb.append("   primary key (id)");
        sb.append(")");
        return sb.toString();
    }

    private String sqlCreateBookTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("create table book (");
        sb.append("   id bigint not null,");
        sb.append("   title varchar(80) not null,");
        sb.append("   author varchar(50) not null,");
        sb.append("   isbn varchar(15) not null,");
        sb.append("   publisher_id  bigint not null,");
        sb.append("   primary key (id),");
        sb.append("   foreign key (publisher_id) references publisher(id)");
        sb.append(")");
        return sb.toString();
    }

    private void insertPublisher(Long id, String pubName) throws SQLException {
        db.insert("publisher")
          .fields("id, pub_name")
          .param(id)
          .param(pubName)
          .execute();
    }

    private void insertBook(Long id, String title, String author, String isbn, Long pubId) throws SQLException {
        db.insert("book")
          .fields("id, title, author, isbn, publisher_id")
          .param(id)
          .param(title)
          .param(author)
          .param(isbn)
          .param(pubId)
          .execute();
    }
}
