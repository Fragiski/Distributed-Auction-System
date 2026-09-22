import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class AuctionServer {
    // --- Variables of current auction ---
    public static AuctionSession[] activeSessions = new AuctionSession[] {
            new AuctionSession(0),
            new AuctionSession(1)
        };

    public static final String RESET = "\u001B[0m";
    public static final String YELLOW = "\u001B[33m";
    public static final String RED = "\u001B[31m";

    // Object for synchronization (Lock) to avoid race conditions
    public static final Object auctionLock = new Object();

    // The port on which the Server listens (known from the beginning)
    private static final int PORT = 8080;

    // Data Structure 1: Registered Users (Key: username, Value: User object)
    // Holds the information of each registered peer
    public static ConcurrentHashMap<String, User> registeredUsers = new ConcurrentHashMap<>();

    // Data Structure 2: Active (logged in) users (Key: token_id, Value: ActiveUser object)
    // Maintains the contact information of each peer that is logged in
    public static ConcurrentHashMap<String, ActiveUser> activeUsers = new ConcurrentHashMap<>();

    // Data Structure 3: List used as a safe FCFS Queue supporting index lookups for Phase 2 reputation matching
    public static final ArrayList<String> auctionQueue = new ArrayList<>();

    // Data Structure 4: Map (obejctId, bidderToken) to keep track of winning objects and the token of the winner
    public static ConcurrentHashMap<String, String> completedAuctionWinners = new ConcurrentHashMap<>();

    
    // Class bid: So that we keep up with the ongoing bids
    public static class Bid {
        public String bidderToken;
        public int amount;
        
        public Bid(String bidderToken, int amount) {
            this.bidderToken = bidderToken;
            this.amount = amount;
        }
    }

   // public static java.util.ArrayList<Bid> currentAuctionBids = new java.util.ArrayList<>();


   // Global broadcast helper required by tasks and handlers
    public static void broadcast(String message) {
        for (ActiveUser user : activeUsers.values()) {
            if (user.getOut() != null) {
                user.getOut().println(message);
            }
        }
    }

    // Direct message helper required to notify buyer and seller privately
    public static void sendDirectMessage(String token, String message) {
        ActiveUser user = activeUsers.get(token);
        if (user != null && user.getOut() != null) {
            user.getOut().println(message);
        }
    }


    // To check if a seller is still connected to the server
    public static boolean isSellerActive(String token) {

        ActiveUser seller = activeUsers.get(token);
        if (seller == null) return false;

        //checkerror() flushes the stream and returns true if an error occurred.
        return !seller.getOut().checkError();
    }

    public static void main(String[] args) {

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("The Server is ready and waiting for connections.");

            // Starting TWO auction management threads for concurrent auctions
            Thread auctionManager0 = new Thread(new AuctionManagerTask(AuctionServer.activeSessions[0]));
            Thread auctionManager1 = new Thread(new AuctionManagerTask(AuctionServer.activeSessions[1]));
            auctionManager0.start();
            auctionManager1.start();

            // Endless loop to continuously accept new peers
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New connection from: " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());

                // Supports multiple threads simultaneously, creating a separate thread for each peer
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                new Thread(clientHandler).start();
            }
        } catch (IOException e) {
            System.err.println("Error while running the Server: " + e.getMessage());
        }
    }
    
}

    //Represents a peer that is currently logged into the Auction Server.
    class ActiveUser {
        private String username;
        private String ipAddress;
        private int port; // The port on which the peer itself listens as a Server Socket
        private PrintWriter out; // The output stream used by the Auction Server to send real-time notifications to this specific peer

        public ActiveUser(String username, String ipAddress, int port, PrintWriter out) {
            this.username = username;
            this.ipAddress = ipAddress;
            this.port = port;
            this.out = out;
        }

        public String getUsername() {
            return username;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public int getPort() {
            return port;
        }

        public void setPort (int port) {
            this.port = port;
        }

        //The PrintWriter used to push data (like auction results) from the server to the peer
        public PrintWriter getOut() {
            return out;
        }
}


