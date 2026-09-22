import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Represents a client in the distributed auction system.
 * Acts as both a client (connecting to the Auction Server) 
 * and a temporary server (accepting P2P connections from buyers).
 */

public class Peer {

    // ANSI Escape Codes for coloring terminal output
    public static final String RESET = "\u001B[0m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String PURPLE = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String WHITE = "\u001B[37m";

    private static final String SERVER_ADDRESS = "localhost"; // IP of Auction Server
    private static final int SERVER_PORT = 8080; // Pre-defined Auction Server port
    private static String currentTokenId = null;
    private static int peerListenPort = 0; // Randomly assigned port for P2P connections
    private static int objectCounter = 1; // Counter for objects
    private static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static String currentUsername = null;
    private static File currentSharedDir = null;

    public static void main(String[] args) {

            // [1]. Starting P2P Server (Accepting connections from successful bidders)
            startP2PServer();

            // [2]. Establishment of connection with the central Auction Server
            connectToAuctionServer();
    }

// -----------------------------------
// [1]. P2P SERVER LOGIC (Seller Side)
//-----------------------------------

    private static void startP2PServer() {
        try {
                ServerSocket peerServerSocket = new ServerSocket(0); //  port 0 (random available port) 
                peerListenPort = peerServerSocket.getLocalPort();
                System.out.println("Peer listens for direct connections (P2P) on port: " + peerListenPort);
                
                // Thread handling direct file transfer requests from successful bidders
                Thread peerServerThread = new Thread(() -> {
                    try {
                        while (true) {
                            Socket incomingSocket = peerServerSocket.accept();
                            new Thread(() -> handleIncomingP2PRequest(incomingSocket)).start();
                        }
                    } catch (IOException e) {
                        System.err.println("Peer Server Socket Error.");
                    }
                });
                peerServerThread.setDaemon(true);
                peerServerThread.start();

            } catch (IOException e) {
                System.err.println("The Peer Server could not be started.");
            }
    }

   /**
    * Handles the P2P file transfer using the Go-Back-N (GBN) protocol over UDP.
    * This method is triggered when a buyer connects to claim their won item.
    */
    private static void handleIncomingP2PRequest(Socket incomingSocket) {
        try {

            // Step 1: Receive the requested Object ID via the existing TCP connection 
            BufferedReader p2pIn = new BufferedReader(new InputStreamReader(incomingSocket.getInputStream()));
            String requestedObjectId = p2pIn.readLine().trim();

            if (requestedObjectId == null) {
                System.err.println("[GBN-Seller] Error: Received empty P2P request message.");
                return;
            }
            
            requestedObjectId = requestedObjectId.trim();
            System.out.println("[DEBUG-Seller] Raw P2P message received: '" +  requestedObjectId + "'");

            if (!requestedObjectId.contains("|")) {
                System.err.println("[GBN-Seller] Error: Invalid message format. Missing '|'. Message was: " +  requestedObjectId);
                return;
            }

            String[] parts = requestedObjectId.split("\\|");
            int buyerUdpPort = Integer.parseInt(parts[1]);

            System.out.println("[DEBUG-Seller] Sending UDP packets to port: " + buyerUdpPort);

            // Step 2: Locate the file in the peer's unique directory
            String fileName = parts[0] + ".txt";
            System.out.println("[DEBUG-Seller] Searching for file: " + fileName);

            File fileToSend = new File(currentSharedDir, fileName);
            if (!fileToSend.exists()) {
                System.err.println("[GBN] Error: File not found in " + currentSharedDir.getName());
                return;
            }

            // Step 3: Read the metadata file content into a byte array
            byte[] fileData;
            try (FileInputStream fis = new FileInputStream(fileToSend)) {
                fileData = fis.readAllBytes();
            }

            // Step 4: Setup UDP Socket for the reliable transfer over unreliable channel
            DatagramSocket udpSocket = new DatagramSocket();
            InetAddress buyerAddress = incomingSocket.getInetAddress();
            // We use TCP port + 1 for UDP to avoid conflicts
            

            System.out.println("[GBN] Target UDP Port: " + buyerUdpPort);

            // GBN Variables 
            int packetSize = 64; // Each packet is restricted to 64 bytes 
            int totalPackets = (int) Math.ceil(fileData.length / (double) packetSize);
            int base = 0;        // Start of the current window
            int nextSeqNum = 0;  // Next sequence number to send
            int N = 3;           // Window size 

            int consecutiveTimeouts = 0;
            int maxTimeouts = 5;
         
            System.out.println("[GBN] Starting transfer of " + requestedObjectId + " (" + totalPackets + " packets)");

            boolean transferSuccess = true;

            // Step 5: Main Go-Back-N Loop
            while (base < totalPackets) {
                    
                // Send packets as long as the window is not full 
                while (nextSeqNum < base + N && nextSeqNum < totalPackets) {
                    // Calculate bytes for current packet
                    int offset = nextSeqNum * packetSize;
                    int length = Math.min(packetSize, fileData.length - offset);
                        
                    // Create a string message: "seqNum|data"
                    String payload = nextSeqNum + "|" + new String(fileData, offset, length);
                    byte[] sendData = payload.getBytes();
                        
                    DatagramPacket packet = new DatagramPacket(sendData, sendData.length, buyerAddress, buyerUdpPort);
                    udpSocket.send(packet);
                        
                    System.out.println("[GBN] Sent Packet " + nextSeqNum);
                    nextSeqNum++;
                }
            

                // Step 6: Wait for ACKs with a 2-second timeout 
                udpSocket.setSoTimeout(2000); 
                try {
                    byte[] ackBuffer = new byte[1024];
                    DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                    udpSocket.receive(ackPacket);
                        
                    // Parse the cumulative ACK from the buyer 
                    int ackNum = Integer.parseInt(new String(ackPacket.getData()).trim());
                    System.out.println("[GBN] Received ACK for packet: " + ackNum);

                    // If ACK is valid, move the window base forward
                    if (ackNum >= base) {
                        base = ackNum + 1; 
                        consecutiveTimeouts = 0;
                    }
                
                } catch (SocketTimeoutException e) {
                    // Step 7: Handle Timeout - Retransmit all packets in the current window 
                    System.out.println("[GBN] Timeout! Retransmitting window starting from packet " + base);
                    nextSeqNum = base; 

                    consecutiveTimeouts++;
                    if (consecutiveTimeouts >= maxTimeouts) {
                        System.out.println("[GBN-Seller] Max retransmissions reached. Assuming Buyer received the file or disconnected. Closing connection.");
                        transferSuccess = false;
                        break; 
                    }
                }
            }
            
            // Step 8: Finalize transaction
            if (transferSuccess) {
                String eofPayload = "EOF";
                DatagramPacket eofPacket = new DatagramPacket(eofPayload.getBytes(), eofPayload.length(), buyerAddress, buyerUdpPort);
                udpSocket.send(eofPacket);
                System.out.println("[GBN] Sent EOF packet to finalize transfer.");
            
                System.out.println("[GBN] Transfer of " + requestedObjectId + " completed successfully.");
                if (fileToSend.delete()) { // Delete file locally after successful P2P transfer 
                    System.out.println("[P2P] Metadata file deleted from seller's directory.");
                }
            } else {
                System.out.println("[GBN] Transfer aborted. Metadata file retained in seller's directory.");
            }
        } catch (Exception e) {
            System.err.println("[GBN] Critical Error during transfer: " + e.getMessage());
        }
    }
    
    


// -----------------------------------
// [2]. AUCTION SERVER CONNECTION & UI
//-----------------------------------

    /**
    * Establishes the primary connection with the central Auction Server.
    * Sets up the communication streams and launches background threads 
    * for automated item generation, auction monitoring, and server listening.
    */
    private static void connectToAuctionServer() {

        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader console = new BufferedReader(new InputStreamReader(System.in))) {
                
                System.out.println("Successful connection to the Auction Server!\n");
                
                startObjectGenerator(out); // Automatically generates items to be sold
                startAuctionParticipant(out); // Periodically polls for new auctions
                startServerListener(in, out); // Asynchronously processes server notifications  from AuctionServer

                //Main menu Loop
                runInteractiveMenu(console, out, socket);

            } catch (IOException e) {
                System.err.println("Unable to connect to the Server: " + e.getMessage());
            }
    }

    // --------------------
    // HELPER METHODS
    //---------------------

    /**
     * Starts the item generation chain in the background.
     */
    private static void startObjectGenerator(PrintWriter out) {
        scheduleNextItem(out); // Starts the first task
    }

    /**
     * Calculates the wait time and schedules (without blocking) the creation of the next item.
     */
    private static void scheduleNextItem(PrintWriter out) {
        Random random = new Random();

        // If the user is not logged in, schedule a quick check in 1 second
        if (currentTokenId == null) {
            scheduler.schedule(() -> scheduleNextItem(out), 1, TimeUnit.SECONDS);
            return;
        }

        // If logged in, calculate a random wait time (1 to 120 seconds)
        int waitTimeSeconds = random.nextInt(120) + 1;

        // Tell the scheduler: "In 'waitTimeSeconds', execute this code block"
        scheduler.schedule(() -> {
            try {
                // Double check: Did the user LOGOUT while we were waiting for the timer?
                if (currentTokenId != null) {
                    
                    String objectId = String.format("Obj%d_%d", peerListenPort, objectCounter++);
                    File newObjectFile = new File(currentSharedDir, objectId + ".txt");
                    
                    try (PrintWriter writer = new PrintWriter(new FileWriter(newObjectFile))) {
                        int startBid = 10 + random.nextInt(90); 
                        int duration = 60; 
                        
                        String metadata = "[peer_name: " + currentUsername + " peer_id: " + currentTokenId + " object_id: " + objectId + "; description: \"A great item\"; start_bid: " + startBid + "; auction_duration: " + duration + "]";
                        writer.println(metadata);
                        System.out.println("\n[Generator] The file was generated: " + newObjectFile.getName() + " in shared_directory!");
                        
                        out.println("REQUEST_AUCTION " + currentTokenId + " " + peerListenPort + " " + metadata);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error in item production: " + e.getMessage());
            } finally {
                // The most crucial step: Once the creation is done (or skipped), 
                // it calls itself again to schedule the *next* item!
                scheduleNextItem(out);
            }
        }, waitTimeSeconds, TimeUnit.SECONDS);
    }


    /**
    * Background thread that polls the Auction Server every minute to check for active auctions.
    * This automates the peer's interest check and participation in the bidding process
    */
    private static void startAuctionParticipant(PrintWriter out) {
        Thread participantThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);

                    // If logged in, automatically asking the server for current auction info
                    if (currentTokenId != null) {
                        out.println("GET_CURRENT_AUCTION");
                    }
                } catch (InterruptedException e) {
                    System.err.println("Participant Thread interrupted.");
                }
            }
        });
        participantThread.setDaemon(true); // to prevent the thread from hanging on application exit
        participantThread.start();
    }


    /**
    * Background thread to listen for messages from the Server.
    * This ensures the Peer can receive asynchronous updates (like new bids or 
    * auction results) without blocking the main user interface menu.
    */
    public static void startServerListener(BufferedReader in, PrintWriter out) {

        // Listener thread to process incoming notifications from the Auction Server
        Thread listenerThread = new Thread(() -> {
            try {
                String serverResponse;
                while ((serverResponse = in.readLine()) != null) {

                    processServerResponse(serverResponse, out);
                }
            } catch (IOException e) {
                        System.out.println("The connection to the Server was closed.");
            }
        });

        listenerThread.start();
    }


    /**
    * Acts as the central message dispatcher for all incoming server notifications.
    * It parses the server's protocol and triggers the appropriate logic:
    * - Session management (token saving)
    * - Automated bidding triggers
    * - P2P transaction initiation upon winning
    */
    private static void processServerResponse(String serverResponse, PrintWriter out) {
        // 1. SUCCESS Messages (Green text)
        if (serverResponse.startsWith("SUCCESS:")) {
            System.out.println(GREEN + serverResponse + RESET);

            // Save token logic
            if (serverResponse.length() > 15 && serverResponse.substring(9).contains("-")) {
                currentTokenId = serverResponse.substring(9);
                System.out.println(CYAN + "-> Session established. Token saved: " + currentTokenId + RESET);
                currentSharedDir = new File("Peer_" + currentUsername);
                if (!currentSharedDir.exists()) {
                    currentSharedDir.mkdir();
                    System.out.println(CYAN + "-> Created unique folder: " + currentSharedDir.getName() + RESET);
                }
                resubmitExistingFiles(out);
            }
            
            // CRITICAL FOR GBN: If this is the winning notification, trigger the P2P transaction immediately!
            if (serverResponse.contains("AUCTION_WON")) {
                handleWinningTransaction(serverResponse, out);
            }
        }
        // 2. FAIL / ERROR Messages (Red text)
        else if (serverResponse.startsWith("FAIL:") || serverResponse.startsWith("ERROR:")) {
            System.out.println(RED + serverResponse + RESET);
        }
        // 3. CURRENT_AUCTION Info (Yellow Banner)
        else if (serverResponse.startsWith("CURRENT_AUCTION")) {
            System.out.println("\n" + YELLOW + "==================================================");
            System.out.println(" ACTIVE AUCTION DETAILS");
            System.out.println(" " + serverResponse);
            System.out.println("==================================================" + RESET);
            
            try { Thread.sleep(100); } catch (InterruptedException e) {}
            out.println("GET_AUCTION_DETAILS");
        }
        // 4. AUCTION_DETAILS (Cyan Info)
        else if (serverResponse.startsWith("AUCTION_DETAILS")) {
            System.out.println(CYAN + "[Bidding Info] " + serverResponse + RESET);
            handleAutoBidding(serverResponse, out);
        }
        // 5. UPDATE_BID (Purple Alert)
        else if (serverResponse.startsWith("UPDATE_BID")) {
            System.out.println(PURPLE + "[BID UPDATE] " + serverResponse + RESET);
            if (currentTokenId != null) {
                try { Thread.sleep(1500); } catch (InterruptedException e) {} // Safe delay brake
                out.println("GET_AUCTION_DETAILS");
            }
        }
        // 6. Generic INFO (White text)
        else if (serverResponse.startsWith("INFO:")) {
            System.out.println(WHITE + serverResponse + RESET);
        }
        // Any other background server message
        else {
            System.out.println(WHITE + "[Server]: " + serverResponse + RESET);
        }
    }


    /**
    * Re-submits all local files to the Auction Server.
    * After a fresh login to ensure all items are in the queue.
    */
    private static void resubmitExistingFiles(PrintWriter out) {

        if (currentSharedDir == null || !currentSharedDir.exists()) return;

        File[] files = currentSharedDir.listFiles();

        if (files != null) {
            
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".txt")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {

                        String metadata = reader.readLine(); 
                        if (metadata != null) {
                            out.println("REQUEST_AUCTION " + currentTokenId + " " + peerListenPort + " " + metadata);
                        }

                    } catch (IOException ex) {
                        System.err.println("Error reading file: " + file.getName());
                    }
                }
            }
        }
    }

    //Logic for the automated bidding agent.
    
    private static void handleAutoBidding(String serverResponse, PrintWriter out) {
        String[] parts = serverResponse.split(" ");

        if (parts.length >= 13) {
            String sellerToken = parts[2];
            String sellerName = parts[4];
            int currentBid = Integer.parseInt(parts[6]);
            double sellerRep = Double.parseDouble(parts[8].replace(",", "."));
            int remainingTime = Integer.parseInt(parts[10]); // Parse remaining time from server
            int sessionId = Integer.parseInt(parts[12]); // Extracting Session ID

            //Use token to make sure we aren't bidding on our own item and based on 60% probability
            if (currentTokenId != null && !currentTokenId.equals(sellerToken)) {

                if (Math.random() < 0.60) {
                    int newBid;

                // Assuming standard auction duration is 60 seconds (10% is 6 seconds or less)
                if (remainingTime <= 6) {
                    // Sniper mode: Auction is ending, bidding up to 20% higher
                    newBid = (int) (currentBid * (1 + Math.random() / 5.0)); // RAND/5 gives up to 20%
                    System.out.println("-> [Auto-Bidder] End of auction sniper mode active (<= 10% time left)! ");
                } else {
                    // Standard mode: Bidding up to 10% higher
                    newBid = (int) (currentBid * (1 + Math.random() / 10.0)); // RAND/10 gives up to 10%
                }

                if (newBid <= currentBid) newBid = currentBid + 1;

                out.println("PLACE_BID " + sessionId + " " + currentTokenId + " " + newBid);
                System.out.println("-> [Auto-Bidder] I am interested! Placing bid: " + newBid + " on Session: " + sessionId + " from Seller: " + sellerName);

                } else {
                    System.out.println("-> [Auto-Bidder] Decided not to bid on this item (60% chance failed).");
                }
            }
        }
    }


    /**
    * Handles the direct P2P connection when this peer wins an auction.
    */
    private static void handleWinningTransaction(String serverResponse, PrintWriter out) {

        System.out.println("DEBUG: Entering handleWinningTransaction!");

        String[] parts = serverResponse.split(" ");
        if (parts.length < 6) return;
            String wonObjectId = parts[2];
            String sellerIp = parts[4];
            int sellerPort = Integer.parseInt(parts[5]);

            // 30% possibility of cancelling
            if (Math.random() < 0.30) {
                System.out.println("-> [Buyer] Decided to CANCEL/REJECT the transaction (30% chance triggered).");
                out.println("BIDDER_CANCEL " + currentTokenId); // updating server

                return;
            }

            System.out.println("-> [P2P Transaction] Connecting to Seller at " + sellerIp + ":" + sellerPort + " to claim " + wonObjectId + " ");

            try {
                // 1. TCP Connection to tell the seller WHICH object we want 
                Socket tcpSocket = new Socket(sellerIp, sellerPort);
                PrintWriter tcpOut = new PrintWriter(tcpSocket.getOutputStream(), true);
                tcpOut.println(wonObjectId + "|" + (peerListenPort + 1));

                // 2. UDP Setup for Go-Back-N
                DatagramSocket udpSocket = new DatagramSocket(peerListenPort + 1);
                System.out.println("[DEBUG-Buyer] Waiting for UDP packets on port: " + (peerListenPort + 1));

                udpSocket.setSoTimeout(5000); // 5 sec safety timeout
                
                int expectedSeqNum = 0;
                ByteArrayOutputStream fileBuffer = new ByteArrayOutputStream();
                Random rand = new Random();

                boolean transferComplete = false;

                System.out.println("[GBN-Receiver] Ready to receive " + wonObjectId + " via UDP...");

                while (!transferComplete) {
                    try {
                        byte[] receiveBuffer = new byte[1024];
                        DatagramPacket incomingPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                        udpSocket.receive(incomingPacket);

                        // Simulation 20% loss packet
                        if (rand.nextDouble() < 0.20) {
                            System.out.println("[GBN-Receiver] Packet dropped intentionally (20% chance).");
                            continue; 
                        }

                        // Split sequence number and data
                        String message = new String(incomingPacket.getData(), 0, incomingPacket.getLength());

                        if (message.trim().equals("EOF")) {
                            System.out.println("[GBN-Receiver] EOF received. Transfer finished successfully!");
                            transferComplete = true;
                            break;
                        }

                        String[] msgParts = message.split("\\|", 2);
                        int seqNum = Integer.parseInt(msgParts[0]);
                        String data = msgParts[1];

                        // Checking if packet was the one expected (In-order delivery) 
                        if (seqNum == expectedSeqNum) {
                            System.out.println("[GBN-Receiver] Received expected packet: " + seqNum);
                            fileBuffer.write(data.getBytes());
                            expectedSeqNum++;

                            // Logic for ending: If the packet is smaller than 64 bytes, it's the last one 
                            if (data.length() < 64) transferComplete = true;
                        }


                        // Simulation 80% ACK sending possibility 
                        if (rand.nextDouble() < 0.80 && expectedSeqNum > 0) {
                            String ackMsg = String.valueOf(expectedSeqNum - 1);
                            byte[] ackData = ackMsg.getBytes();
                            DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, incomingPacket.getAddress(), incomingPacket.getPort());

                            udpSocket.send(ackPacket);
                            System.out.println("[GBN-Receiver] Sent cumulative ACK for: " + (expectedSeqNum - 1));
                        } else {
                            System.out.println("[GBN-Receiver] ACK lost (20% chance).");
                        }

                        
                    } catch (SocketTimeoutException e) {
                        if (expectedSeqNum > 0) break; // Transfer likely finished
                    }
                }

                //Saving file
                saveReceivedFile(wonObjectId, fileBuffer.toByteArray(),out);
                udpSocket.close();
                tcpSocket.close();

            } catch (Exception e) {
                e.printStackTrace(); 
            }
           
    }

    private static void saveReceivedFile(String objectId, byte[] data, PrintWriter out) {
        try {
            // Create the file in the buyer's unique directory 
            File newFile = new File(currentSharedDir, objectId + ".txt");
            
            try (FileOutputStream fos = new FileOutputStream(newFile)) {
                fos.write(data);
            }
            
            System.out.println("-> [P2P] Successfully saved " + objectId + ".txt in " + currentSharedDir.getName());

            // Notify Auction Server that I am the new owner 
            String metadata = new String(data).trim();
            out.println("REQUEST_AUCTION " + currentTokenId + " " + peerListenPort + " " + metadata);
            
        } catch (IOException e) {
            System.err.println("[P2P] Error saving file: " + e.getMessage());
        }

    }

    // MENU LOOP
    public static void runInteractiveMenu(BufferedReader console, PrintWriter out, Socket socket) throws IOException {

        // Interactive Menu (Register/Login/Logout)
            while (true) {
                if (currentTokenId == null) {
                // (When user has not yet logged in)
                System.out.println("\n=== DISTRIBUTED AUCTION SYSTEM ===");
                System.out.println("1. Register");
                System.out.println("2. Login");
                System.out.println("3. Exit");
                System.out.print("Choose (1-3): ");
                        
                String choice = console.readLine();
                if (choice == null) break;

                if (choice.equals("1")) {
                    System.out.print("Enter new username: ");
                    String uname = console.readLine();
                    System.out.print("Enter new password: ");
                    String pass = console.readLine();
                    out.println("REGISTER " + uname + " " + pass);
                            
                    try {
                        Thread.sleep(1000); // Brief pause to receive answer
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } 
                else if (choice.equals("2")) {
                    System.out.print("Enter username: ");
                    String uname = console.readLine();
                    currentUsername = uname;
                    System.out.print("Enter password: ");
                    String pass = console.readLine();
                    out.println("LOGIN " + uname + " " + pass);
                            
                    try {
                        Thread.sleep(1000); // Brief pause to receive answer
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }   
                else if (choice.equals("3")) {
                    System.out.println("Exiting the system...");
                            
                    try {
                        socket.close(); 
                    } catch (IOException e) {
                        System.err.println("Error when closing the Server: " + e.getMessage());
                    }

                    System.exit(0);
                }

            } else {  // (When the user HAS logged in) 
                System.out.println("\nYou are connected! The system auctions and bids automatically.");
                System.out.println("Press 'L' and Enter if you want to log out.");
                        
                String choice = console.readLine();
                if (choice != null && choice.equalsIgnoreCase("L")) {
                    out.println("LOGOUT " + currentTokenId);
                    currentTokenId = null; // restart of token
                }
            }
        }
    }
}