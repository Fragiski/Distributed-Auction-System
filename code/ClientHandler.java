import java.io.*;
import java.net.Socket;
import java.util.UUID;

/**
 * Handles communication between the Auction Server and a specific Peer.
 * Implements Runnable to allow multiple concurrent client connections.
 */

public class ClientHandler implements Runnable {

    private Socket clientSocket;
    private String currentTokenId = null; // Stores the session token after a successful login

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String inputLine;
            // Continuously listening for incoming commands from the Peer
            while ((inputLine = in.readLine()) != null) {

                String[] parts = inputLine.split(" ");
                String command = parts[0].toUpperCase();

                String commandColor = "\u001B[37m"; 
                if (command.equals("PLACE_BID")) {
                    commandColor = "\u001B[36m"; 
                } else if (command.equals("REQUEST_AUCTION")) {
                    commandColor = "\u001B[33m";
                } else if (command.equals("LOGIN") || command.equals("REGISTER")) {
                    commandColor = "\u001B[32m";
                } else if (command.equals("LOGOUT") || command.equals("BIDDER_CANCEL")) {
                    commandColor = "\u001B[35m"; 
                }

                System.out.println(commandColor + "[Server Log] Message received: " + inputLine + "\u001B[0m");

                switch (command) {
                    // -----------------------------------------
                    // 1. AYTHENTICATION COMMANDS
                    // -----------------------------------------

                    case "REGISTER":
                        // Format: REGISTER <username> <password>
                        handleRegister(parts,out);
                        break;

                    case "LOGIN":
                        // Format: LOGIN <username> <password>
                        handleLogin(parts,out);
                        break;

                    case "LOGOUT":
                        // Format: LOGOUT <token_id>
                        handleLogout(parts,out);
                        break;

                    // --------------------------------------------
                    // 2. AUCTION INQUIRY COMMANDS (Info gathering)
                    // --------------------------------------------

                    case "GET_CURRENT_AUCTION":
                        // Returns the ID and description of the item currently being auctioned
                        handleGetCurrentAuction(out);
                        break;

                    case "GET_AUCTION_DETAILS":
                        // Returns the seller's token and the current highest bid
                        handleGetAuctionDetails(inputLine, out);
                        break;

                    // --------------------------------------------
                    // 2.AUCTION ACTION COMMANDS (Participating)
                    // --------------------------------------------

                    case "REQUEST_AUCTION":
                        // Format: REQUEST_AUCTION <token_id> <peer_listen_port> <metadata_string>
                        handleRequestAuction(parts, out);
                        break;

                    case "PLACE_BID":
                        // Format: PLACE_BID <token_id> <bid_amount>
                        handlePlaceBid(inputLine, out);
                        break;
                    case "BIDDER_CANCEL":
                        handleBidderCancel(parts,out);
                        break;

                    default:
                        out.println("ERROR: Unknown command.");
                        break;
                    }
                }

        } catch (IOException e) {
            System.err.println("Client communication error: " + e.getMessage());
        } finally {
            cleanup();
        }
    }


    // --------------------------------------------
    // 3. HELPER METHODS FOR COMMAND HANDLING
    // --------------------------------------------

    private void handleRegister(String[] parts, PrintWriter out) {

        if (parts.length == 3) {
            String username = parts[1];
            String password = parts[2];
                                
            // Check if the username is already taken
            if (AuctionServer.registeredUsers.containsKey(username)) {
                out.println("FAIL: Username already exists. Please try another one."); 
            } else {
                AuctionServer.registeredUsers.put(username, new User(username, password));
                out.println("SUCCESS: Account created successfully!"); 
            }
            
        } else {
            out.println("ERROR: Incorrect syntax. Usage: REGISTER <username> <password>");
        }

    }


    private void handleLogin(String[] parts, PrintWriter out) {

        if (parts.length == 3) {
            String username = parts[1];
            String password = parts[2];
                                
            User user = AuctionServer.registeredUsers.get(username);

            // Authentication of peer 
            if (user != null && user.getPassword().equals(password)) {
                // Generating a random token_id (through UUID)
                currentTokenId = UUID.randomUUID().toString();
                                    
                // Registering the user as active (IP,Port update in requestAuction)
                String ipAddress = clientSocket.getInetAddress().getHostAddress();
                AuctionServer.activeUsers.put(currentTokenId, new ActiveUser(username, ipAddress, 0, out));
                                    
                out.println("SUCCESS: " + currentTokenId); 
            } else {
                out.println("FAIL: Incorrect username or password.");
            }
        } else {
            out.println("ERROR: Incorrect syntax. Usage: LOGIN <username> <password>");
        }
    }



    private void handleLogout(String[] parts,PrintWriter out) {

        if (parts.length == 2) {
            String token = parts[1];
                                
            if (AuctionServer.activeUsers.containsKey(token)) { // Identification with token_id 
                AuctionServer.activeUsers.remove(token); // Removing session from active users
                currentTokenId = null;
                out.println("SUCCESS: You have successfully logged out."); 
            } else {
                out.println("FAIL: Invalid token.");
            }
        } else {
            out.println("ERROR: Incorrect syntax. Usage: LOGOUT <token_id>");
        }
    }


    private void handleGetCurrentAuction(PrintWriter out) {
        AuctionSession session = AuctionServer.activeSessions[0];
        synchronized (AuctionServer.auctionLock) {
            if (session.isAuctionActive) {
                out.println("CURRENT_AUCTION " +session.currentObjectId + " " + session.currentDescription);
            } else {
                out.println("INFO: No auction is currently active.");
            }
        }
    }


    private void handleGetAuctionDetails(String message, PrintWriter out) {
        String[] parts = message.split(" ");
        int sessionId = 0; // Default fallback
        
        // Dynamic detection of optional session request parameter
        if (parts.length >= 2) {
            try {
                sessionId = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                sessionId = 0;
            }
        }

        if (sessionId < 0 || sessionId >= AuctionServer.activeSessions.length) {
            out.println("FAIL: Invalid session ID.");
            return;
        }

        AuctionSession session = AuctionServer.activeSessions[sessionId];

        synchronized (session.sessionLock) {
            if (session.isAuctionActive) {
                ActiveUser sellerActive = AuctionServer.activeUsers.get(session.currentSellerToken);
                double sellerRep = 1.0;
                String sellerName = "Unknown";

                if (sellerActive != null) {
                    sellerName = sellerActive.getUsername();
                    User sellerUser = AuctionServer.registeredUsers.get(sellerActive.getUsername());
                    if (sellerUser != null) {
                        sellerRep = sellerUser.getReputationScore();
                    }
                }

                //Preserves the exact array indices expected by Peer.java auto-bidder parsing logic
                out.println("AUCTION_DETAILS SellerToken: " + session.currentSellerToken + 
                            " SellerName: " + sellerName + 
                            " HighestBid: " + session.currentHighestBid + 
                            " SellerScore: " + String.format(java.util.Locale.US, "%.2f", sellerRep) +
                            " RemainingTime: " + session.currentRemainingTime + 
                            " SessionId: " + sessionId);
            } else {
                out.println("INFO: No auction is active in session " + sessionId);
            }
        }
    }


   private void handleRequestAuction(String[] parts, PrintWriter out) {
        if (parts.length >= 4) {
            String token = parts[1];
            int port = Integer.parseInt(parts[2]);
                            
            // Reconstructing the metadata string which may contain spaces
            StringBuilder metadataBuilder = new StringBuilder();
            for (int i = 3; i < parts.length; i++) {
                metadataBuilder.append(parts[i]).append(" ");
            }
            String metadata = metadataBuilder.toString().trim();

            // Verifying session token and update peer's P2P listening port
            ActiveUser activeUser = AuctionServer.activeUsers.get(token);
            if (activeUser != null) {
                activeUser.setPort(port); 

                // --- ASYNCHRONOUS REPUTATION VERIFICATION VIA WINNERS MAP ---
                synchronized (AuctionServer.auctionLock) {
                    String reqObjId = extractValue(metadata, "object_id");
                    
                    // Retrieve the officially recorded winner for this specific object ID
                    String officialWinnerToken = AuctionServer.completedAuctionWinners.get(reqObjId);
                    
                    // Reward the bidder only if their token matches the recorded winner for this item
                    if (officialWinnerToken != null && officialWinnerToken.equals(token)) {
                        User buyerUser = AuctionServer.registeredUsers.get(activeUser.getUsername());
                        
                        if (buyerUser != null) {
                            // Calculate and update buyer success reputation (S = 1.0)
                            double oldRep = buyerUser.getReputationScore();
                            double newRep = (0.75 * oldRep) + (0.25 * 1.0);
                            buyerUser.setReputationScore(newRep);
                            buyerUser.incrementBidderCount();
                            
                            System.out.format(java.util.Locale.US, "[REPUTATION] Transaction Success! %s Score: %.2f -> %.2f\n", buyerUser.getUsername(), oldRep, newRep);
                        }
                        
                        // Consume the token from the map to prevent duplicate rewards for the same item
                        AuctionServer.completedAuctionWinners.remove(reqObjId);
                    }
                }
                                
                // Storing the item in the FCFS auction queue
                String itemEntry = "seller_token=" + token + "; " + metadata;
                synchronized (AuctionServer.auctionLock) {
                    AuctionServer.auctionQueue.add(itemEntry);
                }
                                
                out.println("SUCCESS: Item added to auction queue.");
            } else {
                out.println("FAIL: Invalid token. Please login first.");
            }
        } else {
            out.println("ERROR: Incorrect syntax for REQUEST_AUCTION.");
        }
    }


    private void handlePlaceBid(String message, PrintWriter out) {
        String[] parts = message.split(" ");
        if (parts.length < 4) {
            out.println("FAIL: Invalid PLACE_BID format.");
            return;
        }

        try {
            int sessionId = Integer.parseInt(parts[1]);
            String bidderToken = parts[2];
            long bidAmount = Long.parseLong(parts[3]); // Long type to prevent overflows

            if (sessionId < 0 || sessionId >= AuctionServer.activeSessions.length) {
                out.println("FAIL: Invalid session ID.");
                return;
            }

            AuctionSession session = AuctionServer.activeSessions[sessionId];

            // Lock ONLY this specific auction session for thread-safety
            synchronized (session.sessionLock) {
                if (!session.isAuctionActive) {
                    out.println("FAIL: No active auction in session " + sessionId);
                    return;
                }

                if (bidderToken.equals(session.currentSellerToken)) {
                    out.println("FAIL: You cannot bid on your own item.");
                    return;
                }

                if (bidAmount <= session.currentHighestBid) {
                    out.println("FAIL: Bid amount is too low for session " + sessionId);
                    return;
                }
                session.currentHighestBid = bidAmount;
                session.currentHighestBidderToken = bidderToken;

                ActiveUser bidderActive = AuctionServer.activeUsers.get(bidderToken);
                String bidderName = (bidderActive != null) ? bidderActive.getUsername() : "Unknown";

                out.println("SUCCESS: Bid placed successfully. New highest bid: " + bidAmount);
                
                session.currentAuctionBids.add(0, new AuctionServer.Bid(bidderToken, (int) bidAmount));
                
                AuctionServer.broadcast("UPDATE_BID Session: " + session.sessionId + " Item: " + session.currentObjectId + " | New Highest Bid: " + bidAmount);
            }
        } catch (NumberFormatException e) {
            out.println("FAIL: Invalid numbers in bid request.");
        }
    }

    private void handleBidderCancel(String[] parts, PrintWriter out) {
        if (parts.length == 2) {
            String token = parts[1];
            
            ActiveUser buyerActive = AuctionServer.activeUsers.get(token);
            if (buyerActive != null) {
                User buyerUser = AuctionServer.registeredUsers.get(buyerActive.getUsername());
                if (buyerUser != null) {

                    // Auto-detect the active session where this user is leading
                    AuctionSession session = null;
                    for (AuctionSession s : AuctionServer.activeSessions) {
                        if (s.isAuctionActive && s.currentHighestBidderToken != null && s.currentHighestBidderToken.equals(token)) {
                            session = s;
                            break;
                        }
                    }

                    if (session == null) {
                        out.println("FAIL: You are not the highest bidder in any active session.");
                        return;
                    }

                    synchronized (session.sessionLock) {
                        // Remove all bids from this specific user to exclude them from backup selection
                        session.currentAuctionBids.removeIf(b -> b.bidderToken.equals(token)); 

                        // Penalize the cancelling bidder immediately (S = 0.0)
                        double oldRep = buyerUser.getReputationScore();
                        double newRep = (0.75 * oldRep) + (0.25 * 0.0);
                        buyerUser.setReputationScore(newRep);
                        System.out.format(java.util.Locale.US, "[CANCEL] Bidder %s backed out! Score: %.2f -> %.2f\n", buyerActive.getUsername(), oldRep, newRep);
                        
                        out.println("INFO: Cancellation processed. Reputation decreased.");

                        // Looking for the backup bidder sequentially
                        boolean backupFound = false;
                        for (AuctionServer.Bid executionBid : session.currentAuctionBids) {
                            if (executionBid.bidderToken.equals(token)) {
                                continue; 
                            }
                            
                            // Checking if the next highest bidder is still online and active
                            ActiveUser backupActive = AuctionServer.activeUsers.get(executionBid.bidderToken);
                            if (backupActive != null && AuctionServer.isSellerActive(executionBid.bidderToken)) {
                                
                                System.out.println("[BACKUP WINNER] Item transferred to next highest bidder: " + backupActive.getUsername() + " for " + executionBid.amount);
                                
                                ActiveUser sellerActive = AuctionServer.activeUsers.get(session.currentSellerToken);
                                if (sellerActive != null) {
                                    
                                    // Update global auction state to reflect the new winner
                                    session.currentHighestBid = executionBid.amount;
                                    session.currentHighestBidderToken = executionBid.bidderToken;
                                    
                                    // --- UPDATE WINNERS MAP WITH BACKUP TOKEN FOR ASYNCHRONOUS REWARD ---
                                    AuctionServer.completedAuctionWinners.put(session.currentObjectId, executionBid.bidderToken);

                                    // Triggering the P2P transaction protocols for both parties
                                    String buyerMsg = "AUCTION_WON " + session.currentObjectId + " " + executionBid.amount + " " + sellerActive.getIpAddress() + " " + sellerActive.getPort();
                                    String sellerMsg = "AUCTION_SOLD " + session.currentObjectId + " " + executionBid.amount + " to backup buyer: " + backupActive.getUsername();
                                    
                                    backupActive.getOut().println("SUCCESS: " + buyerMsg);
                                    sellerActive.getOut().println("SUCCESS: " + sellerMsg);
                                    
                                    backupFound = true;
                                    break; 
                                }
                            }
                        }
                        
                        if (!backupFound) {
                            System.out.println("[AUCTION FAILED] No other connected bidders found. Item was not sold.");
                            ActiveUser sellerActive = AuctionServer.activeUsers.get(session.currentSellerToken);
                            if (sellerActive != null) {
                                sellerActive.getOut().println("INFO: The buyer cancelled and no backup bidders were available.");
                            }
                        }
                    }
                }
            }
        } else {
            out.println("ERROR: Incorrect syntax for BIDDER_CANCEL.");
        }
    }

    private void cleanup() {
        // Cleanup: remove the user from the active list if the connection drops unexpectedly
            if (currentTokenId != null) {
                AuctionServer.activeUsers.remove(currentTokenId);
            }
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    /**
    * Helper method to parse specific values from the formatted metadata string.
    */
    private String extractValue(String metadata, String key) {
        String[] delims = { key + ":", key + "="};
        for (String delim : delims) {
            int start = metadata.indexOf(delim);
            if (start != -1) {
                start += delim.length();
                int end = metadata.length();
                
                // The field terminates at a semicolon, a closing bracket, or another space-separated key
                int semiColon = metadata.indexOf(";", start);
                int closeBracket = metadata.indexOf("]", start);
                
                if (semiColon != -1 && semiColon < end) end = semiColon;
                if (closeBracket != -1 && closeBracket < end) end = closeBracket;
                
                String val = metadata.substring(start, end).trim();
                
                // Strip surrounding quotes if parsing the description field
                if (val.startsWith("\"") && val.endsWith("\"")) {
                    val = val.substring(1, val.length() - 1);
                }
                return val;
            }
        }
        return "";
    }

}
