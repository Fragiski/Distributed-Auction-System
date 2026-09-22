/**
* Managed by the Auction Server, this task runs in a separate thread to handle 
* the lifecycle of each auction sequentially.
*/

public class AuctionManagerTask implements Runnable {

    private final AuctionSession session;

    public AuctionManagerTask(AuctionSession session) {
        this.session = session;
    }

    @Override
    public void run() {
        while (true) {
            try {
                // If there is no active auction in this session, check the queue
                if (!session.isAuctionActive) {
                    String chosenMetadata = null;

                synchronized (AuctionServer.auctionLock) {
                    if (!AuctionServer.auctionQueue.isEmpty()) {
                       //Check the top 2 items based on Seller Reputation Score
                        if (AuctionServer.auctionQueue.size() >= 2) {
                            String item1 = AuctionServer.auctionQueue.get(0);
                            String item2 = AuctionServer.auctionQueue.get(1);

                            String seller1 = extractValue(item1, "seller_token");
                            String seller2 = extractValue(item2, "seller_token");

                            double rep1 = 1.0;
                            double rep2 = 1.0;

                            ActiveUser u1 = AuctionServer.activeUsers.get(seller1);
                            if (u1 != null && AuctionServer.registeredUsers.containsKey(u1.getUsername())) {
                                rep1 = AuctionServer.registeredUsers.get(u1.getUsername()).getReputationScore();
                            }

                            ActiveUser u2 = AuctionServer.activeUsers.get(seller2);
                            if (u2 != null && AuctionServer.registeredUsers.containsKey(u2.getUsername())) {
                                rep2 = AuctionServer.registeredUsers.get(u2.getUsername()).getReputationScore();
                            }

                            // If the second seller has a higher reputation, pick their item instead
                                if (rep2 > rep1) {
                                    System.out.println(AuctionServer.YELLOW + "[REPUTATION CHOICE] Session " + session.sessionId + 
                                            ": Selected 2nd item over 1st due to higher Reputation (" + rep2 + " > " + rep1 + ")" + AuctionServer.RESET);
                                    chosenMetadata = AuctionServer.auctionQueue.remove(1);
                                } else {
                                    // Default to First-Come-First-Served (FCFS)
                                    chosenMetadata = AuctionServer.auctionQueue.remove(0);
                                }                           
                            } else {
                                // If only one item exists in the queue, retrieve it normally
                                chosenMetadata = AuctionServer.auctionQueue.remove(0);
                            }
                        }
                    }
                
                    // If an item was selected, initialize the auction for this specific session
                    if (chosenMetadata != null) {
                        synchronized (session.sessionLock) {
                            session.currentSellerToken = extractValue(chosenMetadata, "seller_token");
                            session.currentObjectId = extractValue(chosenMetadata, "object_id");
                            session.currentDescription = extractValue(chosenMetadata, "description");
                            session.currentHighestBid = Long.parseLong(extractValue(chosenMetadata, "start_bid"));
                            session.currentHighestBidderToken = null;
                            session.currentRemainingTime = Integer.parseInt(extractValue(chosenMetadata, "auction_duration"));
                            session.isAuctionActive = true;
                            session.currentAuctionBids.clear();
                        }

                        // Broadcast auction start, specifying the concurrent Session ID
                        AuctionServer.broadcast("AUCTION_STARTED Session: " + session.sessionId + 
                                " Item: " + session.currentObjectId + 
                                " | Starting Bid: " + session.currentHighestBid + 
                                " | Duration: " + session.currentRemainingTime);
                    }
                }

                // If the auction is active, handle countdown and periodic seller check
                if (session.isAuctionActive) {
                    boolean sellerDisconnected = false;
                    int duration = session.currentRemainingTime;

                    for (int i = 0; i < duration; i++) {
                        synchronized (session.sessionLock) {
                            session.currentRemainingTime = duration - i;
                            
                            // Broadcast updates every 10 seconds, or continuously in the final 5 seconds
                           if (session.currentRemainingTime % 10 == 0 || session.currentRemainingTime <= 5) {
                            ActiveUser sellerActive = AuctionServer.activeUsers.get(session.currentSellerToken);
                            String sellerName = (sellerActive != null) ? sellerActive.getUsername() : "Unknown";
                            User sellerUser = (sellerActive != null) ? AuctionServer.registeredUsers.get(sellerActive.getUsername()) : null;
                            double sellerRep = (sellerUser != null) ? sellerUser.getReputationScore() : 1.0;

                            AuctionServer.broadcast("AUCTION_DETAILS SellerToken: " + session.currentSellerToken + 
                                    " SellerName: " + sellerName + 
                                    " HighestBid: " + session.currentHighestBid + 
                                    " SellerScore: " + String.format(java.util.Locale.US, "%.2f", sellerRep) + 
                                    " RemainingTime: " + session.currentRemainingTime + 
                                    " SessionId: " + session.sessionId);
                            }
                        }

                        Thread.sleep(1000);

                        // Periodic Check: Verify if the seller disconnected mid-auction
                        if (!AuctionServer.activeUsers.containsKey(session.currentSellerToken)) {
                            sellerDisconnected = true;
                            break;
                        }
                    }

                    // Finalizing the auction after expiration or cancellation
                    synchronized (session.sessionLock) {
                        session.isAuctionActive = false;

                        if (sellerDisconnected) {
                            System.out.println("AuctionServer.RED + [AUCTION CANCELLED] Session " + session.sessionId + 
                                    ": Seller disconnected. Item " + session.currentObjectId + " is cancelled." + AuctionServer.RESET);
                            
                            String cancelMsg = "AUCTION_CANCELLED Item: " + session.currentObjectId + " (Seller dropped)";
                            for (ActiveUser user : AuctionServer.activeUsers.values()) {
                                if (user.getOut() != null) {
                                    user.getOut().println("INFO: " + cancelMsg);
                                }
                            }
                        }
                        // Normal flow: Auction finished successfully
                        else if (session.currentHighestBidderToken != null) {
                            ActiveUser winnerUser = AuctionServer.activeUsers.get(session.currentHighestBidderToken);
                            String winnerName = (winnerUser != null) ? winnerUser.getUsername() : "Unknown";

                            AuctionServer.broadcast("AUCTION_ENDED Session: " + session.sessionId + 
                                    " Item " + session.currentObjectId + " sold to: " + winnerName + 
                                    " for " + session.currentHighestBid);
                            
                            AuctionServer.completedAuctionWinners.put(session.currentObjectId, session.currentHighestBidderToken);
                            
                            ActiveUser sellerActive = AuctionServer.activeUsers.get(session.currentSellerToken);
                            if (sellerActive != null && winnerUser != null) {

                                User sellerUser = AuctionServer.registeredUsers.get(sellerActive.getUsername());
                                if (sellerUser != null) {
                                    sellerUser.incrementSellerCount();
                                }

                                String buyerMsg = "AUCTION_WON " + session.currentObjectId + " " + session.currentHighestBid + " " + sellerActive.getIpAddress() + " " + sellerActive.getPort();
                                String sellerMsg = "AUCTION_SOLD " + session.currentObjectId + " " + session.currentHighestBid + " to buyer: " + winnerName;
                                
                                winnerUser.getOut().println("SUCCESS: " + buyerMsg);
                                sellerActive.getOut().println("SUCCESS: " + sellerMsg);
                            }
                        } else {
                            AuctionServer.broadcast("AUCTION_ENDED Session: " + session.sessionId + 
                                    " Item " + session.currentObjectId + " closed with no bids.");
                        }
                    }
                    
                    // Brief pause before processing the next item in the queue
                    Thread.sleep(2000); 
                } else {
                    // Wait if the queue is empty until a peer submits a new item
                    Thread.sleep(2000); 
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Exception in AuctionManagerTask Session " + session.sessionId + ": " + e.getMessage());
            }
        }
    }

   /**
    * Helper method to parse specific values from the formatted metadata string.
    */
    private String extractValue(String metadata, String key) {
        // Support both delimiter types flexibly ("key:" and "key=")
        String[] delims = { key + ":", key + "=" };
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