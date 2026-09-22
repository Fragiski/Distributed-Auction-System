import java.util.ArrayList;

public class AuctionSession {
    public final int sessionId;
    public final Object sessionLock = new Object();
    
    public volatile boolean isAuctionActive = false;
    public String currentSellerToken = null;
    public String currentObjectId = null;
    public String currentDescription = null;
    public long currentHighestBid = 0; // Configured as long to prevent overflows
    public String currentHighestBidderToken = null;
    public int currentRemainingTime = 0;
    
    public final ArrayList<AuctionServer.Bid> currentAuctionBids = new ArrayList<>();

    public AuctionSession(int sessionId) {
        this.sessionId = sessionId;
    }
}