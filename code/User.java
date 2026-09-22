public class User {
    private String username;
    private String password;
    private int numAuctionsSeller; // Increases +1 when participating as a seller
    private int numAuctionsBidder; // Increases by +1 when winning an auction as a buyer
    private double reputationScore = 1.0;

    public User(String username, String password) {
        this.username = username;
        this.password = password;
        this.numAuctionsBidder = 0;
        this.numAuctionsSeller = 0;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getNumAuctionsSeller() {
        return numAuctionsSeller;
    }

    public void incrementSellerCount() {
        this.numAuctionsSeller++;
    }

    public int getNumAuctionsBidder() {
        return numAuctionsBidder;
    }

    public void incrementBidderCount() {
        this.numAuctionsBidder++;
    }

    public double getReputationScore() {

        return reputationScore;
    }

    public void setReputationScore(double score) {

        this.reputationScore = score;
    }
    
}
