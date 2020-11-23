public class MessageContextPair {
    public int clId;
    public int operation;
    //public byte[] resp;

    public MessageContextPair(int clId, int operation) {
        this.clId = clId;
        this.operation = operation;
    }

}
