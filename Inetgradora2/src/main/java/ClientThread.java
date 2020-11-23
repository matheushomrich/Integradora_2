public class ClientThread extends Thread {

    private int id;
    private PooledScheduler ps;
    private int nReqs;

    public ClientThread(int _id, PooledScheduler _ps, int _nr){
        id = _id;
        ps = _ps;
        nReqs = _nr;
    }

    public void run(){

        for (int i=1 ; i < nReqs; i++) {
            ps.schedule(new MessageContextPair(id, id*1000000+i));
        }




    }
}
