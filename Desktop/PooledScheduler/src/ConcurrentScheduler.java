
import java.util.concurrent.*;

public class ConcurrentScheduler {

    private final int nThreads;
    private final ForkJoinPool forkJoinPool;

    public ConcurrentScheduler(int nThreads, ForkJoinPool forkJoinPool) {
        this.nThreads = nThreads;
        this.forkJoinPool = forkJoinPool;
    }

    public int getnThreads() {
        return nThreads;
    }

    public ForkJoinPool getForkJoinPool() {
        return forkJoinPool;
    }

    public void concurrentScheduling(int nThreads, ForkJoinPool forkJoinPool, PooledScheduler scheduler, MessageContextPair msg){
        try {
            forkJoinPool = new ForkJoinPool(nThreads);
            forkJoinPool.submit(() -> scheduler.schedule(msg)).get();

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (forkJoinPool != null) {
                forkJoinPool.shutdown();
            }
        }
    }
}
