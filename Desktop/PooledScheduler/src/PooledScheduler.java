//package parallelism.pooled;
/*

import com.codahale.metrics.MetricRegistry;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;
import parallelism.late.ConflictDefinition;
import parallelism.scheduler.Scheduler;
*/

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.concurrent.ForkJoinPool;

final class PooledScheduler {

    private static final int MAX_SIZE = 150;

    private static final class Task {
        private final MessageContextPair request;
        private final CompletableFuture<Void> future;

        Task(MessageContextPair request) {
            this.request = request;
            this.future = new CompletableFuture<>();
        }
    }

    private final int nThreads;
    private final ConflictDefinition conflict;
    private final ExecutorService pool;
    private final Semaphore space;
    private final List<Task> scheduled;


    private Consumer<MessageContextPair> executor;

    PooledScheduler(int nThreads,
                    ConflictDefinition conflict) {
        this.nThreads = nThreads;
        this.conflict = conflict;
        this.space = new Semaphore(MAX_SIZE);
        this.scheduled = new LinkedList<>();
        this.pool = Executors.newWorkStealingPool(nThreads);
    }

    // Breaks cyclic dependency with PooledServiceReplica
    void setExecutor(Consumer<MessageContextPair> executor) {
        this.executor = executor;
    }

    public int getNumWorkers() {
        return nThreads;
    }

    public void schedule(MessageContextPair request) {
        try {
            space.acquire();
            doSchedule(request);
        } catch (InterruptedException e) {
            // Ignored.
        }
    }

    private void doSchedule(MessageContextPair request) {
        Task newTask = new Task(request);
        submit(newTask, addTask(newTask));
    }

    private List<CompletableFuture<Void>> addTask(Task newTask) {
        List<CompletableFuture<Void>> dependencies = new LinkedList<>();
        ListIterator<Task> iterator = scheduled.listIterator();

        while (iterator.hasNext()) {
            Task task = iterator.next();
            if (task.future.isDone()) {
                iterator.remove();
                continue;
            }
            if (conflict.isDependent(task.request, newTask.request)) {
                dependencies.add(task.future);
            }
        }
        scheduled.add(newTask);
        return dependencies;
    }

    private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
        if (dependencies.isEmpty()) {
            pool.execute(() -> execute(newTask));
        } else {
            after(dependencies).thenRun(() -> {
                execute(newTask);
            });
        }
    }

    private static CompletableFuture<Void> after(List<CompletableFuture<Void>> fs) {
        if (fs.size() == 1) return fs.get(0); // fast path
        return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
    }

    private void execute(Task task) {
        executor.accept(task.request);
        System.out.println(task.toString());
        space.release();
        task.future.complete(null);
    }

    private static void executeRequest(MessageContextPair messageContextPair){
        System.out.println("" + messageContextPair.operation);
    }

    private static void taskScheduleParallelism(int nThreads, MessageContextPair msg, PooledScheduler sch){

        ForkJoinPool forkJoinPool = null;
        try {
            forkJoinPool = new ForkJoinPool(nThreads);
            forkJoinPool.submit(() -> sch.schedule(msg)).get();
            
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (forkJoinPool != null) {
                forkJoinPool.shutdown(); 
            }
        }

    }

    /*private static void taskCreationParallelism(int nThreads, MessageContextPair msg, PooledScheduler sch){

        ForkJoinPool forkJoinPool = null;
        try {
            forkJoinPool = new ForkJoinPool(nThreads);
            forkJoinPool.submit(() -> sch.schedule(msg)).get();

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (forkJoinPool != null) {
                forkJoinPool.shutdown();
            }
        }

    }*/

    public static void main(String [] args){
        int i = 0;
        ConflictDefinition cf = new ConflictDefinition();
        PooledScheduler sch = new PooledScheduler(2, cf);
        sch.setExecutor(PooledScheduler::executeRequest);
        while(true) {
            taskScheduleParallelism(10, new MessageContextPair(i++), sch);
        }

    }
}