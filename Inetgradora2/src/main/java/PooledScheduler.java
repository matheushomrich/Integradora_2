//package parallelism.pooled;
/*

import com.codahale.metrics.MetricRegistry;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;
import parallelism.late.ConflictDefinition;
import parallelism.scheduler.Scheduler;
*/


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;
import java.util.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.function.Consumer;


public class PooledScheduler {

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

    public PooledScheduler(int nThreads,
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
            System.out.println(e);
        }
    }

    private void doSchedule(MessageContextPair request) {
        Task newTask = new Task(request);
        submit(newTask, addTask(newTask));
    }
    private synchronized List<CompletableFuture<Void>> addTask(Task newTask) {
        List<CompletableFuture<Void>> dependencies = new LinkedList<>();
        ListIterator<Task> iterator = scheduled.listIterator();

        while (iterator.hasNext()) {
            Task task = iterator.next();
            if (task.future.isDone()) {
                iterator.remove();//SC
                continue;
            }
            if (conflict.isDependent(task.request, newTask.request)) {
                dependencies.add(task.future);
            }
        }
        scheduled.add(newTask);  //SC
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
        //System.out.println(task.toString());
        space.release();
        task.future.complete(null);

    }
    private static void executeRequest(MessageContextPair messageContextPair){
        String delta = "    ";
        for (int i=1; i<messageContextPair.clId; i++){ // gera um delta para cada id de cliente
            delta = delta + "                ";
        }
        System.out.println(delta + messageContextPair.operation);
    }

    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static void main(String [] args) throws IOException, RunnerException {
        // parametros
        int nt = 6;  // numero de threads no pool
        int nc = 10;  // numero de threads clientes
        int nr = 100; // numero de requisicoes que cada cliente faz
        ConflictDefinition cf = new ConflictDefinition();

        // lado servidor
        PooledScheduler sch = new PooledScheduler(nt, cf);
        sch.setExecutor(PooledScheduler::executeRequest);

        // lado cliente

        for (int i=1; i<=nc; i++){
            new ClientThread(i, sch, nr).start();
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

//Teste:Fixar um numero de threads no pool(max da maquina(umas 6))
//executa por determinado tempo e verifica a quantidade de requisicoes tratadas 
//apos a thread main dormir para cada configuracao

//mesmo nao tendo uma opcao de funcionalidade de lista concorrente de java, existe na literatura implementacoes que abordam
//solucoes para isso