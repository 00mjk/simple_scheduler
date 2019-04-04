import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.LocalDateTime.now;

public class Scheduler {

    private AtomicLong id = new AtomicLong(0);

    private ConcurrentSkipListSet<Job> queue = new ConcurrentSkipListSet<>(new JobComparator());

    private Object sLock = new Object();

    public Scheduler() {
        LinkedBlockingDeque<Job> backlog = new LinkedBlockingDeque<>();

        Thread scheduler = new Thread(new ScheduleBuilder(queue, backlog, sLock), "scheduler");
        scheduler.start();
        //если нужен четкий порядок, то делаем один воркер
        for (int i = 1; i < 2; i++) {
            (new Thread(new Worker(backlog), "worker-" + i)).start();
        }
    }

    public long schedule(LocalDateTime time, Callable callable) {
        Job job = new Job(id.incrementAndGet(), time, callable);
        queue.add(job);
        //
        synchronized (sLock) {
            sLock.notify();
        }
        return job.id;
    }


    private static class ScheduleBuilder implements Runnable {

        private ConcurrentSkipListSet<Job> queue;

        private LinkedBlockingDeque<Job> backlog;

        private Object lock;

        public ScheduleBuilder(ConcurrentSkipListSet<Job> queue, LinkedBlockingDeque<Job> backlog, Object lock) {
            this.queue = queue;
            this.backlog = backlog;
            this.lock = lock;
        }

        @Override
        public void run() {
            for (; ; ) {
                long toWait = 0;
                while (!queue.isEmpty() && (toWait = queue.first().time - toMillis(now())) <= 0) {
                    backlog.add(queue.pollFirst());
                }
                try {
                    //очередь пуста
                    synchronized (lock) {
                        lock.wait(toWait > 0 ? toWait : 0);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }


    private static class Job {

        private long id;

        private long time;

        private Callable task;

        public Job(long id, LocalDateTime time, Callable callable) {
            this.id = id;
            this.time = toMillis(time);
            this.task = callable;
        }
    }

    private static class JobComparator implements Comparator<Job> {
        @Override
        public int compare(Job j1, Job j2) {
            if (j1.time < j2.time) return -1;
            if (j1.time == j2.time) return Long.compare(j1.id, j2.id);
            return 1;
        }
    }

    private class Worker implements Runnable {

        LinkedBlockingDeque<Job> backlog;

        public Worker(LinkedBlockingDeque<Job> backlog) {
            this.backlog = backlog;
        }

        @Override
        public void run() {
            for (; ; ) {
                Job task;
                try {
                    //ждем пока в очереди что-то не появится
                    task = backlog.takeFirst();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    task.task.call();
                } catch (Exception e) {
                    // TODO something
                }
            }

        }
    }

    private static long toMillis(LocalDateTime time) {
        return time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    }


}
