import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.LocalDateTime.now;

public class Scheduler {

    private AtomicLong id = new AtomicLong(0);

    private ConcurrentSkipListSet<Job> queue = new ConcurrentSkipListSet<>(new JobComparator());

    public Scheduler() {
        ConcurrentLinkedQueue<Job> backlog = new ConcurrentLinkedQueue<>();

        Thread scheduler = new Thread(new ScheduleBuilder(queue, backlog), "scheduler");
        scheduler.start();
        //если нужен четкий порядок, то делаем один воркер
        for (int i = 1; i < 4; i++) {
            (new Thread(new Worker(backlog), "worker-" + i)).start();
        }
    }

    public long schedule(LocalDateTime time, Callable callable) {
        Job job = new Job(id.incrementAndGet(), time, callable);
        queue.add(job);
        return job.id;
    }


    private static class ScheduleBuilder implements Runnable {

        private ConcurrentSkipListSet<Job> queue;

        private ConcurrentLinkedQueue<Job> backlog;

        public ScheduleBuilder(ConcurrentSkipListSet<Job> queue, ConcurrentLinkedQueue<Job> backlog) {
            this.queue = queue;
            this.backlog = backlog;
        }

        @Override
        public void run() {
            for (;;) {
                while(!queue.isEmpty() && queue.first().time < toMillis(now())) {
                    backlog.add(queue.pollFirst());
                }
                try {
                    Thread.sleep(100);
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
            if (j1.time == j2.time) return j1.id < j2.id ? -1 : 1;
            return 1;
        }
    }

    private class Worker implements Runnable {

        ConcurrentLinkedQueue<Job> backlog;

        public Worker(ConcurrentLinkedQueue<Job> backlog) {
            this.backlog = backlog;
        }

        @Override
        public void run() {
            for(;;) {
                Job task = backlog.poll();
                if (task == null) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    try {
                        task.task.call();
                    } catch (Exception e) {
                        // TODO something
                    }
                }

            }

        }
    }

    private static long toMillis(LocalDateTime time) {
        return time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    }


}
