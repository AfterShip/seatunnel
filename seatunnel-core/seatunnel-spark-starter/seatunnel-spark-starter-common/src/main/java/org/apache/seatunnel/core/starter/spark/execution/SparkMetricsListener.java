package org.apache.seatunnel.core.starter.spark.execution;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.utils.PrintUtil;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * spark metric
 */
@Slf4j
class SparkMetricsListener extends SparkListener {
    private final Long unit = 100000000L;
    private AtomicInteger jobsCompleted = new AtomicInteger(0);
    private AtomicInteger stagesCompleted = new AtomicInteger(0);
    private AtomicInteger tasksCompleted = new AtomicInteger(0);
    private LongAccumulator recordsRead = new LongAccumulator();
    private LongAccumulator stageTotalInputBytes = new LongAccumulator();
    private LongAccumulator stageTotalOutputBytes = new LongAccumulator();
    private LongAccumulator readBytes = new LongAccumulator();
    private LongAccumulator recordsWritten = new LongAccumulator();
    private LongAccumulator writeBytes = new LongAccumulator();
    private LongAccumulator jvmGCTime = new LongAccumulator();
    private String executorCpuTime = "0";
    private String executorRunTime = "0";
    private Double jobRuntime = 0.0;
    private LongAccumulator peakExecutionMemory = new LongAccumulator();
    private LongAccumulator numTasks = new LongAccumulator();
    private Double submissionTime = 0.0;
    private Double completionTime = 0.0;
    private LongAccumulator attemptNumber = new LongAccumulator();
    private String taskEndReason = "";
    private Map<String, String> failureReason = new HashMap<>();
    private Map<String, Object> metrics = new LinkedHashMap<>();
    private SparkSession sparkSession;
    private String jobName;
    private Long startTime;
    private String applicationId;

    SparkMetricsListener(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.jobName = sparkSession.sparkContext().appName();
        this.startTime = sparkSession.sparkContext().startTime();
        this.applicationId = sparkSession.sparkContext().applicationId();
    }

    /**
     * Get stage index value
     *
     * @param stageCompleted stageCompleted
     */
    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        stagesCompleted.incrementAndGet();
        if (stageCompleted.stageInfo().failureReason().nonEmpty()) {
            failureReason.put(
                    stageCompleted.stageInfo().name(),
                    stageCompleted.stageInfo().failureReason().get());
        }
        numTasks.add(stageCompleted.stageInfo().numTasks());
        submissionTime =
                Double.parseDouble(stageCompleted.stageInfo().submissionTime().get().toString())
                        / unit
                        / 10000;
        completionTime =
                Double.parseDouble(stageCompleted.stageInfo().completionTime().get().toString())
                        / unit
                        / 10000;
        attemptNumber.add(stageCompleted.stageInfo().attemptNumber());
        InputMetrics inputMetrics = stageCompleted.stageInfo().taskMetrics().inputMetrics();
        OutputMetrics outputMetrics = stageCompleted.stageInfo().taskMetrics().outputMetrics();
        stageTotalInputBytes.add(inputMetrics._bytesRead().value());
        stageTotalOutputBytes.add(outputMetrics._bytesWritten().value());
        //        recordsWritten = outputMetrics.recordsWritten();
        //        writeBytes.add(outputMetrics._bytesWritten().value());
    }

    /**
     * To get the index value of each task, it should be noted here that each task of the
     * distributed system runs on a different executor, and the local running on the driver does not
     * need to accumulate the value twice, so some shuffle values need to be accumulated
     *
     * @param taskEnd taskEnd
     */
    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        tasksCompleted.incrementAndGet();
        // peak memory usage
        peakExecutionMemory.add(taskEnd.taskMetrics().peakExecutionMemory());
        // keep only one value
        taskEndReason = taskEnd.reason().toString();
        jvmGCTime.add(taskEnd.taskMetrics().jvmGCTime() / unit);
        executorCpuTime =
                new java.math.BigDecimal(taskEnd.taskMetrics().executorCpuTime() / unit)
                        .setScale(2, RoundingMode.UP)
                        .toPlainString();
        executorRunTime =
                new java.math.BigDecimal(taskEnd.taskMetrics().executorRunTime() / 1000)
                        .setScale(2, RoundingMode.UP)
                        .toPlainString();
        ShuffleReadMetrics shuffleReadMetrics = taskEnd.taskMetrics().shuffleReadMetrics();
        ShuffleWriteMetrics shuffleWriteMetrics = taskEnd.taskMetrics().shuffleWriteMetrics();

        recordsRead.add(taskEnd.taskMetrics().inputMetrics()._recordsRead().value());
        recordsRead.add(shuffleReadMetrics._recordsRead().value());
        readBytes.add(shuffleReadMetrics.totalBytesRead());
        writeBytes.add(shuffleWriteMetrics._bytesWritten().value());
        recordsWritten.add(taskEnd.taskMetrics().outputMetrics()._recordsWritten().value());
    }

    /**
     * Get the number of jobs and the execution time after the job ends
     *
     * @param jobEnd jobEnd
     */
    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        jobsCompleted.incrementAndGet();
        jobRuntime = Double.parseDouble(String.valueOf(jobEnd.time() - startTime)) / 1000;
    }

    /**
     * After the application ends, the indicators are collected and output or output to an external
     * system to generate a task execution report
     *
     * @param applicationEnd applicationEnd
     */
    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        // Handling no shuffle stage value statistics
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        metrics.put("jobName", jobName);
        metrics.put("createTime", dateFormat.format(new Date(startTime)));
        metrics.put("applicationId", applicationId);
        metrics.put("recordsRead", recordsRead.value());
        metrics.put("recordsWritten", recordsWritten.value());
        metrics.put("jobsCompleted", jobsCompleted.get());
        metrics.put("jobRuntime(s)", jobRuntime);
        metrics.put("stagesCompleted", stagesCompleted.get());
        metrics.put("tasksCompleted", tasksCompleted);
        metrics.put("stageSubmissionTime(s)", submissionTime);
        metrics.put("stageCompletionTime(s)", completionTime);
        metrics.put("stageAttemptNumber", attemptNumber.value());
        metrics.put("executorRuntime(s)", executorRunTime);
        metrics.put("shuffleReadBytes", readBytes.value());
        metrics.put("shuffleWriteBytes", writeBytes.value());
        metrics.put("totalInputBytes", stageTotalInputBytes.value());
        metrics.put("totalOutputBytes", stageTotalOutputBytes.value());
        metrics.put("jvmGCTime(s)", jvmGCTime.value());
        metrics.put("executorCpuTime(s)", executorCpuTime);
        metrics.put("peakExecutionMemory", peakExecutionMemory.value());
        metrics.put("taskEndReason", taskEndReason);
        metrics.put("failureReason", failureReason);
        PrintUtil.printResult(metrics);
    }
}
