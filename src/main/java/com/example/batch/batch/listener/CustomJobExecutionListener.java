package com.example.batch.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CustomJobExecutionListener implements JobExecutionListener {

    @Override
    public void afterJob(JobExecution jobExecution) {
        jobExecution.getStepExecutions()
                .stream()
                .findAny()
                .ifPresent(stepExecution -> {
                    long writeCount = stepExecution.getWriteCount();
                    log.info("The job has written {} lines", writeCount);
                });
    }
}
