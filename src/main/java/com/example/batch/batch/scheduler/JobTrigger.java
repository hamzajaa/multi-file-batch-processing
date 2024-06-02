package com.example.batch.batch.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobTrigger {

    private final JobLauncher jobLauncher; // to launch the job
    private final Job job; // if you have multiple jobs, you must specify the name of the job to be triggered

//    @Scheduled(cron = "0/15 * * ? * *") // every 30 seconds
    @SneakyThrows
    void launchJobPeriodically(){
        log.info("==================> launching the job");
        var jobParameters = new JobParametersBuilder(); // to pass parameters to the job
        jobParameters.addDate("uniqueness", new Date()); // to make the job unique
        JobExecution jobExecution = jobLauncher.run(job, jobParameters.toJobParameters());// to launch the job

        log.info("job finished with status: {}", jobExecution.getExitStatus()); // to get the status of the job
    }

}
