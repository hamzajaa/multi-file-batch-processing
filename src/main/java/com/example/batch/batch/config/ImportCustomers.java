package com.example.batch.batch.config;

import com.example.batch.dao.CustomerDao;
import com.example.batch.bean.Customer;
import com.example.batch.batch.listener.CustomJobExecutionListener;
import com.example.batch.batch.reader.MultiResourceReaderThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Objects;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class ImportCustomers {

    //    @Value("${input.folder.customers}")
    @Setter
    @Getter
    private Resource[] resources = {
//            new ClassPathResource("/data/customers.csv"),
//            new ClassPathResource("/data/customers1.csv"),
//            new ClassPathResource("/data/customers2.csv"),
//            new ClassPathResource("/data/customers3.csv")
    };

    private final CustomJobExecutionListener customJobExecutionListener;
    private final CustomerDao customerDao;

    @Bean
    public Job customerJob(JobRepository jobRepository, Step customerStep) {
        return new JobBuilder("customerJob", jobRepository)
                .incrementer(new RunIdIncrementer()) // to generate a unique job instance id
                .start(customerStep)
                .listener(customJobExecutionListener) // to listen to the job execution
                .build();
    }

    @Bean
    public Step customerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("customerStep", jobRepository)
                .<Customer, Customer>chunk(100, transactionManager) // the chunk size => every 100 items, having processing a single thread
                .reader(multiResourceReaderThreadSafe()) // the reader to be used to read the resources
                .processor(item -> customerProcessor(item)) // the processor to be used to process the items
                .writer(items -> customerWriter(items)) // the writer to be used to write the items
                .taskExecutor(taskExecutor()) // the executor to be used to execute the tasks
                .build();

    }

    public void customerWriter(Chunk<? extends Customer> items) {
        log.info("Writing items: {}", items);
    }

    public Customer customerProcessor(Customer item) {
        customerDao.save(item);
        log.info("Processing item: {}", item);
        return item;
    }

    @Bean
    @StepScope // for each step execution, a new instance of the reader is created
    public MultiResourceReaderThreadSafe<Customer> multiResourceReaderThreadSafe() {
        var multiResourceReader = new MultiResourceReaderThreadSafe<>(multiResourceItemReader());
        multiResourceReader.setResources(resources);
        return multiResourceReader;
    }

    @Bean
    public MultiResourceItemReader<Customer> multiResourceItemReader() {
        return new MultiResourceItemReaderBuilder<Customer>()
                .name("customer resources reader")
                .resources(resources) // the resources to be read
                .delegate(customerReader()) // the reader to be used to read the resources
                .build();
    }

    // Reader must be open before it can be read. => FlatFileItemReader is not thread-safe
    public ResourceAwareItemReaderItemStream<Customer> customerReader() {
        return new FlatFileItemReaderBuilder<Customer>()
//                .strict(false) // (by default is true) when true, the reader will throw an exception if a line is shorter than expected
                .name("Customer ItemReader")
                .saveState(false) // when false, the name is not required => when false
                .linesToSkip(1) // skip the first line
                .delimited()
                .delimiter(",")
                .names("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob")
                .comments("#","Evaluation Only. Created with Aspose.Cells for Java.Copyright 2003 - 2024 Aspose Pty Ltd.") // ignore lines starting with #
                .targetType(Customer.class) // the type to be returned by the reader
                .build();
    }

    @Bean
    public VirtualThreadTaskExecutor taskExecutor() {
        return new VirtualThreadTaskExecutor("Custom-Thread-");
    }

}
