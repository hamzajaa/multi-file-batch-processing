package com.example.batch.service.impl;

import com.aspose.cells.TxtSaveOptions;
import com.aspose.cells.Workbook;
import com.example.batch.batch.config.ImportCustomers;
import com.example.batch.service.CustomerService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Objects;

@Service
@Slf4j
public class CustomerServiceImpl implements CustomerService {

    private final Job job;
    private final JobLauncher jobLauncher;
    private final ImportCustomers importCustomers;

    @Value("${file.path}")
    private String filePath;

    public CustomerServiceImpl(Job job, JobLauncher jobLauncher, ImportCustomers importCustomers) {
        this.job = job;
        this.jobLauncher = jobLauncher;
        this.importCustomers = importCustomers;
    }

    @Override
    public void importCustomers(MultipartFile file) throws Exception {
        String dir = storeFileLocal(file);
        Resource[] resources = new Resource[]{
                new FileSystemResource(dir)
        };
        importCustomers.setResources(resources);

        var jobParameters = new JobParametersBuilder();
        jobParameters.addDate("uniqueness", new Date());
        var jobExecution = jobLauncher.run(job, jobParameters.toJobParameters());

        deleteFiles(dir);
        log.info("job finished with status: {}", jobExecution.getExitStatus()); // to get the status of the job
    }

    @SneakyThrows
    private void deleteFiles(String filePath) {
        String[] strings = filePath.split("/");
        String directoryPath = filePath.replace(strings[strings.length - 1], "");
        directoryPath = directoryPath.substring(0, directoryPath.length() - 1);
        Path dirPath = Paths.get(directoryPath);
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dirPath)) {
            for (Path path : directoryStream) {
                if (Files.isRegularFile(path)) {
                    Files.delete(path);
                }
            }
        }
    }


    private String storeFileLocal(MultipartFile file) throws Exception {
        String dir = System.getProperty("user.dir") + "/" + filePath;
        if (!new File(dir).exists()) {
            new File(dir).mkdir();
        }
        String fileToImport = dir + "/" + file.getOriginalFilename();
        file.transferTo(new File(fileToImport).toPath());
        fileToImport = validateFile(file, fileToImport);
        return fileToImport;
    }

    private String validateFile(MultipartFile file, String dataDir)  {
        boolean validCsv = isValidCsv(file);
        boolean validExcel = isValidExcel(file);
        if (!validCsv && validExcel) {
            // Load the Excel file into Workbook that is to be converted to CSV
            Workbook excelWorkbook = null;
            try {
                excelWorkbook = new Workbook(dataDir);
            } catch (Exception e) {
                throw new IllegalArgumentException("The file is not exist");
            }
            // Instantiate the TxtSaveOption object to set parameters for output CSV
            TxtSaveOptions txtSaveOptions = new TxtSaveOptions();
            // Set the separator that is to be used in the output CSV
            txtSaveOptions.setSeparator(',');
            // Save the Excel workbook as CSV file
            try {
                excelWorkbook.save(dataDir.replace("xlsx", "csv"), txtSaveOptions);
            } catch (Exception e) {
                throw new IllegalArgumentException("error in converting the file");
            }
            return dataDir.replace(".xlsx", ".csv");
        } else if (validCsv) {
            return dataDir;
        } else {
            throw new IllegalArgumentException("Invalid file format");
        }
    }

    private boolean isValidExcel(MultipartFile file) {
        return Objects.equals(file.getContentType(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    }

    private boolean isValidCsv(MultipartFile file) {
        return Objects.equals(file.getContentType(), "text/csv");
    }
}
