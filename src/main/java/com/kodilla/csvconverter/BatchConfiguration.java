package com.kodilla.csvconverter;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfiguration {

    @Bean
    AgeProcessor processor() {
        return new AgeProcessor();
    }

    @Bean
    FlatFileItemReader<PersonWithBirthYear> reader() {
        FlatFileItemReader<PersonWithBirthYear> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("input.csv"));

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("name", "surname", "birthYear");

        BeanWrapperFieldSetMapper<PersonWithBirthYear> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(PersonWithBirthYear.class);

        DefaultLineMapper<PersonWithBirthYear> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    FlatFileItemWriter<PersonWithAge> writer() {
        BeanWrapperFieldExtractor<PersonWithAge> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] {"name", "surname", "age"});

        DelimitedLineAggregator<PersonWithAge> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        FlatFileItemWriter<PersonWithAge> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(aggregator);

        return writer;
    }

    @Bean
    Step birthToAgeChange(
            ItemReader<PersonWithBirthYear> reader,
            ItemProcessor<PersonWithBirthYear, PersonWithAge> processor,
            ItemWriter<PersonWithAge> writer, JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        return new StepBuilder("birthToAgeChange", jobRepository)
                .<PersonWithBirthYear,PersonWithAge>chunk(100, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    Job birthToAgeChangeJob(Step birthToAgeChange, JobRepository jobRepository) {
        return new JobBuilder("birthToAgeChangeJob",jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(birthToAgeChange)
                .end()
                .build();
    }
}