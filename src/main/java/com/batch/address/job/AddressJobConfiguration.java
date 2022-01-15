package com.batch.address.job;



import com.spring.batch.AddressVo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class AddressJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Value("${geodata.test_dir}")
    private String match_directory;


    @Bean
    public Job stepAddressJob() throws Exception {
        return jobBuilderFactory.get("stepAddressJob")
                .start(start_step())
                .build();
    }


    @Bean
    @StepScope
    public FlatFileItemReader<AddressVo> addressReader() {
        return new FlatFileItemReaderBuilder<AddressVo>()
            .name("userItemReader")
            .delimited().delimiter("|")
            .names(new String[] {"townCode","cityName","cityCountryName","townName","roadNameCode","roadName","undergroundStatus","buildingNum","buildingSideNum","zipCode",
                    "buildingManagementNum","buildingNameForCity","buildingUseClassification","administrativeTownCode","administrativeTownName",
                    "groundFloorNumber","undergroundFloorNumber","classificationApartmentBuildings","buildingCnt","detailBuildingName",
                    "BuildingNameChangeHistory","BuildingNameChangeHistoryDetail","livingStatus","buildingCenterXCoordinate","buildingCenterYCoordinate",
                    "exitXCoordinate","exitYCoordinate","cityNameEng","cityCountryNameEng","townNameEng",
                    "roadNameEng","townMobileClassification","mobileReasonCode"})
            .targetType(AddressVo.class)
            .recordSeparatorPolicy(new SimpleRecordSeparatorPolicy() {
                @Override
                public String postProcess(String record) {
                    return record.trim();
                }
            })
            .build();
    }


    @Bean
    @StepScope
    public MultiResourceItemReader<AddressVo> addressReaderV2() {


        MultiResourceItemReader<AddressVo> multiResourceItemReader = new MultiResourceItemReader<>();
        multiResourceItemReader.setDelegate(this.addressReader());

        List<Resource> resourceList = new ArrayList<>();
        File dir = new File(match_directory);
        if(dir.isDirectory()){
            File[] files = dir.listFiles((directory, name) -> {
                return name.toLowerCase().startsWith("match_building") && name.toLowerCase().endsWith("txt");
            });

            for(File file : files){
                System.out.println(file.getAbsolutePath());
                resourceList.add(new FileSystemResource(file.getAbsolutePath()));
            }
        }

        multiResourceItemReader.setResources(resourceList.toArray(new Resource[0]));

        return multiResourceItemReader;
    }


    @Bean
    public JdbcBatchItemWriter<AddressVo> addressWriter() {
        return new JdbcBatchItemWriterBuilder<AddressVo>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())

            .dataSource(dataSource)
            .sql("INSERT test_address(townCode,cityName,cityCountryName,townName,roadNameCode,roadName,undergroundStatus,buildingNum," +
                "buildingSideNum,zipCode,buildingManagementNum,buildingNameForCity,buildingUseClassification,administrativeTownCode," +
                "administrativeTownName,groundFloorNumber,undergroundFloorNumber,classificationApartmentBuildings,buildingCnt,detailBuildingName," +
                "BuildingNameChangeHistory,BuildingNameChangeHistoryDetail,livingStatus,buildingCenterXCoordinate,buildingCenterYCoordinate," +
                "exitXCoordinate,exitYCoordinate,cityNameEng,cityCountryNameEng,townNameEng,roadNameEng,townMobileClassification,mobileReasonCode) VALUES" +

                "(:townCode,:cityName,:cityCountryName,:townName,:roadNameCode, :roadName,:undergroundStatus,:buildingNum,:buildingSideNum,:zipCode, " +
                ":buildingManagementNum,:buildingNameForCity,:buildingUseClassification,:administrativeTownCode,:administrativeTownName, " +
                ":groundFloorNumber,:undergroundFloorNumber,:classificationApartmentBuildings,:buildingCnt,:detailBuildingName,  " +
                ":BuildingNameChangeHistory,:BuildingNameChangeHistoryDetail,:livingStatus,:buildingCenterXCoordinate,:buildingCenterYCoordinate, " +
                ":exitXCoordinate,:exitYCoordinate,:cityNameEng,:cityCountryNameEng,:townNameEng, " +
                ":roadNameEng,:townMobileClassification,:mobileReasonCode) "
            )
            .build();
    }


    @Bean
    public Step start_step() throws Exception {

        return stepBuilderFactory.get("start_step")
                .<AddressVo, AddressVo>chunk(1)
                .reader(this.addressReaderV2())
                .writer(this.addressWriter())
                .build();
    }





}