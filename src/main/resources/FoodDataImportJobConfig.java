package com.romertec.fsdata.batch;

import com.romertec.fsdata.entity.Menu;
import com.romertec.fsdata.entity.Restaurant;
import com.romertec.fsdata.support.CsvUtils;
import com.romertec.fsdata.support.MenuCsvRow;
import com.romertec.fsdata.support.PriceParser;
import com.romertec.fsdata.support.RestaurantCsvRow;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Objects;

@Configuration
public class FoodDataImportJobConfig {

    // Ajusta el chunk según el tamaño real de los/player files
    private static final int CHUNK_SIZE = 1000;

    // ========= JOB =========

    @Bean
    public Job foodDataImportJob(
            JobRepository jobRepository,
            Step importRestaurantsStep,
            Step importMenusStep
    ) {
        return new JobBuilder("foodDataImportJob", jobRepository)
                .start(importRestaurantsStep)
                .next(importMenusStep)
                .build();
    }

    // ========= STEPS =========

    @Bean
    public Step importRestaurantsStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<RestaurantCsvRow> restaurantsReader,
            ItemProcessor<RestaurantCsvRow, Restaurant> restaurantProcessor,
            JdbcBatchItemWriter<Restaurant> restaurantWriter
    ) {
        return new org.springframework.batch.core.step.builder.StepBuilder("importRestaurantsStep", jobRepository)
                .<RestaurantCsvRow, Restaurant>chunk(CHUNK_SIZE, transactionManager)
                .reader(restaurantsReader)
                .processor(restaurantProcessor)
                .writer(restaurantWriter)
                .build();
    }

    @Bean
    public Step importMenusStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<MenuCsvRow> menusReader,
            ItemProcessor<MenuCsvRow, Menu> menuProcessor,
            JdbcBatchItemWriter<Menu> menuWriter
    ) {
        return new org.springframework.batch.core.step.builder.StepBuilder("importMenusStep", jobRepository)
                .<MenuCsvRow, Menu>chunk(CHUNK_SIZE, transactionManager)
                .reader(menusReader)
                .processor(menuProcessor)
                .writer(menuWriter)
                .build();
    }

    // ========= READERS =========

    @Bean
    public FlatFileItemReader<RestaurantCsvRow> restaurantsReader() {
        FlatFileItemReader<RestaurantCsvRow> reader = new FlatFileItemReader<>();
        reader.setName("restaurantsCsvReader");
        reader.setResource(new ClassPathResource("food-data/restaurants.csv"));
        reader.setLinesToSkip(1);

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);
        tokenizer.setNames(
                "id",
                "position",
                "name",
                "score",
                "ratings",
                "category",
                "priceRange",
                "fullAddress",
                "zipCode",
                "lat",
                "lng"
        );

        BeanWrapperFieldSetMapper<RestaurantCsvRow> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(RestaurantCsvRow.class);

        DefaultLineMapper<RestaurantCsvRow> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    public FlatFileItemReader<MenuCsvRow> menusReader() {
        FlatFileItemReader<MenuCsvRow> reader = new FlatFileItemReader<>();
        reader.setName("menusCsvReader");
        reader.setResource(new ClassPathResource("food-data/restaurant-menus.csv"));
        reader.setLinesToSkip(1);

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);
        tokenizer.setNames(
                "restaurantId",
                "category",
                "name",
                "description",
                "price"
        );

        BeanWrapperFieldSetMapper<MenuCsvRow> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(MenuCsvRow.class);

        DefaultLineMapper<MenuCsvRow> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }

    // ========= PROCESSORS =========

    @Bean
    public ItemProcessor<RestaurantCsvRow, Restaurant> restaurantProcessor() {
        return row -> {
            if (row == null || row.getId() == null) {
                return null;
            }

            Restaurant r = new Restaurant();
            r.setId(row.getId());
            r.setName(CsvUtils.clean(row.getName()));
            r.setCategory(CsvUtils.clean(row.getCategory()));
            r.setPosition(CsvUtils.clean(row.getPosition()));
            r.setScore(CsvUtils.clean(row.getScore()));
            // DB column is `raitings` (typo en la tabla), CSV trae `ratings`
            r.setRaitings(CsvUtils.clean(row.getRatings()));
            r.setPriceRange(CsvUtils.clean(row.getPriceRange()));
            r.setLat(CsvUtils.clean(row.getLat()));
            r.setLng(CsvUtils.clean(row.getLng()));

            CsvUtils.AddressParts ap = CsvUtils.parseFullAddress(row.getFullAddress(), row.getZipCode());
            r.setStreet(ap.street());
            r.setCity(ap.city());
            r.setState(ap.state());
            r.setZip(ap.zip());
            r.setUnit(ap.unit());

            return r;
        };
    }

    @Bean
    public ItemProcessor<MenuCsvRow, Menu> menuProcessor(JdbcTemplate jdbcTemplate) {
        return row -> {
            if (row == null || row.getRestaurantId() == null) {
                return null;
            }

            Menu m = new Menu();
            m.setId(nextIdFromSeqTable(jdbcTemplate, "menu_seq")); // genera id desde la tabla seq
            m.setRestaurantId(row.getRestaurantId());
            m.setCategory(CsvUtils.clean(row.getCategory()));
            m.setName(CsvUtils.clean(row.getName()));
            m.setDescription(CsvUtils.clean(row.getDescription()));
            m.setPrice(PriceParser.normalize(row.getPrice()));

            return m;
        };
    }

    // ========= WRITERS (JDBC batch) =========

    @Bean
    public JdbcBatchItemWriter<Restaurant> restaurantWriter(DataSource dataSource) {
        JdbcBatchItemWriter<Restaurant> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("""
                INSERT INTO restaurants
                (id, category, city, lat, lng, name, position, price_range, raitings, score, state, street, unit, zip)
                VALUES
                (:id, :category, :city, :lat, :lng, :name, :position, :priceRange, :raitings, :score, :state, :street, :unit, :zip)
                """);
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    @Bean
    public JdbcBatchItemWriter<Menu> menuWriter(DataSource dataSource) {
        JdbcBatchItemWriter<Menu> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("""
                INSERT INTO menu
                (id, category, description, name, price, restaurant_id)
                VALUES
                (:id, :category, :description, :name, :price, :restaurantId)
                """);
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    // ========= SEQ ID (MySQL table seq con 1 fila, col: next_val) =========

    private int nextIdFromSeqTable(JdbcTemplate jdbcTemplate, String seqTableName) {
        // Incremento atómico en MySQL:
        // 1) Actualiza next_val = next_val + 1
        // 2) LAST_INSERT_ID(x) guarda x por conexión
        // 3) SELECT LAST_INSERT_ID() devuelve el nuevo valor sin carrera
        String update = "UPDATE " + seqTableName + " SET next_val = LAST_INSERT_ID(next_val + 1)";
        String select = "SELECT LAST_INSERT_ID()";

        int updated = jdbcTemplate.update(update);

        // Si no hay fila (tabla vacía), inicializa y reintenta
        if (updated == 0) {
            jdbcTemplate.update("INSERT INTO " + seqTableName + " (next_val) VALUES (1)");
            jdbcTemplate.update(update);
        }

        Long newVal = jdbcTemplate.queryForObject(select, Long.class);
        if (newVal == null) {
            throw new IllegalStateException("Could not read LAST_INSERT_ID() for " + seqTableName);
        }
        if (newVal > Integer.MAX_VALUE) {
            throw new IllegalStateException("Sequence overflow in " + seqTableName + ": " + newVal);
        }

        // OJO: este método devuelve el valor incrementado (post-increment).
        // Si quieres usar el valor "antes" del incremento, ajustamos la fórmula.
        return newVal.intValue();
    }

}
