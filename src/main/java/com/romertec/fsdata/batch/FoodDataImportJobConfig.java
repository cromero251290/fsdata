package com.romertec.fsdata.batch;

import com.romertec.fsdata.entity.Menu;
import com.romertec.fsdata.entity.Restaurant;
import com.romertec.fsdata.policy.QuoteBalancedRecordSeparatorPolicy;
import com.romertec.fsdata.policy.QuotedMultilineRecordSeparatorPolicy;
import com.romertec.fsdata.support.CsvUtils;
import com.romertec.fsdata.support.MenuCsvRow;
import com.romertec.fsdata.support.PriceParser;
import com.romertec.fsdata.support.RestaurantCsvRow;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.StringUtils;

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
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100000)
                .build();
    }

    // ========= READERS =========

    @Bean
    @StepScope
    public FlatFileItemReader<RestaurantCsvRow> restaurantsReader(
            @Value("${fooddata.base-dir}") String baseDir
    ) {
        String path = normalizeDir(baseDir) + "restaurants.csv";

        FlatFileItemReader<RestaurantCsvRow> reader = new FlatFileItemReader<>();
        reader.setName("restaurantsCsvReader");      // estable para restart
        reader.setResource(new FileSystemResource(path));
        reader.setLinesToSkip(1);
        reader.setSaveState(true);
        reader.setStrict(true);

        // CLAVE: soportar records multi-línea cuando hay \n dentro de campos "quoted"
        reader.setRecordSeparatorPolicy(new QuoteBalancedRecordSeparatorPolicy('"'));

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setQuoteCharacter('"');            // CLAVE: CSV con comillas
        tokenizer.setStrict(false);

        // Si tu CSV trae EXACTAMENTE estas 11 columnas, esto está perfecto.
        tokenizer.setNames(
                "id", "position", "name", "score", "ratings", "category",
                "priceRange", "fullAddress", "zipCode", "lat", "lng"
        );

        // OPCIONAL (recomendado): si el CSV a veces trae columnas extra, fija el mapping a las 11 esperadas
        tokenizer.setIncludedFields(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        BeanWrapperFieldSetMapper<RestaurantCsvRow> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(RestaurantCsvRow.class);

        DefaultLineMapper<RestaurantCsvRow> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }


    @Bean
    @StepScope
    public FlatFileItemReader<MenuCsvRow> menusReader(
            @Value("${fooddata.base-dir}") String baseDir
    ) {
        String path = normalizeDir(baseDir) + "restaurant-menus.csv";

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);

        // CLAVE: el CSV tiene campos con comillas
        tokenizer.setQuoteCharacter('"');

        // Ajusta los nombres EXACTOS a tu CSV (ejemplo típico)
        tokenizer.setNames(
                "restaurantId",   // int
                "category",       // string
                "itemName",       // string
                "description",    // string (puede traer saltos de línea / comas)
                "price"           // string o decimal (según tu modelo)
        );

        BeanWrapperFieldSetMapper<MenuCsvRow> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(MenuCsvRow.class);

        return new FlatFileItemReaderBuilder<MenuCsvRow>()
                .name("menusCsvReader")                 // estable para restart
                .resource(new FileSystemResource(path))
                .linesToSkip(1)                         // header
                .strict(true)
                .saveState(true)
                // CLAVE: recomponer líneas cuando un campo entrecomillado tiene \n
                .recordSeparatorPolicy(new QuotedMultilineRecordSeparatorPolicy())
                .lineTokenizer(tokenizer)
                .fieldSetMapper(mapper)
                .build();
    }

    private static String normalizeDir(String baseDir) {
        if (!StringUtils.hasText(baseDir)) {
            throw new IllegalArgumentException("fooddata.base-dir is empty");
        }
        String d = baseDir.trim().replace("\\", "/");
        if (!d.endsWith("/")) d += "/";
        return d;
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
                ON DUPLICATE KEY UPDATE
                category=VALUES(category),
                city=VALUES(city),
                lat=VALUES(lat),
                lng=VALUES(lng),
                name=VALUES(name),
                position=VALUES(position),
                price_range=VALUES(price_range),
                raitings=VALUES(raitings),
                score=VALUES(score),
                state=VALUES(state),
                street=VALUES(street),
                unit=VALUES(unit),
                zip=VALUES(zip)
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
                ON DUPLICATE KEY UPDATE
                description=VALUES(description)
                """);

        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    // ========= SEQ ID (MySQL table seq con 1 fila, col: next_val) =========

    /**
     * Genera un id incremental usando una tabla tipo:
     * CREATE TABLE menu_seq (next_val bigint DEFAULT NULL);
     * <p>
     * Estrategia:
     * - Asume 1 sola fila en la tabla.
     * - Si está vacía, inserta next_val=1.
     * - Hace SELECT ... FOR UPDATE dentro de la transacción del chunk.
     */
    private int nextIdFromSeqTable(JdbcTemplate jdbcTemplate, String seqTableName) {
        String update = "UPDATE " + seqTableName + " SET next_val = LAST_INSERT_ID(next_val + 1)";
        String select = "SELECT LAST_INSERT_ID()";

        int updated = jdbcTemplate.update(update);
        if (updated == 0) {
            jdbcTemplate.update("INSERT INTO " + seqTableName + " (next_val) VALUES (1)");
            jdbcTemplate.update(update);
        }

        Long newVal = jdbcTemplate.queryForObject(select, Long.class);
        long id = (newVal == null) ? 0 : (newVal - 1); // porque incrementamos antes
        if (id <= 0) id = 1;

        if (id > Integer.MAX_VALUE) {
            throw new IllegalStateException("Sequence overflow in " + seqTableName + ": " + id);
        }
        return (int) id;
    }

}