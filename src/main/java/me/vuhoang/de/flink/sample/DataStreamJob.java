/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.vuhoang.de.flink.sample;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import me.vuhoang.de.flink.sample.deserializer.JSONValueDeserializationSchema;
import me.vuhoang.de.flink.sample.dto.SalesPerCategoryDTO;
import me.vuhoang.de.flink.sample.dto.SalesPerDayDTO;
import me.vuhoang.de.flink.sample.dto.SalesPerMonthDTO;
import me.vuhoang.de.flink.sample.dto.TransactionDTO;
import me.vuhoang.de.flink.sample.utils.JsonUtil;
import me.vuhoang.de.flink.sample.utils.ParameterToolUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static final String JDBC_URL_DEFAULT_VALUE = "jdbc:postgresql://postgres:5432/postgres";
	private static final String USER_DEFAULT_VALUE = "postgres";
	private static final String PASSWORD_DEFAULT_VALUE = "postgres";
	private static final String FLINK_APPLICATION_GROUP_ID = "FlinkEcommerce";

	private static final String KAFKA_SERVER_KEY = "kafkaServers";
	private static final String DATABASE_URL_KEY = "databaseUrl";
	private static final String DATABASE_USER_KEY = "databaseUser";
	private static final String DATABASE_PASSWORD_KEY = "databasePassword";

	private static boolean isLocal(StreamExecutionEnvironment env) {
		return env instanceof LocalStreamEnvironment;
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		ParameterTool parameter = loadParameter(args, env);

		String topic = "financial_transactions";

		KafkaSource<TransactionDTO> source = KafkaSource.<TransactionDTO>builder()
												.setBootstrapServers(parameter.get(KAFKA_SERVER_KEY, "broker:29092"))
												.setTopics(topic).setGroupId("flink-group")
												.setStartingOffsets(OffsetsInitializer.latest())
												.setValueOnlyDeserializer(new JSONValueDeserializationSchema()).build();


		DataStream<TransactionDTO> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source");

		JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(parameter.get(DATABASE_URL_KEY, JDBC_URL_DEFAULT_VALUE))
				.withPassword(parameter.get(DATABASE_USER_KEY, USER_DEFAULT_VALUE))
				.withUsername(parameter.get(DATABASE_PASSWORD_KEY, PASSWORD_DEFAULT_VALUE))
				.withDriverName("org.postgresql.Driver")
				.build();

		transactionStream.addSink(createTransactionSink(executionOptions, connectionOptions)).name("Create transactions table sink");

		transactionStream.addSink(createSalesPerCategorySink(executionOptions, connectionOptions)).name("Create sales_per_category table sink");

		transactionStream.addSink(createSalesPerDaySink(executionOptions, connectionOptions)).name("Create sales_per_day table sink");

		transactionStream.addSink(createSalePerMonthSink(executionOptions, connectionOptions)).name("Create sales_per_month table sink");

		transactionStream.addSink(insertTransactionTableSink(executionOptions, connectionOptions)).name("Insert into transactions table sink");

		transactionStream.map( transaction -> {
			Date transactionDate = Date.valueOf(
					transaction
							.getTransactionDate()
							.toLocalDateTime()
							.toLocalDate()
			);
			String category = transaction.getProductCategory();
			double totalSales = transaction.getTotalAmount();
			return new SalesPerCategoryDTO(transactionDate, category, totalSales);
		}).keyBy(SalesPerCategoryDTO::getCategory).reduce(((salesPerCategory, t1) -> {
			salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
			return salesPerCategory;
		})).addSink(insertSalesPerCategorySink(executionOptions, connectionOptions)).name("Insert into sales_per_category table sink");

		transactionStream.map( transaction -> {
			LocalDateTime localDateTime = transaction.getTransactionDate().toLocalDateTime();
			LocalDate localDate = LocalDate.of(
					localDateTime.getYear(),
					localDateTime.getMonth(),
					localDateTime.getDayOfMonth()
			);

			Date transactionDate = Date.valueOf(localDate);

			double totalSales = transaction.getTotalAmount();
			return new SalesPerDayDTO(transactionDate, totalSales);
		}).keyBy(SalesPerDayDTO::getTransactionDate).reduce(((salesPerCategory, t1) -> {
			salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
			return salesPerCategory;
		})).addSink(insertSalePerDaySink(executionOptions, connectionOptions)).name("Insert into sales_per_day table sink");

		transactionStream.map( transaction -> {
			LocalDateTime localDateTime = transaction.getTransactionDate().toLocalDateTime();

			double totalSales = transaction.getTotalAmount();
			return new SalesPerMonthDTO(localDateTime.getYear(), localDateTime.getMonthValue(), totalSales);
		}).keyBy(SalesPerMonthDTO::getMonth).reduce(((salesPerCategory, t1) -> {
			salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
			return salesPerCategory;
		})).addSink(insertSalePerMonthSink(executionOptions, connectionOptions)).name("Insert into sales_per_month table sink");

//		transactionStream.sinkTo(
//				generateElasticSearchSink()
//		).name("Elasticsearch Sink");
		// Execute program, beginning computation.
		env.execute("Flink Ecommerce Data Stream");
	}

	private static ElasticsearchSink<TransactionDTO> generateElasticSearchSink() {
		return new Elasticsearch7SinkBuilder<TransactionDTO>()
				.setHosts(new HttpHost("es-container", 9200, "http"))
				.setEmitter((transaction, runtimeContext, requestIndexer) -> {
					String json = JsonUtil.convertObjectToJson(transaction);
					IndexRequest indexRequest = Requests.indexRequest()
							.index("transactions")
							.id(transaction.getTransactionId())
							.source(json, XContentType.JSON);
					requestIndexer.add(indexRequest);
				}).build();
	}

	private static SinkFunction<SalesPerMonthDTO> insertSalePerMonthSink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"INSERT INTO sales_per_month(year, month, total_sales) " +
						"VALUES (?, ?, ?) " +
						"ON CONFLICT (year, month) DO UPDATE SET " +
						"total_sales = EXCLUDED.total_sales " +
						"WHERE sales_per_month.year = EXCLUDED.year " +
						"AND sales_per_month.month = EXCLUDED.month;",
				(JdbcStatementBuilder<SalesPerMonthDTO>) (preparedStatement, salesPerMonthDTO) -> {
					preparedStatement.setInt(1, salesPerMonthDTO.getYear());
					preparedStatement.setInt(2, salesPerMonthDTO.getMonth());
					preparedStatement.setDouble(3, salesPerMonthDTO.getTotalSales());
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<SalesPerDayDTO> insertSalePerDaySink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"INSERT INTO sales_per_day(transaction_date, total_sales) " +
						"VALUES (?, ?) " +
						"ON CONFLICT (transaction_date) DO UPDATE SET " +
						"total_sales = EXCLUDED.total_sales " +
						"WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date;",
				(JdbcStatementBuilder<SalesPerDayDTO>) (preparedStatement, salesPerDayDTO) -> {
					preparedStatement.setDate(1, salesPerDayDTO.getTransactionDate());
					preparedStatement.setDouble(2, salesPerDayDTO.getTotalSales());
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<SalesPerCategoryDTO> insertSalesPerCategorySink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
						"VALUES (?, ?, ?) " +
						"ON CONFLICT (transaction_date, category) DO UPDATE SET " +
						"total_sales = EXCLUDED.total_sales " +
						"WHERE sales_per_category.transaction_date = EXCLUDED.transaction_date " +
						"AND sales_per_category.category = EXCLUDED.category",
				(JdbcStatementBuilder<SalesPerCategoryDTO>) (preparedStatement, salesPerCategoryDTO) -> {
					preparedStatement.setDate(1, salesPerCategoryDTO.getTransactionDate());
					preparedStatement.setString(2, salesPerCategoryDTO.getCategory());
					preparedStatement.setDouble(3, salesPerCategoryDTO.getTotalSales());
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<TransactionDTO> insertTransactionTableSink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, product_quantity, product_brand," +
						"total_amount, currency, customer_id, transaction_date, payment_method) " +
						"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
						"ON CONFLICT (transaction_id) DO UPDATE SET " +
						"product_id = EXCLUDED.product_id, " +
						"product_name = EXCLUDED.product_name, " +
						"product_category = EXCLUDED.product_category, " +
						"product_price = EXCLUDED.product_price, " +
						"product_quantity = EXCLUDED.product_quantity, " +
						"product_brand = EXCLUDED.product_brand, " +
						"total_amount = EXCLUDED.total_amount, " +
						"currency = EXCLUDED.currency, " +
						"customer_id = EXCLUDED.customer_id, " +
						"transaction_date = EXCLUDED.transaction_date, " +
						"payment_method = EXCLUDED.payment_method " +
						"WHERE transactions.transaction_id = EXCLUDED.transaction_id",
				(JdbcStatementBuilder<TransactionDTO>) (preparedStatement, transactionDTO) -> {
					preparedStatement.setString(1, transactionDTO.getTransactionId());
					preparedStatement.setString(2, transactionDTO.getProductId());
					preparedStatement.setString(3, transactionDTO.getProductName());
					preparedStatement.setString(4, transactionDTO.getProductCategory());
					preparedStatement.setDouble(5, transactionDTO.getProductPrice());
					preparedStatement.setInt(6, transactionDTO.getProductQuantity());
					preparedStatement.setString(7, transactionDTO.getProductBrand());
					preparedStatement.setDouble(8, transactionDTO.getTotalAmount());
					preparedStatement.setString(9, transactionDTO.getCurrency());
					preparedStatement.setString(10, transactionDTO.getCustomerId());
					preparedStatement.setTimestamp(11, transactionDTO.getTransactionDate());
					preparedStatement.setString(12, transactionDTO.getPaymentMethod());
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<TransactionDTO> createSalePerMonthSink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_month (" +
						"year INTEGER, " +
						"month INTEGER, " +
						"total_sales DOUBLE PRECISION, " +
						"PRIMARY KEY (year, month)" +
						")",
				(JdbcStatementBuilder<TransactionDTO>) (preparedStatement, transactionDTO) -> {
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<TransactionDTO> createSalesPerDaySink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_day (" +
						"transaction_date DATE PRIMARY KEY, " +
						"total_sales DOUBLE PRECISION )",
				(JdbcStatementBuilder<TransactionDTO>) (preparedStatement, transactionDTO) -> {
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<TransactionDTO> createSalesPerCategorySink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_category (" +
						"transaction_date DATE, " +
						"category VARCHAR(255), " +
						"total_sales DOUBLE PRECISION, " +
						"PRIMARY KEY (transaction_date, category)" +
						")",
				(JdbcStatementBuilder<TransactionDTO>) (preparedStatement, transactionDTO) -> {
				},
				executionOptions,
				connectionOptions
		);
	}

	private static SinkFunction<TransactionDTO> createTransactionSink(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
		return JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS transactions (" +
						"transaction_id VARCHAR(255) PRIMARY KEY, " +
						"product_id VARCHAR(255), " +
						"product_name VARCHAR(255), " +
						"product_category VARCHAR(255), " +
						"product_price DOUBLE PRECISION, " +
						"product_quantity INTEGER, " +
						"product_brand VARCHAR(255), " +
						"total_amount DOUBLE PRECISION, " +
						"currency VARCHAR(255), " +
						"customer_id VARCHAR(255), " +
						"transaction_date TIMESTAMP, " +
						"payment_method VARCHAR(255) " +
						")",
				(JdbcStatementBuilder<TransactionDTO>) (preparedStatement, transactionDTO) -> {
				},
				executionOptions,
				connectionOptions
		);
	}

	private static ParameterTool loadParameter(String[] args, StreamExecutionEnvironment env) throws IOException {
		ParameterTool parameter;

		if (isLocal(env)) {
			parameter = ParameterTool.fromArgs(args);
		} else {
			// read properties from Kinesis Data Analytics environment
			Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
			Properties flinkProperties = applicationProperties.get(FLINK_APPLICATION_GROUP_ID);
			if (flinkProperties == null) {
				throw new RuntimeException("Unable to load properties from Group ID" + FLINK_APPLICATION_GROUP_ID);
			}
			parameter = ParameterToolUtils.fromApplicationProperties(flinkProperties);
		}

		return parameter;
	}
}
