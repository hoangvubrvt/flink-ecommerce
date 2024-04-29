package me.vuhoang.de.flink.sample.deserializer;

import me.vuhoang.de.flink.sample.dto.TransactionDTO;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class JSONValueDeserializationSchema implements org.apache.flink.api.common.serialization.DeserializationSchema<me.vuhoang.de.flink.sample.dto.TransactionDTO> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public TransactionDTO deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, TransactionDTO.class);
    }

    @Override
    public boolean isEndOfStream(TransactionDTO transactionDTO) {
        return false;
    }

    @Override
    public TypeInformation<TransactionDTO> getProducedType() {
        return TypeInformation.of(TransactionDTO.class);
    }
}
