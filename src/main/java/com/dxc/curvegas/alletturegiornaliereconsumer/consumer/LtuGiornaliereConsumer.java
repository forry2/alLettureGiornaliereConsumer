package com.dxc.curvegas.alletturegiornaliereconsumer.consumer;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.*;
import com.dxc.curvegas.alletturegiornaliereconsumer.repository.CustomLtuGiornaliereAggregatedRepository;
import com.dxc.curvegas.alletturegiornaliereconsumer.service.CurveGasService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.*;

@Component
public class LtuGiornaliereConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    MongoTemplate mongoTemplate;
    @Autowired
    CustomLtuGiornaliereAggregatedRepository repository;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private CurveGasService curveGasService;

    public LtuGiornaliereConsumer() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(LtuGiornaliereRawDto.class, new LtuGiornaliereDeserializer());
        objectMapper.registerModule(module);
    }

//    private Date getFirstDateOfMonth(Date date) {
//        return Date.from(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant());
//    }
//
//    private Date getLastDateOfMonth(Date date) {
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(date);
//        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
//        return calendar.getTime();
//    }

    @KafkaListener(topics = "LTU_GIORNALIERE_TOPIC", concurrency = "1")
    public void processMessage(
            @Payload String content,
            @Header("ccgHeader.messageType") String ccgMessageType
    ) throws JsonProcessingException {
        long startTime = new Date().getTime();
        log.debug("Received json message: {}", content);
        switch (ccgMessageType){
            case "LTU_GIORNALIERA":
                curveGasService.manageLtuGiornalieraMessage(content);
                break;
            case "PDM_PDF_TLELET_GUASTI":
                curveGasService.manageGuastoMessage(content);
                break;
            default:
                break;
        }
        log.debug("Message processed in {} ms", new Date().getTime() - startTime);
    }
}
