package com.dxc.curvegas.alletturegiornaliereconsumer.consumer;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.*;
import com.dxc.curvegas.alletturegiornaliereconsumer.repository.CustomLtuGiornaliereAggregatedRepository;
import com.dxc.curvegas.alletturegiornaliereconsumer.service.CurveGasService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
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

    private Date getFirstDateOfMonth(Date date) {
        return Date.from(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant());
    }

    private Date getLastDateOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        return calendar.getTime();
    }

    @KafkaListener(topics = "LTU_GIORNALIERE_TOPIC", concurrency = "1")
    public void processMessage(String content) throws JsonProcessingException {
        long startTime = new Date().getTime();
        log.debug("Received json message: {}", content);
        LtuGiornaliereRawDto nuovaLetturaGiornaliera = objectMapper.readValue(content, LtuGiornaliereRawDto.class);
        mongoTemplate.insert(nuovaLetturaGiornaliera, "ltuGiornaliereRaw");
        if (nuovaLetturaGiornaliera.datLettura == null) {
            log.error("Content {} caused Exception in parsing", content);
            return;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(nuovaLetturaGiornaliera.datLettura);
        String mese = String.format("%02d", cal.get(Calendar.MONTH) + 1);
        String anno = String.format("%04d", cal.get(Calendar.YEAR));

        LtuGiornaliereAggregatedDto currentContentDateLtuAggr = repository.getLtuGiornaliereAggregatedDtoByAnnoAndMeseAndCodPdfAndCodTipoFornituraAndCodTipVoceLtu(anno, mese, nuovaLetturaGiornaliera.codPdf, nuovaLetturaGiornaliera.codTipoFornitura, nuovaLetturaGiornaliera.codTipVoceLtu);
        if (currentContentDateLtuAggr == null) {
            // Non c'è ancora nessun dato aggregato con questo anno/mese/pdf/tipo
            // Ne creo uno nuovo e lo inserisco nel db
            currentContentDateLtuAggr =
                    LtuGiornaliereAggregatedDto
                            .builder()
                            .codPdf(nuovaLetturaGiornaliera.codPdf)
                            .codTipoFornitura(nuovaLetturaGiornaliera.codTipoFornitura)
                            .codPdm(nuovaLetturaGiornaliera.codPdm)
                            .mese(mese)
                            .anno(anno)
                            .firstCurveDate(getFirstDateOfMonth(nuovaLetturaGiornaliera.datLettura))
                            .lastCurveDate(getLastDateOfMonth(nuovaLetturaGiornaliera.datLettura))
                            .codTipVoceLtu(nuovaLetturaGiornaliera.codTipVoceLtu)
                            .consumoReale(null)
                            .dtaPrimaLetturaValida(null)
                            .primaLetturaValida(null)
                            .dtaUltimaLetturaValida(null)
                            .ultimaLetturaValida(null)
                            .maxQuaLettura(null)
                            .minQuaLettura(null)
                            .ltuGiornaliereStatsDto(new LtuGiornaliereStatsDto())
                            .lettureSingole(new ArrayList<>())
                            .build();
            currentContentDateLtuAggr.pushLtuGiornalieraRaw(nuovaLetturaGiornaliera);

            for (Date runningDate = currentContentDateLtuAggr.getFirstCurveDate(); !runningDate.after(currentContentDateLtuAggr.getLastCurveDate()); runningDate = DateUtils.addDays(runningDate, 1)) {
                // TODO codice per creare il nuovo mese se non ci sono aggregati con le stesse chiavi
                Date finalRunningDate = runningDate;
                if (currentContentDateLtuAggr.lettureSingole.stream().noneMatch(ltuSingola -> ltuSingola.getDatLettura().compareTo(finalRunningDate) == 0)) {
                    LtuGiornaliereRawDto letturaSingola = LtuGiornaliereRawDto.builder().datLettura(finalRunningDate).build();
                    currentContentDateLtuAggr.pushLtuGiornalieraRaw(letturaSingola);
                }
            }

//            curveGasService.updateConsumiReali(ltuGiornaliereAggregatedDto);
//            curveGasService.updateStatistics(currentContentDateLtuAggr);

            mongoTemplate.insert(currentContentDateLtuAggr, "ltuGiornaliereAggregated");
        } else {
            // C'è già un dato aggregato per questo anno/mese/pdf/tipo
            if (currentContentDateLtuAggr.lettureSingole.stream().noneMatch(l -> l.datLettura.compareTo(nuovaLetturaGiornaliera.datLettura) == 0)) {
                // Per questo anno/mese/pdf/tipo non c'è una lettura con dtaLettura uguale a quella appena entrata
                // la creo, aggiorno le letture per questo anno/mese/pdf/tipo e salvo
                LtuGiornaliereLetturaSingolaDto singolaDto = new LtuGiornaliereLetturaSingolaDto();
                BeanUtils.copyProperties(nuovaLetturaGiornaliera, singolaDto);
                LtuGiornaliereLetturaSingolaItemDto storicoItem = new LtuGiornaliereLetturaSingolaItemDto();
                BeanUtils.copyProperties(nuovaLetturaGiornaliera, storicoItem);
                singolaDto.storico = new ArrayList<>(List.of(storicoItem));
                currentContentDateLtuAggr.lettureSingole.add(singolaDto);
                curveGasService.updateStatistics(currentContentDateLtuAggr);
                repository.save(currentContentDateLtuAggr);
            } else {
                // Per questo anno/mese/pdf/tipo c'è già una lettura con dtaLettura uguale a quella arrivata
                // Aggiorno i campi vivi, inserisco la lettura arrivata anche nello storico e salvo
                currentContentDateLtuAggr.pushLtuGiornalieraRaw(nuovaLetturaGiornaliera);
//                curveGasService.updateConsumiReali(retrievedLtuAggr);
                curveGasService.updateStatistics(currentContentDateLtuAggr);
//                retrievedLtuAggr.setConsumoReale(curveGasService.getConsumoReale(retrievedLtuAggr));
                repository.save(currentContentDateLtuAggr);

            }
            log.debug("Insert letturaSingola: {}\n for codPdf {}, anno {}, mese {}", nuovaLetturaGiornaliera.getLetturaSingola(), nuovaLetturaGiornaliera.codPdf, anno, mese);
        }
        if (nuovaLetturaGiornaliera.getCodTipLtuGio().equals("4")) {
            curveGasService.interpolateAggregatedLtus(nuovaLetturaGiornaliera, InterpolationDirection.FORWARD);
            curveGasService.interpolateAggregatedLtus(nuovaLetturaGiornaliera, InterpolationDirection.BACKWARD);
        }
        curveGasService.updateStatistics(currentContentDateLtuAggr);
        log.debug("Message processed in {} ms", new Date().getTime() - startTime);
    }
}
