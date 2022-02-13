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

        LtuGiornaliereAggregatedDto retrievedLtuAggr = repository.getLtuGiornaliereAggregatedDtoByAnnoAndMeseAndCodPdfAndCodTipoFornituraAndCodTipVoceLtu(anno, mese, nuovaLetturaGiornaliera.codPdf, nuovaLetturaGiornaliera.codTipoFornitura, nuovaLetturaGiornaliera.codTipVoceLtu);
        if (retrievedLtuAggr == null) {
            // Non c'è ancora nessun dato aggregato con questo anno/mese/pdf/tipo
            // Ne creo uno nuovo e lo inserisco nel db
            LtuGiornaliereAggregatedDto ltuGiornaliereAggregatedDto =
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
            ltuGiornaliereAggregatedDto.pushLtuGiornalieraRaw(nuovaLetturaGiornaliera);

//            for (Date runningDate = ltuGiornaliereAggregatedDto.getFirstCurveDate(); !runningDate.after(ltuGiornaliereAggregatedDto.getLastCurveDate()); runningDate = DateUtils.addDays(runningDate, 1)) {
//                // TODO codice per creare il nuovo mese se non ci sono aggregati con le stesse chiavi
//                Date finalRunningDate = runningDate;
//                if (ltuGiornaliereAggregatedDto.lettureSingole.stream().noneMatch(ltuSingola -> ltuSingola.getDatLettura().compareTo(finalRunningDate) == 0)) {
//                    LtuGiornaliereLetturaSingolaDto letturaSingola = new LtuGiornaliereLetturaSingolaDto();
//                    BeanUtils.copyProperties(nuovaLetturaGiornaliera.getLetturaSingola().toBuilder().quaLettura(null).datLettura(finalRunningDate).codTipLtuGio("3").build(), letturaSingola);
//                    letturaSingola.storico = new ArrayList<>(List.of(nuovaLetturaGiornaliera.getLetturaSingola().toBuilder().quaLettura(null).datLettura(finalRunningDate).build()));
//                    ltuGiornaliereAggregatedDto.lettureSingole.add(letturaSingola);
//                }
//            }

            ltuGiornaliereAggregatedDto.updateStatistics();
            curveGasService.updateConsumiReali(ltuGiornaliereAggregatedDto);

            mongoTemplate.insert(ltuGiornaliereAggregatedDto, "ltuGiornaliereAggregated");
        } else {
            // C'è già un dato aggregato per questo anno/mese/pdf/tipo
            if (retrievedLtuAggr.lettureSingole.stream().noneMatch(l -> l.datLettura.compareTo(nuovaLetturaGiornaliera.datLettura) == 0)) {
                // Per questo anno/mese/pdf/tipo non c'è una lettura con dtaLettura uguale a quella appena entrata
                // la creo, aggiorno le letture per questo anno/mese/pdf/tipo e salvo
                LtuGiornaliereLetturaSingolaDto singolaDto = new LtuGiornaliereLetturaSingolaDto();
                BeanUtils.copyProperties(nuovaLetturaGiornaliera, singolaDto);
                LtuGiornaliereLetturaSingolaItemDto storicoItem = new LtuGiornaliereLetturaSingolaItemDto();
                BeanUtils.copyProperties(nuovaLetturaGiornaliera, storicoItem);
                singolaDto.storico = new ArrayList<>(List.of(storicoItem));
                retrievedLtuAggr.lettureSingole.add(singolaDto);
                retrievedLtuAggr.updateStatistics();
                repository.save(retrievedLtuAggr);
            } else {
                // Per questo anno/mese/pdf/tipo c'è già una lettura con dtaLettura uguale a quella arrivata
                // Aggiorno i campi vivi, inserisco la lettura arrivata anche nello storico e salvo
                retrievedLtuAggr.pushLtuGiornalieraRaw(nuovaLetturaGiornaliera);
                retrievedLtuAggr.updateStatistics();

                retrievedLtuAggr.setConsumoReale(curveGasService.getConsumoReale(retrievedLtuAggr));
                curveGasService.updateConsumiReali(retrievedLtuAggr);
                repository.save(retrievedLtuAggr);

            }
            log.debug("Insert letturaSingola: {}\n for codPdf {}, anno {}, mese {}", nuovaLetturaGiornaliera.getLetturaSingola(), nuovaLetturaGiornaliera.codPdf, anno, mese);
        }

        curveGasService.interpolateAggregatedLtusForward(nuovaLetturaGiornaliera);
    }
}
