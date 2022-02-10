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
    private Logger log = LoggerFactory.getLogger(this.getClass());
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
            LtuGiornaliereAggregatedDto ltuGiornaliereAggregatedDto = LtuGiornaliereAggregatedDto.builder().codPdf(nuovaLetturaGiornaliera.codPdf).codTipoFornitura(nuovaLetturaGiornaliera.codTipoFornitura).codPdm(nuovaLetturaGiornaliera.codPdm).anno(anno).mese(mese).codTipVoceLtu(nuovaLetturaGiornaliera.codTipVoceLtu).build();
            LtuGiornaliereLetturaSingolaDto letturaSingola = new LtuGiornaliereLetturaSingolaDto();
            BeanUtils.copyProperties(nuovaLetturaGiornaliera.getLetturaSingola(), letturaSingola);
            letturaSingola.setQuaLetturaStimata(letturaSingola.getQuaLettura());
            letturaSingola.storico = new ArrayList<>(Arrays.asList(nuovaLetturaGiornaliera.getLetturaSingola().toBuilder().quaLetturaStimata(letturaSingola.getQuaLettura()).build()));
            ltuGiornaliereAggregatedDto.lettureSingole = new ArrayList<>(Arrays.asList(letturaSingola));

            for (Date runningDate = getFirstDateOfMonth(letturaSingola.datLettura); !runningDate.after(getLastDateOfMonth(letturaSingola.datLettura)); runningDate = DateUtils.addDays(runningDate, 1)) {
                // TODO codice per creare il nuovo mese se non ci sono aggregati con le stesse chiavi
                Date finalRunningDate = runningDate;
                if (!ltuGiornaliereAggregatedDto.lettureSingole.stream().anyMatch(ltuSingola -> ltuSingola.getDatLettura().compareTo(finalRunningDate) == 0)) {
                    System.out.println();
                    letturaSingola = new LtuGiornaliereLetturaSingolaDto();
                    BeanUtils.copyProperties(nuovaLetturaGiornaliera.getLetturaSingola().toBuilder().quaLettura(null).quaLetturaStimata(nuovaLetturaGiornaliera.quaLettura).datLettura(finalRunningDate).build(), letturaSingola);
                    letturaSingola.storico = new ArrayList<>(Arrays.asList(nuovaLetturaGiornaliera.getLetturaSingola().toBuilder().quaLettura(null).quaLetturaStimata(nuovaLetturaGiornaliera.quaLettura).datLettura(finalRunningDate).build()));
                    ltuGiornaliereAggregatedDto.lettureSingole.add(letturaSingola);
                }
            }

            ltuGiornaliereAggregatedDto.setMaxQuaLettura(ltuGiornaliereAggregatedDto.getMaxQuaLettura());
            ltuGiornaliereAggregatedDto.setMinQuaLettura(ltuGiornaliereAggregatedDto.getMinQuaLettura());
//            ltuGiornaliereAggregatedDto.setConsumoReale(curveGasService.getConsumoReale(ltuGiornaliereAggregatedDto));
            curveGasService.updateConsumiReali(ltuGiornaliereAggregatedDto);
            LtuGiornaliereLetturaSingolaDto firstValidLtu = ltuGiornaliereAggregatedDto
                    .lettureSingole
                    .stream()
                    .filter(l -> l.getQuaLettura() != null)
                    .min(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
                    .orElse(LtuGiornaliereLetturaSingolaDto.builder().datLettura(null).build());
            ltuGiornaliereAggregatedDto.setDtaPrimaLetturaValida(firstValidLtu.getDatLettura());
            ltuGiornaliereAggregatedDto.setPrimaLetturaValida(firstValidLtu.getQuaLettura());
            LtuGiornaliereLetturaSingolaDto lastValidLtu = ltuGiornaliereAggregatedDto
                    .lettureSingole
                    .stream()
                    .filter(l -> l.getQuaLettura() != null)
                    .max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
                    .orElse(LtuGiornaliereLetturaSingolaDto.builder().datLettura(null).build());
            ltuGiornaliereAggregatedDto.setDtaUltimaLetturaValida(lastValidLtu.getDatLettura());
            ltuGiornaliereAggregatedDto.setUltimaLetturaValida(lastValidLtu.getQuaLettura());


            mongoTemplate.insert(ltuGiornaliereAggregatedDto, "ltuGiornaliereAggregated");
        } else {
            // C'è già un dato aggregato per questo anno/mese/pdf/tipo
            LtuGiornaliereLetturaSingolaDto foundLtuGiornaliera = retrievedLtuAggr.lettureSingole.stream().filter(l -> l.datLettura.compareTo(nuovaLetturaGiornaliera.datLettura) == 0).findFirst().orElse(null);
            if (foundLtuGiornaliera == null) {
                // Per questo anno/mese/pdf/tipo non c'è una lettura con dtaLettura uguale a quella appena entrata
                // la creo, aggiorno le letture per questo anno/mese/pdf/tipo e salvo
                LtuGiornaliereLetturaSingolaDto singolaDto = new LtuGiornaliereLetturaSingolaDto();
                BeanUtils.copyProperties(nuovaLetturaGiornaliera, singolaDto);
                LtuGiornaliereLetturaSingolaItemDto storicoItem = new LtuGiornaliereLetturaSingolaItemDto();
                BeanUtils.copyProperties(nuovaLetturaGiornaliera, storicoItem);
                singolaDto.storico = new ArrayList<>(Arrays.asList(storicoItem));
                retrievedLtuAggr.lettureSingole.add(singolaDto);
                repository.save(retrievedLtuAggr);
            } else {
                // Per questo anno/mese/pdf/tipo c'è già una lettura con dtaLettura uguale a quella arrivata
                // Aggiorno i campi vivi, inserisco la lettura arrivata anche nello storico e salvo
                LtuGiornaliereLetturaSingolaItemDto storicoItem = new LtuGiornaliereLetturaSingolaItemDto();
                Integer randomIntEstimation = (int) (new Random().nextFloat() * 100);
                BeanUtils.copyProperties(nuovaLetturaGiornaliera.toBuilder().quaLetturaStimata(nuovaLetturaGiornaliera.quaLettura + randomIntEstimation).build(), storicoItem);
                foundLtuGiornaliera.storico.add(storicoItem);
                BeanUtils.copyProperties(nuovaLetturaGiornaliera.toBuilder().quaLetturaStimata(nuovaLetturaGiornaliera.quaLettura + randomIntEstimation).build(), foundLtuGiornaliera);
                retrievedLtuAggr.lettureSingole.removeIf(obj -> obj.datLettura.compareTo(foundLtuGiornaliera.datLettura) == 0);
                retrievedLtuAggr.lettureSingole.add(foundLtuGiornaliera);
                retrievedLtuAggr.setMaxQuaLettura(retrievedLtuAggr.getMaxQuaLettura());
                retrievedLtuAggr.setMinQuaLettura(retrievedLtuAggr.getMinQuaLettura());
                retrievedLtuAggr.setConsumoReale(curveGasService.getConsumoReale(retrievedLtuAggr));
                curveGasService.updateConsumiReali(retrievedLtuAggr);
                LtuGiornaliereLetturaSingolaDto firstValidLtu = retrievedLtuAggr
                        .lettureSingole
                        .stream()
                        .filter(l -> l.getQuaLettura() != null)
                        .min(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
                        .orElse(LtuGiornaliereLetturaSingolaDto.builder().datLettura(null).build());
                retrievedLtuAggr.setDtaPrimaLetturaValida(firstValidLtu.getDatLettura());
                retrievedLtuAggr.setPrimaLetturaValida(firstValidLtu.getQuaLettura());
                LtuGiornaliereLetturaSingolaDto lastValidLtu = retrievedLtuAggr
                        .lettureSingole
                        .stream()
                        .filter(l -> l.getQuaLettura() != null)
                        .max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
                        .orElse(LtuGiornaliereLetturaSingolaDto.builder().datLettura(null).build());
                retrievedLtuAggr.setDtaUltimaLetturaValida(lastValidLtu.getDatLettura());
                retrievedLtuAggr.setUltimaLetturaValida(lastValidLtu.getQuaLettura());
                repository.save(retrievedLtuAggr);

            }
            log.debug("Insert letturaSingola: {}\n for codPdf {}, anno {}, mese {}", nuovaLetturaGiornaliera.getLetturaSingola(), nuovaLetturaGiornaliera.codPdf, anno, mese);
        }
    }
}
