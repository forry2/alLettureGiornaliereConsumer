package com.dxc.curvegas.alletturegiornaliereconsumer.service;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.*;
import com.dxc.curvegas.alletturegiornaliereconsumer.repository.CustomLtuGiornaliereAggregatedRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
public class CurveGasService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    CustomLtuGiornaliereAggregatedRepository repository;
    @Autowired
    private MongoTemplate mongoTemplate;

    public CurveGasService(){
        SimpleModule module = new SimpleModule();
        module.addDeserializer(LtuGiornaliereRawDto.class, new LtuGiornaliereDeserializer());
        objectMapper.registerModule(module);
        module = new SimpleModule();
        module.addDeserializer(Guasto.class, new GuastoDeserializer());
        objectMapper.registerModule(module);
    }

    public Document findLastValidQuaLettura(String codPdf, String codTipoFornitura, String codTipVoceLtu, String anno, String mese) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(new Criteria().and("codPdf").is(codPdf).and("codTipoFornitura").is(codTipoFornitura).and("codTipVoceLtu").is(codTipVoceLtu).and("anno").lte(anno)));
        aggrList.add(sort(Sort.Direction.DESC, "anno").and(Sort.Direction.DESC, "mese"));
        AggregationOperation customAddFieldsOperation = aoc -> {
            Document doc = new Document();
            Document subDoc = new Document();
            subDoc.put("$concat", Arrays.asList("$anno", "$mese"));
            doc.put("annoMeseString", subDoc);
            return new Document("$addFields", doc);
        };
        aggrList.add(customAddFieldsOperation);
        aggrList.add(match(Criteria.where("annoMeseString").lt(anno + mese)));
        aggrList.add(limit(1));
        aggrList.add(unwind("$lettureSingole"));
        aggrList.add(sort(Sort.Direction.DESC, "lettureSingole.datLettura"));
        aggrList.add(match(Criteria.where("lettureSingole.quaLettura").ne(null)));
        aggrList.add(limit(1));

        return mongoTemplate.aggregate(newAggregation(aggrList), "ltuGiornaliereAggregated", Document.class).getUniqueMappedResult();
    }

    public Integer findPreciseDateLtuGiornalieraQuaLettura(String codPdf, String codPdm, String codTipoFornitura, String codTipVoceLtu, Date date) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(
                match(
                        Criteria
                                .where("codPdf").is(codPdf)
                                .and("codPdm").is(codPdm)
                                .and("codTipoFornitura").is(codTipoFornitura)
                                .and("codTipVoceLtu").is(codTipVoceLtu)
                                .and("lettureSingole.datLettura").is(date)
                ));
        aggrList.add(sort(Sort.Direction.DESC, "firstCurveDate"));
        aggrList.add(limit(1));
        aggrList.add(unwind("lettureSingole"));
        aggrList.add(match(Criteria.where("lettureSingole.datLettura").is(date)));
        Document retDoc = mongoTemplate.aggregate(newAggregation(aggrList), "ltuGiornaliereAggregated", Document.class)
                .getUniqueMappedResult();
        if (retDoc == null)
            return null;
        Document lettureSingole = (Document) retDoc.get("lettureSingole");
        if (lettureSingole == null)
            return null;
        return lettureSingole.getInteger("quaLettura");
    }

    public Document findLastValidLetturaSingolaInLastMonth(String codPdf, String codPdm, String codTipoFornitura, String codTipVoceLtu, Date date) {
        // trova l'ultima lettura singola valida (con quaLettura != null) a partire dall'ultimo mese indietro nel tempo
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(
                match(
                        Criteria
                                .where("codPdf").is(codPdf)
                                .and("codPdm").is(codPdm)
                                .and("codTipoFornitura").is(codTipoFornitura)
                                .and("codTipVoceLtu").is(codTipVoceLtu)
                                .and("firstCurveDate").lt(Date.from(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant()))
                ));

        aggrList.add(sort(Sort.Direction.DESC, "firstCurveDate"));
        aggrList.add(limit(1));
        aggrList.add(unwind("lettureSingole"));
        aggrList.add(match(Criteria.where("lettureSingole.quaLettura").ne(null)));
        aggrList.add(sort(Sort.Direction.DESC, "lettureSingole.datLettura"));
        aggrList.add(limit(1));
        return mongoTemplate.aggregate(newAggregation(aggrList), "ltuGiornaliereAggregated", Document.class)
                .getUniqueMappedResult();
    }

    public ObjectId findNextValidAggregatedLtuId(LtuGiornaliereAggregatedDto ltu) {
        ArrayList<AggregationOperation> aggregationOperations = new ArrayList<>();
        aggregationOperations.add(match(Criteria.where("codPdf").is(ltu.getCodPdf()).and("codTipoFornitura").is(ltu.getCodTipoFornitura()).and("codTipVoceLtu").is(ltu.getCodTipVoceLtu()).and("anno").gte(ltu.getAnno())));
        aggregationOperations.add(unwind("$lettureSingole"));
        aggregationOperations.add(sort(Sort.Direction.ASC, "lettureSingole.datLettura"));
        aggregationOperations.add(match(
                Criteria
                        .where("lettureSingole.quaLettura")
                        .ne(null)
                        .and("lettureSingole.datLettura")
                        .gt(ltu.lettureSingole.stream().max(Comparator.comparing(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getDatLettura())).get().getDatLettura())
        ));
        aggregationOperations.add(limit(1));
        aggregationOperations.add(project("_id"));
        Document retIdDocument = mongoTemplate.aggregate(newAggregation(aggregationOperations), "ltuGiornaliereAggregated", Document.class).getUniqueMappedResult();
        if (retIdDocument == null) return null;
        return (ObjectId) retIdDocument.get("_id");
    }

    public void updateConsumoMensile(LtuGiornaliereAggregatedDto aggrLtuCorrente) {
        LtuGiornaliereLetturaSingolaDto lastValidLtuGiornaliere =
                aggrLtuCorrente
                        .getLettureSingole()
                        .stream()
                        .filter(l -> l.getQuaLettura() != null)
                        .max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
                        .orElse(null);
        if (lastValidLtuGiornaliere == null) {
            aggrLtuCorrente.setConsumoReale(null);
            return;
        }
        Document lastValidLtuGiornalierePastMonth =
                findLastValidLetturaSingolaInLastMonth(
                        aggrLtuCorrente.getCodPdf(),
                        aggrLtuCorrente.getCodPdm(),
                        aggrLtuCorrente.getCodTipoFornitura(),
                        aggrLtuCorrente.getCodTipVoceLtu(),
                        aggrLtuCorrente.getFirstCurveDate()
                );
        if (lastValidLtuGiornalierePastMonth == null) {
            aggrLtuCorrente.setConsumoReale(null);
            return;
        }
        aggrLtuCorrente.setConsumoReale(lastValidLtuGiornaliere.getQuaLettura() - ((Document) lastValidLtuGiornalierePastMonth.get("lettureSingole")).getInteger("quaLettura"));
    }

    public Date findLast4DateBackward(LtuGiornaliereRawDto rawDto) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(Criteria.where("codPdf").is(rawDto.getCodPdf()).and("codTipoFornitura").is(rawDto.getCodTipoFornitura()).and("codPdm").is(rawDto.getCodPdm()).and("codTipVoceLtu").is(rawDto.getCodTipVoceLtu()).and("lettureSingole.codTipLtuGio").is("4").and("lettureSingole.datLettura").lt(rawDto.getDatLettura())));
        aggrList.add(unwind("$lettureSingole"));
        aggrList.add(sort(Sort.Direction.DESC, "lettureSingole.datLettura"));
        aggrList.add(match(Criteria.where("lettureSingole.codTipLtuGio").is("4").and("lettureSingole.datLettura").lt(rawDto.getDatLettura())));
        aggrList.add(limit(1));
        aggrList.add(project().and("$lettureSingole.datLettura").as("last4Date").andExclude("_id"));
        AggregationResults<Document> results = mongoTemplate.aggregate(newAggregation(aggrList), "ltuGiornaliereAggregated", Document.class);
        if (results.getMappedResults().isEmpty()) return null;
        return results.getUniqueMappedResult().getDate("last4Date");
    }

    public Date findNext4DateForward(LtuGiornaliereRawDto rawDto) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(Criteria.where("codPdf").is(rawDto.getCodPdf()).and("codTipoFornitura").is(rawDto.getCodTipoFornitura()).and("codPdm").is(rawDto.getCodPdm()).and("codTipVoceLtu").is(rawDto.getCodTipVoceLtu()).and("lettureSingole.codTipLtuGio").is("4").and("lettureSingole.datLettura").gt(rawDto.getDatLettura())));
        aggrList.add(unwind("$lettureSingole"));
        aggrList.add(sort(Sort.Direction.ASC, "lettureSingole.datLettura"));
        aggrList.add(match(Criteria.where("lettureSingole.codTipLtuGio").is("4").and("lettureSingole.datLettura").gt(rawDto.getDatLettura())));
        aggrList.add(limit(1));
        aggrList.add(project().and("$lettureSingole.datLettura").as("next4Date").andExclude("_id"));
        AggregationResults<Document> results = mongoTemplate.aggregate(newAggregation(aggrList), "ltuGiornaliereAggregated", Document.class);
        if (results.getMappedResults().isEmpty()) return null;
        return results.getUniqueMappedResult().getDate("next4Date");
    }

    public ArrayList<LtuGiornaliereAggregatedDto> getInterpolationLtuGiornaliereAggregatedList(
            String codPdf,
            String codPdm,
            String codTipoFornitura,
            String codTipVoceLtu,
            Date interpolationStartDate,
            Date interpolationEndDate
    ) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        Date lowerFirstDateOfMonth = Date.from(interpolationStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant());
        Date higherFirstDateOfMonth = Date.from(interpolationEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant());
        Set<Date> firstDates = new HashSet<>();
        Date runningDate;
        for (
                runningDate = lowerFirstDateOfMonth;
                runningDate.compareTo(higherFirstDateOfMonth) <= 0;
                runningDate = DateUtils.addMonths(runningDate, 1)
        ) {
            firstDates.add(runningDate);
        }
        aggrList.add(match(Criteria
                .where("codPdf").is(codPdf)
                .and("codPdm").is(codPdm)
                .and("codTipoFornitura").is(codTipoFornitura)
                .and("codTipVoceLtu").is(codTipVoceLtu)
                .and("firstCurveDate").in(firstDates)
        ));
        aggrList.add(sort(Sort.Direction.ASC, "dtaPrimaLetturaValida"));

        return new ArrayList<>(mongoTemplate.aggregate(newAggregation(aggrList), "ltuGiornaliereAggregated", LtuGiornaliereAggregatedDto.class).getMappedResults());
    }

    public void interpolateAggregatedLtus(LtuGiornaliereRawDto rawDto, InterpolationDirection direction) {
        Date interpolationStartDate = null;
        Date interpolationEndDate = null;
        LtuGiornaliereAggregatedDto currentAggrLtu = getInterpolationLtuGiornaliereAggregatedList(
                rawDto.getCodPdf(),
                rawDto.getCodPdm(),
                rawDto.getCodTipoFornitura(),
                rawDto.getCodTipVoceLtu(),
                rawDto.datLettura,
                rawDto.datLettura
        ).get(0);

        switch (direction) {
            case FORWARD:
                interpolationStartDate = rawDto.getDatLettura();
                interpolationEndDate = findNext4DateForward(rawDto);
                break;
            case BACKWARD:
                interpolationStartDate = findLast4DateBackward(rawDto);
                interpolationEndDate = rawDto.getDatLettura();
                break;
        }

        if (
                (direction == InterpolationDirection.FORWARD && interpolationEndDate == null)
                        ||
                        (direction == InterpolationDirection.BACKWARD && interpolationStartDate == null)
        ) {
            // Non ci sono letture 4 dalla lettura di inserimento in avanti (FORWARD)
            // o prima della lettura di inserimento (BACKWARD)
            int runningDaysDelta = (direction == InterpolationDirection.FORWARD ? 1 : -1);
            // Stima a sbalzo
            for (
                    Date runningDate = DateUtils.addDays(rawDto.datLettura, runningDaysDelta);
                    !runningDate.after(currentAggrLtu.getLastCurveDate()) && !runningDate.before(currentAggrLtu.getFirstCurveDate());
                    runningDate = DateUtils.addDays(runningDate, runningDaysDelta)
            ) {
                currentAggrLtu.pushLtuGiornalieraRaw(
                        rawDto.toBuilder().datLettura(runningDate).codTipLtuGio("3").quaLettura(rawDto.getQuaLettura()).codTipoFonteLtuGio("6").build()
                );
            }
            currentAggrLtu.lettureSingole.sort(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura));
            updateStatistics(currentAggrLtu);
            repository.save(currentAggrLtu);
            return;
        }

        // Recupero tutti gli aggregati oggetto di interpolazione
        ArrayList<LtuGiornaliereAggregatedDto> ltuAggrInterpolationList =
                getInterpolationLtuGiornaliereAggregatedList(
                        rawDto.getCodPdf(),
                        rawDto.getCodPdm(),
                        rawDto.getCodTipoFornitura(),
                        rawDto.getCodTipVoceLtu(),
                        interpolationStartDate,
                        interpolationEndDate
                );

        long deltaDays = interpolationStartDate
                .toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate()
                .until(interpolationEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), ChronoUnit.DAYS);
        if (deltaDays > 1) {

            for (
                    Date runningDate = Date.from(interpolationStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant());
                    runningDate.before(Date.from(interpolationEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant()));
                    runningDate = DateUtils.addMonths(runningDate, 1)
            ) {
                Date finalRunningDate = runningDate;
                Calendar cal = Calendar.getInstance();
                cal.setTime(runningDate);
                if (ltuAggrInterpolationList.stream().noneMatch(l -> l.getFirstCurveDate().compareTo(finalRunningDate) == 0))
                    ltuAggrInterpolationList.add(
                            LtuGiornaliereAggregatedDto
                                    .builder()
                                    .codPdf(ltuAggrInterpolationList.get(0).getCodPdf())
                                    .mese(String.format("%02d", cal.get(Calendar.MONTH) + 1))
                                    .anno(String.format("%04d", cal.get(Calendar.YEAR)))
                                    .codTipoFornitura(ltuAggrInterpolationList.get(0).getCodTipoFornitura())
                                    .codPdm(ltuAggrInterpolationList.get(0).getCodPdm())
                                    .codTipVoceLtu(ltuAggrInterpolationList.get(0).getCodTipVoceLtu())
                                    .firstCurveDate(getFirstDateOfMonth(runningDate))
                                    .lastCurveDate(getLastDateOfMonth(runningDate))
                                    .build()
                    );
            }
            ltuAggrInterpolationList.sort(Comparator.comparing(LtuGiornaliereAggregatedDto::getFirstCurveDate));

            Date finalInterpolationStartDate = interpolationStartDate;
            LtuGiornaliereLetturaSingolaDto firstInterpolationLtuSingola =
                    getInterpolationLtuGiornaliereAggregatedList(
                            rawDto.getCodPdf(),
                            rawDto.getCodPdm(),
                            rawDto.getCodTipoFornitura(),
                            rawDto.getCodTipVoceLtu(),
                            interpolationStartDate,
                            interpolationStartDate
                    ).get(0).lettureSingole.stream().filter(ltu -> ltu.datLettura.compareTo(finalInterpolationStartDate) == 0).findFirst().get();
            Date finalInterpolationEndDate = interpolationEndDate;
            LtuGiornaliereLetturaSingolaDto lastInterpolationLtuSingola =
                    getInterpolationLtuGiornaliereAggregatedList(
                            rawDto.getCodPdf(),
                            rawDto.getCodPdm(),
                            rawDto.getCodTipoFornitura(),
                            rawDto.getCodTipVoceLtu(),
                            interpolationEndDate,
                            interpolationEndDate
                    ).get(0).lettureSingole.stream().filter(ltu -> ltu.datLettura.compareTo(finalInterpolationEndDate) == 0).findFirst().get();
            int ltuDelta = lastInterpolationLtuSingola.getQuaLettura() - firstInterpolationLtuSingola.getQuaLettura();
            Date runningDate;
            ArrayList<LtuGiornaliereRawDto> interpolationsLtu = new ArrayList<>();
            int interpolationIndex = 1;
            for (
                    runningDate = DateUtils.addDays(interpolationStartDate, 1);
                    runningDate.before(interpolationEndDate);
                    runningDate = DateUtils.addDays(runningDate, 1)
            ) {
                // Interpolazione dati
                // TODO per ora, implementato algoritmo di interpolazione lineare
                LtuGiornaliereRawDto newRawDto = new LtuGiornaliereRawDto();
                BeanUtils.copyProperties(firstInterpolationLtuSingola, newRawDto);
                Integer interpolatedQuaLettura = Math.toIntExact(firstInterpolationLtuSingola.getQuaLettura() + ltuDelta * interpolationIndex / deltaDays);
                newRawDto = newRawDto.toBuilder().datLettura(runningDate).codTipLtuGio("3").codTipoFonteLtuGio("7").quaLettura(interpolatedQuaLettura).build();
                interpolationsLtu.add(newRawDto);
                interpolationIndex++;
            }

            interpolationsLtu.forEach(
                    rawDto1 -> ltuAggrInterpolationList
                            .stream()
                            .filter(aggr -> !aggr.getFirstCurveDate().after(rawDto1.datLettura) && !aggr.getLastCurveDate().before(rawDto1.datLettura))
                            .findFirst()
                            .get()
                            .pushLtuGiornalieraRaw(rawDto1)
            );
        }
        ltuAggrInterpolationList.forEach(
                aggr -> {
                    updateStatistics(aggr);
                }
        );
        repository.saveAll(ltuAggrInterpolationList);
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

    public void updateStatistics(LtuGiornaliereAggregatedDto ltuGiornaliereAggregatedDto) {
        ltuGiornaliereAggregatedDto.setMaxQuaLettura(ltuGiornaliereAggregatedDto.lettureSingole.stream().filter(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getQuaLettura() != null).max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getQuaLettura)).map(LtuGiornaliereLetturaSingolaDto::getQuaLettura).orElse(null));
        ltuGiornaliereAggregatedDto.setMinQuaLettura(ltuGiornaliereAggregatedDto.lettureSingole.stream().filter(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getQuaLettura() != null).min(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getQuaLettura)).map(LtuGiornaliereLetturaSingolaDto::getQuaLettura).orElse(null));
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

        if (ltuGiornaliereAggregatedDto.getLtuGiornaliereStatsDto() == null)
            ltuGiornaliereAggregatedDto.setLtuGiornaliereStatsDto(new LtuGiornaliereStatsDto());
        ltuGiornaliereAggregatedDto.getLtuGiornaliereStatsDto().setNumLtuGte25000(ltuGiornaliereAggregatedDto.lettureSingole.isEmpty() ? 0 : ltuGiornaliereAggregatedDto.lettureSingole.stream().filter(ltu -> ltu.getQuaLettura() != null && ltu.getQuaLettura() >= 25000).collect(Collectors.counting()).intValue());
        ltuGiornaliereAggregatedDto.getLtuGiornaliereStatsDto().setNumLtuGte1000Lt25000(ltuGiornaliereAggregatedDto.lettureSingole.isEmpty() ? 0 : ltuGiornaliereAggregatedDto.lettureSingole.stream().filter(ltu -> ltu.getQuaLettura() != null && ltu.getQuaLettura() >= 1000 && ltu.getQuaLettura() < 25000).collect(Collectors.counting()).intValue());
        ltuGiornaliereAggregatedDto.getLtuGiornaliereStatsDto().setNumLtuLt1000(ltuGiornaliereAggregatedDto.lettureSingole.isEmpty() ? 0 : ltuGiornaliereAggregatedDto.lettureSingole.stream().filter(ltu -> ltu.getQuaLettura() != null && ltu.getQuaLettura() < 1000).collect(Collectors.counting()).intValue());


        ltuGiornaliereAggregatedDto.lettureSingole.sort(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura));
        updateConsumoMensile(ltuGiornaliereAggregatedDto);
        for (int i = 0; i < ltuGiornaliereAggregatedDto.lettureSingole.size(); i++) {
            if (ltuGiornaliereAggregatedDto.getConsumoReale() != null)
                ltuGiornaliereAggregatedDto.lettureSingole.get(i).setConsumoGiornalieroStimato((long) (ltuGiornaliereAggregatedDto.getConsumoReale() / ltuGiornaliereAggregatedDto.lettureSingole.size()));
            else
                ltuGiornaliereAggregatedDto.lettureSingole.get(i).setConsumoGiornalieroStimato(null);

            if (i == 0 && ltuGiornaliereAggregatedDto.lettureSingole.get(0).getQuaLettura() != null) {
                Integer yestedayQuaLettura = Optional.ofNullable(findPreciseDateLtuGiornalieraQuaLettura(
                        ltuGiornaliereAggregatedDto.getCodPdf(),
                        ltuGiornaliereAggregatedDto.getCodPdm(),
                        ltuGiornaliereAggregatedDto.getCodTipoFornitura(),
                        ltuGiornaliereAggregatedDto.getCodTipVoceLtu(),
                        DateUtils.addDays(ltuGiornaliereAggregatedDto.getFirstCurveDate(), -1)
                )).orElse(null);
                if (yestedayQuaLettura == null)
                    ltuGiornaliereAggregatedDto.lettureSingole.get(0).setConsumoGiornaliero(null);
                else
                    ltuGiornaliereAggregatedDto.lettureSingole.get(0).setConsumoGiornaliero(
                            (long) (ltuGiornaliereAggregatedDto.lettureSingole.get(0).getQuaLettura() - yestedayQuaLettura)
                    );
            } else if (i == 0) {
                ltuGiornaliereAggregatedDto.lettureSingole.get(0).setConsumoGiornaliero(null);
            } else if (i > 0) {
                if (ltuGiornaliereAggregatedDto.lettureSingole.get(i - 1).getQuaLettura() != null && ltuGiornaliereAggregatedDto.lettureSingole.get(i).getQuaLettura() != null)
                    ltuGiornaliereAggregatedDto.lettureSingole.get(i).setConsumoGiornaliero((long) (ltuGiornaliereAggregatedDto.lettureSingole.get(i).getQuaLettura() - ltuGiornaliereAggregatedDto.lettureSingole.get(i - 1).getQuaLettura()));
                else
                    ltuGiornaliereAggregatedDto.lettureSingole.get(i).setConsumoGiornaliero(null);
            }
        }
    }

    public void manageLtuGiornalieraMessage(String content) throws JsonProcessingException {
        long startTime = new Date().getTime();
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
                            .lastUpdate(new Date())
                            .build();
            currentContentDateLtuAggr.pushLtuGiornalieraRaw(nuovaLetturaGiornaliera);

            for (Date runningDate = currentContentDateLtuAggr.getFirstCurveDate(); !runningDate.after(currentContentDateLtuAggr.getLastCurveDate()); runningDate = DateUtils.addDays(runningDate, 1)) {
                // TODO codice per creare il nuovo mese se non ci sono aggregati con le stesse chiavi
                Date finalRunningDate = runningDate;
                if (currentContentDateLtuAggr.lettureSingole.stream().noneMatch(ltuSingola -> ltuSingola.getDatLettura().compareTo(finalRunningDate) == 0)) {
                    LtuGiornaliereRawDto letturaSingola = LtuGiornaliereRawDto.builder().datLettura(finalRunningDate).lastUpdate(currentContentDateLtuAggr.getLastUpdate()).build();
                    currentContentDateLtuAggr.pushLtuGiornalieraRaw(letturaSingola);
                }
            }
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
                updateStatistics(currentContentDateLtuAggr);
                repository.save(currentContentDateLtuAggr);
            } else {
                // Per questo anno/mese/pdf/tipo c'è già una lettura con dtaLettura uguale a quella arrivata
                // Aggiorno i campi vivi, inserisco la lettura arrivata anche nello storico e salvo
                currentContentDateLtuAggr.pushLtuGiornalieraRaw(nuovaLetturaGiornaliera);
                updateStatistics(currentContentDateLtuAggr);
                repository.save(currentContentDateLtuAggr);

            }
            log.debug("Insert letturaSingola: {}\n for codPdf {}, anno {}, mese {}", nuovaLetturaGiornaliera.getLetturaSingola(), nuovaLetturaGiornaliera.codPdf, anno, mese);
        }
        if (nuovaLetturaGiornaliera.getCodTipLtuGio().equals("4")) {
            interpolateAggregatedLtus(nuovaLetturaGiornaliera, InterpolationDirection.FORWARD);
            interpolateAggregatedLtus(nuovaLetturaGiornaliera, InterpolationDirection.BACKWARD);
        }
        updateStatistics(currentContentDateLtuAggr);
        log.debug("Message processed in {} ms", new Date().getTime() - startTime);
    }

    public void manageGuastoMessage(String content) throws JsonProcessingException {
        Guasto guasto = objectMapper.readValue(content, Guasto.class);
        log.debug("Received failure message: {}", guasto.toString());
        ArrayList<LtuGiornaliereAggregatedDto> guastoLtuGiornaliereAggrList = getInterpolationLtuGiornaliereAggregatedList(
                guasto.getCodPdf(),
                guasto.getCodPdm(),
                guasto.getCodTipoFornitura(),
                "46",
                guasto.getDatInoGuasto(),
                guasto.getDatFinGuasto()
        );
        for (LtuGiornaliereAggregatedDto aggr : guastoLtuGiornaliereAggrList){
            for (LtuGiornaliereLetturaSingolaDto singola : aggr.getLettureSingole()){
                if (!singola.datLettura.after(guasto.getDatInoGuasto()) && !singola.datLettura.before(guasto.getDatFinGuasto())){
                    LtuGiornaliereRawDto rawDto = new LtuGiornaliereRawDto();
                    BeanUtils.copyProperties(singola, rawDto);
                    aggr.pushLtuGiornalieraRaw(rawDto);
                }
            }
        }
    }
}
