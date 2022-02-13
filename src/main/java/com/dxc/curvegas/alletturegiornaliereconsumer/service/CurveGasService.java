package com.dxc.curvegas.alletturegiornaliereconsumer.service;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereAggregatedDto;
import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereLetturaSingolaDto;
import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereRawDto;
import com.dxc.curvegas.alletturegiornaliereconsumer.repository.CustomLtuGiornaliereAggregatedRepository;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
public class CurveGasService {
    @Autowired
    CustomLtuGiornaliereAggregatedRepository repository;
    @Autowired
    private MongoTemplate mongoTemplate;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public Document findLastValidQuaLettura(String codPdf, String codTipoFornitura, String codTipVoceLtu, String anno, String mese) {
        Document retDoc = null;
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(new Criteria().and("codPdf").is(codPdf).and("codTipoFornitura").is(codTipoFornitura).and("codTipVoceLtu").is(codTipVoceLtu).and("anno").lte(anno)));
        aggrList.add(sort(Sort.Direction.DESC, "anno").and(Sort.Direction.DESC, "mese"));
        AggregationOperation customAddFieldsOperation = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext aoc) {
                Document doc = new Document();
                Document subDoc = new Document();
                subDoc.put("$concat", Arrays.asList("$anno", "$mese"));
                doc.put("annoMeseString", subDoc);
                return new Document("$addFields", doc);
            }
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

    public ObjectId findNextValidAggregatedLtuId(LtuGiornaliereAggregatedDto ltu) {
        ArrayList<AggregationOperation> aggregationOperations = new ArrayList<>();
        ObjectId retId = null;

        aggregationOperations.add(match(Criteria.where("codPdf").is(ltu.getCodPdf()).and("codTipoFornitura").is(ltu.getCodTipoFornitura()).and("codTipVoceLtu").is(ltu.getCodTipVoceLtu()).and("anno").gte(ltu.getAnno())));
        aggregationOperations.add(unwind("$lettureSingole"));
        aggregationOperations.add(sort(Sort.Direction.ASC, "lettureSingole.datLettura"));
        aggregationOperations.add(match(Criteria.where("lettureSingole.quaLettura").ne(null).and("lettureSingole.datLettura").gt(ltu.lettureSingole.stream().max(Comparator.comparing(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getDatLettura())).get().getDatLettura())));
        aggregationOperations.add(limit(1));
        aggregationOperations.add(project("_id"));
        Document retIdDocument = mongoTemplate.aggregate(newAggregation(aggregationOperations), "ltuGiornaliereAggregated", Document.class).getUniqueMappedResult();
        if (retIdDocument == null) return null;
        return (ObjectId) retIdDocument.get("_id");
    }

    public Integer getConsumoReale(LtuGiornaliereAggregatedDto ltu) {
        Document lastValidLtu = findLastValidQuaLettura(ltu.getCodPdf(), ltu.getCodTipoFornitura(), ltu.getCodTipVoceLtu(), ltu.getAnno(), ltu.getMese());
        if (lastValidLtu == null) return null;

        Integer currLastQuaLettura = ltu.lettureSingole.stream().filter(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getQuaLettura() != null).max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura)).map(LtuGiornaliereLetturaSingolaDto::getQuaLettura).orElse(null);
        if (currLastQuaLettura == null) return null;
        return currLastQuaLettura - (Integer) ((Document) lastValidLtu.get("lettureSingole")).get("quaLettura");
    }

    public void updateConsumiReali(LtuGiornaliereAggregatedDto aggrLtuCorrente) {
        aggrLtuCorrente.setConsumoReale(getConsumoReale(aggrLtuCorrente));
        ObjectId nextValidAggregatedLtuId = findNextValidAggregatedLtuId(aggrLtuCorrente);
        if (nextValidAggregatedLtuId == null) return;
        Optional<LtuGiornaliereAggregatedDto> nextValidAggregatedLtuOpt = repository.findById(nextValidAggregatedLtuId.toString());
        if (nextValidAggregatedLtuOpt.isEmpty()) {
            log.warn("No valid aggregated found next to the current to be updated");
            return;
        }
        LtuGiornaliereAggregatedDto nextValidAggregatedLtu = nextValidAggregatedLtuOpt.get();
        nextValidAggregatedLtu.setConsumoReale(nextValidAggregatedLtu.getMaxQuaLettura() - aggrLtuCorrente.getMaxQuaLettura());
        repository.save(nextValidAggregatedLtu);
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

    public void interpolateAggregatedLtusForward(LtuGiornaliereRawDto rawDto){
        Date interpolationStartDate = rawDto.getDatLettura();
        Date interpolationEndDate = findNext4DateForward(rawDto);
        if (interpolationEndDate == null){
            // Non ci sono letture 4 dalla lettura di inserimento in avanti
            // Caso limite ingestion: riempire di 3 fino a fine mese
            LtuGiornaliereAggregatedDto currentAggrLtu = repository.getLtuGiornaliereAggregatedDtoByCodPdfAndCodPdmAndCodTipoFornituraAndCodTipVoceLtuAndFirstCurveDateLessThanEqualAndLastCurveDateGreaterThanEqualOrderByFirstCurveDate(
                    rawDto.getCodPdf(),
                    rawDto.getCodPdm(),
                    rawDto.getCodTipoFornitura(),
                    rawDto.getCodTipVoceLtu(),
                    rawDto.datLettura,
                    rawDto.datLettura
            ).get(0);
            for (
                    Date runningDate = DateUtils.addDays(rawDto.datLettura, 1);
                    runningDate.compareTo(currentAggrLtu.getLastCurveDate()) <= 0;
                    runningDate = DateUtils.addDays(runningDate, 1)
            ){
                currentAggrLtu.pushLtuGiornalieraRaw(
                        rawDto.toBuilder().datLettura(runningDate).codTipLtuGio("3").build()
                );
            }
            currentAggrLtu.updateStatistics();
            repository.save(currentAggrLtu);
            return;
        }

        ArrayList<LtuGiornaliereAggregatedDto> ltuAggrInterpolationList =
                repository.getLtuGiornaliereAggregatedDtoByCodPdfAndCodPdmAndCodTipoFornituraAndCodTipVoceLtuAndFirstCurveDateLessThanEqualAndLastCurveDateGreaterThanEqualOrderByFirstCurveDate(
                        rawDto.getCodPdf(),
                        rawDto.getCodPdm(),
                        rawDto.getCodTipoFornitura(),
                        rawDto.getCodTipVoceLtu(),
                        interpolationStartDate,
                        interpolationStartDate

                );
        Integer letturaSingolaEndDate =
                repository.getLtuGiornaliereAggregatedDtoByCodPdfAndCodPdmAndCodTipoFornituraAndCodTipVoceLtuAndFirstCurveDateLessThanEqualAndLastCurveDateGreaterThanEqualOrderByFirstCurveDate(
                        rawDto.getCodPdf(),
                        rawDto.getCodPdm(),
                        rawDto.getCodTipoFornitura(),
                        rawDto.getCodTipVoceLtu(),
                        interpolationEndDate,
                        interpolationEndDate
                )
                        .get(0)
                        .getLettureSingole()
                        .stream()
                        .filter(ltu -> ltu.datLettura.compareTo(interpolationEndDate)== 0)
                        .findFirst()
                        .get()
                        .getQuaLettura();

        //TODO completare il codice
    }


}
