package com.dxc.curvegas.alletturegiornaliereconsumer.service;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereAggregatedDto;
import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereLetturaSingolaDto;
import com.dxc.curvegas.alletturegiornaliereconsumer.repository.CustomLtuGiornaliereAggregatedRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
public class CurveGasService {
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    CustomLtuGiornaliereAggregatedRepository repository;
    private Logger log = LoggerFactory.getLogger(this.getClass());

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

        aggregationOperations.add(match(Criteria.where("codPdf").is(ltu.codPdf).and("codTipoFornitura").is(ltu.codTipoFornitura).and("codTipVoceLtu").is(ltu.codTipVoceLtu).and("anno").gte(ltu.anno)));
        aggregationOperations.add(unwind("$lettureSingole"));
        aggregationOperations.add(sort(Sort.Direction.ASC, "lettureSingole.datLettura"));
        aggregationOperations.add(match(
                Criteria
                        .where("lettureSingole.quaLettura")
                        .ne(null)
                        .and("lettureSingole.datLettura")
                        .gt(
                                ltu.lettureSingole
                                        .stream()
                                        .max(Comparator.comparing(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getDatLettura()))
                                        .get()
                                        .getDatLettura()
                        )
        ));
        aggregationOperations.add(limit(1));
        aggregationOperations.add(project("_id"));
        Document retIdDocument = mongoTemplate.aggregate(newAggregation(aggregationOperations), "ltuGiornaliereAggregated", Document.class).getUniqueMappedResult();
        if (retIdDocument == null)
            return null;
        return (ObjectId) retIdDocument.get("_id");
    }

    public Integer getConsumoReale(LtuGiornaliereAggregatedDto ltu) {
        Document lastValidLtu = findLastValidQuaLettura(ltu.codPdf, ltu.codTipoFornitura, ltu.codTipVoceLtu, ltu.anno, ltu.mese);
        if (lastValidLtu == null) return null;

        Integer currLastQuaLettura = ltu.lettureSingole.stream().filter(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getQuaLettura() != null).max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura)).map(LtuGiornaliereLetturaSingolaDto::getQuaLettura).orElse(null);
        if (currLastQuaLettura == null) return null;
        return currLastQuaLettura - (Integer) ((Document) lastValidLtu.get("lettureSingole")).get("quaLettura");
    }

    public void updateConsumiReali(LtuGiornaliereAggregatedDto aggrLtuCorrente) {
        aggrLtuCorrente.setConsumoReale(getConsumoReale(aggrLtuCorrente));
        ObjectId nextValidAggregatedLtuId = findNextValidAggregatedLtuId(aggrLtuCorrente);
        if (nextValidAggregatedLtuId == null)
            return;
        Optional<LtuGiornaliereAggregatedDto> nextValidAggregatedLtuOpt = repository.findById(nextValidAggregatedLtuId.toString());
        if (nextValidAggregatedLtuOpt.isEmpty()){
            log.warn("No valid aggregated found next to the current to be updated");
            return;
        }
        LtuGiornaliereAggregatedDto nextValidAggregatedLtu = nextValidAggregatedLtuOpt.get();
        nextValidAggregatedLtu.setConsumoReale(nextValidAggregatedLtu.getMaxQuaLettura() - aggrLtuCorrente.getMaxQuaLettura());
        repository.save(nextValidAggregatedLtu);
    }
}
