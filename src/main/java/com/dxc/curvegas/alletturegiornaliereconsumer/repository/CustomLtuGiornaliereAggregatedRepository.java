package com.dxc.curvegas.alletturegiornaliereconsumer.repository;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereAggregatedDto;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Date;
@Repository
public interface CustomLtuGiornaliereAggregatedRepository extends MongoRepository<LtuGiornaliereAggregatedDto, String>
{
    LtuGiornaliereAggregatedDto getLtuGiornaliereAggregatedDtoByAnnoAndMeseAndCodPdfAndCodTipoFornituraAndCodTipVoceLtu(String anno, String mese, String codPdf, String codTipoFornitura, String codTipVoceLtu);

    ArrayList<LtuGiornaliereAggregatedDto> getLtuGiornaliereAggregatedDtoByCodPdfAndCodPdmAndCodTipoFornituraAndCodTipVoceLtuAndFirstCurveDateLessThanEqualAndLastCurveDateGreaterThanEqualOrderByFirstCurveDate(
            String codPdf, String codPdm, String codTipoFornitura, String codTipVoceLtu, Date firstCurveDateBefore, Date lastCurveDateAfter
    );
}
