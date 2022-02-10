package com.dxc.curvegas.alletturegiornaliereconsumer.repository;

import com.dxc.curvegas.alletturegiornaliereconsumer.model.LtuGiornaliereAggregatedDto;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface CustomLtuGiornaliereAggregatedRepository extends MongoRepository<LtuGiornaliereAggregatedDto, String> {

    LtuGiornaliereAggregatedDto getLtuGiornaliereAggregatedDtoByAnnoAndMeseAndCodPdfAndCodTipoFornituraAndCodTipVoceLtu(String anno, String mese, String codPdf, String codTipoFornitura, String codTipVoceLtu);
}
