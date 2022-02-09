package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Document(collection = "ltuGiornaliereAggregated")

public class LtuGiornaliereAggregatedDto {
    @Id
    public String id;
    public String codPdf;
    public String mese;
    public String anno;
    public String codTipoFornitura;
    public String codPdm;
    public String codTipVoceLtu;
    public ArrayList<LtuGiornaliereLetturaSingolaDto> lettureSingole;
}
