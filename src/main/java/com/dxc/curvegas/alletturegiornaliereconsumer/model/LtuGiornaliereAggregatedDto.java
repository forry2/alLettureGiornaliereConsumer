package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;

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
    public Integer consumoReale;
    public Integer minQuaLettura;
    public Integer maxQuaLettura;
    public Date dtaPrimaLetturaValida;
    public Integer primaLetturaValida;
    public Date dtaUltimaLetturaValida;
    public Integer ultimaLetturaValida;
    public ArrayList<LtuGiornaliereLetturaSingolaDto> lettureSingole;

    public Integer getMaxQuaLettura() {
        Integer max = lettureSingole
                .stream()
                .filter(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getQuaLettura() != null)
                .max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getQuaLettura))
                .map(LtuGiornaliereLetturaSingolaDto::getQuaLettura)
                .orElse(null);
        return max;
    }

    public Integer getMinQuaLettura() {
        Integer min = lettureSingole
                .stream()
                .filter(ltuGiornaliereLetturaSingolaDto -> ltuGiornaliereLetturaSingolaDto.getQuaLettura() != null)
                .min(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getQuaLettura))
                .map(LtuGiornaliereLetturaSingolaDto::getQuaLettura)
                .orElse(null);
        return min;
    }

}
