package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Document(collection = "ltuGiornaliereAggregated")

public class LtuGiornaliereAggregatedDto {
    public ArrayList<LtuGiornaliereLetturaSingolaDto> lettureSingole;
    @Id
    String id;
    String codPdf;
    String mese;
    String anno;
    Date firstCurveDate;
    Date lastCurveDate;
    String codTipoFornitura;
    String codPdm;
    String codTipVoceLtu;
    Integer consumoReale;
    Integer minQuaLettura;
    Integer maxQuaLettura;
    Date dtaPrimaLetturaValida;
    Integer primaLetturaValida;
    Date dtaUltimaLetturaValida;
    Integer ultimaLetturaValida;
    public Date lastUpdate;
    LtuGiornaliereStatsDto ltuGiornaliereStatsDto;

    public LtuGiornaliereAggregatedDto pushLtuGiornalieraRaw(LtuGiornaliereRawDto rawLtu) {
        rawLtu.setLastUpdate(new Date());
        if (lettureSingole == null)
            lettureSingole = new ArrayList<>();
        Optional<LtuGiornaliereLetturaSingolaDto> ltuSingola =
                lettureSingole
                        .stream()
                        .filter(ltu -> ltu.datLettura.compareTo(rawLtu.datLettura) == 0)
                        .findFirst();
        if (ltuSingola.isPresent()) {
            // C'è una lettura per questa data
            // push nello storico e inserimento della nuova lettura a livello 0
            LtuGiornaliereLetturaSingolaItemDto ltuToPush = new LtuGiornaliereLetturaSingolaItemDto();
            BeanUtils.copyProperties(
                    rawLtu,
                    ltuToPush
            );
            ltuSingola.get().storico.add(ltuToPush);
            BeanUtils.copyProperties(
                    rawLtu,
                    ltuSingola.get()
            );
        } else {
            // Non c'è una lettura per questa data
            // Aggiungo la data, aggiunto la lettura e ritorno
            LtuGiornaliereLetturaSingolaDto singolaDto = new LtuGiornaliereLetturaSingolaDto();
            BeanUtils.copyProperties(rawLtu, singolaDto);
            singolaDto.storico = new ArrayList<>();
            LtuGiornaliereLetturaSingolaItemDto singolaItemDto = new LtuGiornaliereLetturaSingolaItemDto();
            BeanUtils.copyProperties(rawLtu, singolaItemDto);
            singolaDto.storico.add(singolaItemDto);
            this.lettureSingole.add(singolaDto);
        }
        return this;
    }

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
                .map(LtuGiornaliereLetturaSingolaDto::getQuaLettura).orElse(null);
        return min;
    }

//    public LtuGiornaliereAggregatedDto updateStatistics() {
//        maxQuaLettura = getMaxQuaLettura();
//        minQuaLettura = getMinQuaLettura();
//        LtuGiornaliereLetturaSingolaDto firstValidLtu = lettureSingole
//                .stream()
//                .filter(l -> l.getQuaLettura() != null)
//                .min(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
//                .orElse(LtuGiornaliereLetturaSingolaDto.builder().datLettura(null).build());
//        dtaPrimaLetturaValida = firstValidLtu.getDatLettura();
//        primaLetturaValida = firstValidLtu.getQuaLettura();
//        LtuGiornaliereLetturaSingolaDto lastValidLtu = lettureSingole
//                .stream()
//                .filter(l -> l.getQuaLettura() != null)
//                .max(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura))
//                .orElse(LtuGiornaliereLetturaSingolaDto.builder().datLettura(null).build());
//        dtaUltimaLetturaValida = lastValidLtu.getDatLettura();
//        ultimaLetturaValida = lastValidLtu.getQuaLettura();
//
//        if (ltuGiornaliereStatsDto == null) ltuGiornaliereStatsDto = new LtuGiornaliereStatsDto();
//        ltuGiornaliereStatsDto.numLtuGte25000 = (lettureSingole.isEmpty() ? 0 : lettureSingole.stream().filter(ltu -> ltu.getQuaLettura() != null && ltu.getQuaLettura() >= 25000).collect(Collectors.counting()).intValue());
//        ltuGiornaliereStatsDto.numLtuGte1000Lt25000 = (lettureSingole.isEmpty() ? 0 : lettureSingole.stream().filter(ltu -> ltu.getQuaLettura() != null && ltu.getQuaLettura() >= 1000 && ltu.getQuaLettura() < 25000).collect(Collectors.counting()).intValue());
//        ltuGiornaliereStatsDto.numLtuLt1000 = (lettureSingole.isEmpty() ? 0 : lettureSingole.stream().filter(ltu -> ltu.getQuaLettura() != null && ltu.getQuaLettura() < 1000).collect(Collectors.counting()).intValue());
//
//
//        this.lettureSingole.sort(Comparator.comparing(LtuGiornaliereLetturaSingolaDto::getDatLettura));
//        for (int i = 1; i < lettureSingole.size(); i++) {
//            if (consumoReale != null)
//                lettureSingole.get(i).setConsumoGiornalieroStimato((long) (consumoReale * i / lettureSingole.size()));
//            if (lettureSingole.get(i - 1).getQuaLettura() != null && (lettureSingole.get(i).getQuaLettura() != null)) {
//                lettureSingole.get(i).setConsumoGiornaliero((long) (lettureSingole.get(i).getQuaLettura() - lettureSingole.get(i - 1).getQuaLettura()));
//            }
//        }
//        return this;
//    }

}
