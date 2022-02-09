package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Document(collection = "ltuGiornaliereRaw")

public class LtuGiornaliereRawDto {
    public String codPdf;
    public String codTipoFornitura;
    public String codPdm;
    public Date datLettura;
    public String codTipVoceLtu;
    public String numMtcAppar;
    public String codTipLtuGio;
    public Integer quaLettura;
    public Integer quaLetturaStimata;
    public Date datLtuPrecedente;
    public Integer quaLtuPrecedente;
    public String codFlgValida;
    public String codFlgRettificata;
    public String codTipoFonteLtuGio;
    public String codFlgQuadrata;
    public String codAnomalia;
    public Date datAcquisizioneLtu;
    public Date datPubblicazioneLtu;
    public Integer quaLtuPublic;
    public Date datCreazioneRec;
    public Date datUltAggRec;
    public String codOperatore;
    public String numMtcApparNew;
    public String codTipLtuGioNew;
    public Integer quaLetturaNew;
    public Integer quaLtuPrdNew;
    public Integer quaLtuScsNew;
    public String codTipoFonteLtuGioNew;
    public String codAnomaliaNew;
    public String codTipoStatoFinNew;
    public String codFlgRetPbl;
    public Date datForzatura;
    public String codFlgForzata;

    public LtuGiornaliereLetturaSingolaItemDto getLetturaSingola(){
        return LtuGiornaliereLetturaSingolaItemDto
                .builder()
                .datLettura(datLettura)
//                .codTipVoceLtu(codTipVoceLtu)
                .numMtcAppar(numMtcAppar)
                .codTipLtuGio(codTipLtuGio)
                .quaLettura(quaLettura)
                .quaLetturaStimata(quaLetturaStimata)
                .datLtuPrecedente(datLtuPrecedente)
                .quaLtuPrecedente(quaLtuPrecedente)
                .codFlgValida(codFlgValida)
                .codFlgRettificata(codFlgRettificata)
                .codTipoFonteLtuGio(codTipoFonteLtuGio)
                .codFlgQuadrata(codFlgQuadrata)
                .codAnomalia(codAnomalia)
                .datAcquisizioneLtu(datAcquisizioneLtu)
                .datPubblicazioneLtu(datPubblicazioneLtu)
                .quaLtuPublic(quaLtuPublic)
                .datCreazioneRec(datCreazioneRec)
                .datUltAggRec(datUltAggRec)
                .codOperatore(codOperatore)
                .numMtcApparNew(numMtcApparNew)
                .codTipLtuGioNew(codTipLtuGioNew)
                .quaLetturaNew(quaLetturaNew)
                .quaLtuPrdNew(quaLtuPrdNew)
                .quaLtuScsNew(quaLtuScsNew)
                .codTipoFonteLtuGioNew(codTipoFonteLtuGioNew)
                .codAnomaliaNew(codAnomaliaNew)
                .codTipoStatoFinNew(codTipoStatoFinNew)
                .codFlgRetPbl(codFlgRetPbl)
                .datForzatura(datForzatura)
                .codFlgForzata(codFlgForzata)
                .build();
    }
}
