package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LtuGiornaliereDeserializer extends JsonDeserializer<LtuGiornaliereRawDto> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public LtuGiornaliereRawDto deserialize(JsonParser jp, DeserializationContext dc) throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);
        LtuGiornaliereRawDto ltuGiornalereDto = LtuGiornaliereRawDto.builder().build();
        try {
            ltuGiornalereDto.codPdf = node.get("COD_PDF").isNull() ? null : node.get("COD_PDF").asText();
            ltuGiornalereDto.codTipoFornitura = node.get("COD_TIPO_FORNITURA").isNull() ? null : node.get("COD_TIPO_FORNITURA").asText();
            ltuGiornalereDto.codPdm = node.get("COD_PDM").isNull() ? null : node.get("COD_PDM").asText();
            ltuGiornalereDto.datLettura = dateFromParsedString(node.get("DAT_LETTURA").asText());
            ltuGiornalereDto.codTipVoceLtu = node.get("COD_TIP_VOCE_LTU").isNull() ? null : node.get("COD_TIP_VOCE_LTU").asText();
            ltuGiornalereDto.numMtcAppar = node.get("NUM_MTC_APPAR").isNull() ? null : node.get("NUM_MTC_APPAR").asText();
            ltuGiornalereDto.codTipLtuGio = node.get("COD_TIP_LTU_GIO").isNull() ? null : node.get("COD_TIP_LTU_GIO").asText();
            ltuGiornalereDto.quaLettura = integerFromParsedString(node.get("QUA_LETTURA").asText());
            ltuGiornalereDto.datLtuPrecedente = dateFromParsedString(node.get("DAT_LTU_PRECEDENTE").asText());
            ltuGiornalereDto.quaLtuPrecedente = integerFromParsedString(node.get("QUA_LTU_PRECEDENTE").asText());
            ltuGiornalereDto.codFlgValida = node.get("COD_FLG_VALIDA").isNull() ? null : node.get("COD_FLG_VALIDA").asText();
            ltuGiornalereDto.codFlgRettificata = node.get("COD_FLG_RETTIFICATA").isNull() ? null : node.get("COD_FLG_RETTIFICATA").asText();
            ltuGiornalereDto.codTipoFonteLtuGio = node.get("COD_TIPO_FONTE_LTU_GIO").isNull() ? null : node.get("COD_TIPO_FONTE_LTU_GIO").asText();
            ltuGiornalereDto.codFlgQuadrata = node.get("COD_FLG_QUADRATA").isNull() ? null : node.get("COD_FLG_QUADRATA").asText();
            ltuGiornalereDto.codAnomalia = node.get("COD_ANOMALIA").isNull() ? null : node.get("COD_ANOMALIA").asText();
            ltuGiornalereDto.datAcquisizioneLtu = dateFromParsedString(node.get("DAT_ACQUISIZIONE_LTU").asText());
            ltuGiornalereDto.datPubblicazioneLtu = dateFromParsedString(node.get("DAT_PUBBLICAZIONE_LTU").asText());
            ltuGiornalereDto.quaLtuPublic = integerFromParsedString(node.get("QUA_LTU_PUBLIC").asText());
            ltuGiornalereDto.datCreazioneRec = dateFromParsedString(node.get("DAT_CREAZIONE_REC").asText());
            ltuGiornalereDto.datUltAggRec = dateFromParsedString(node.get("DAT_ULT_AGG_REC").asText());
            ltuGiornalereDto.codOperatore = node.get("COD_OPERATORE").isNull() ? null : node.get("COD_OPERATORE").asText();
            ltuGiornalereDto.numMtcApparNew = node.get("NUM_MTC_APPAR_NEW").isNull() ? null : node.get("NUM_MTC_APPAR_NEW").asText();
            ltuGiornalereDto.codTipLtuGioNew = node.get("COD_TIP_LTU_GIO_NEW").isNull() ? null : node.get("COD_TIP_LTU_GIO_NEW").asText();
            ltuGiornalereDto.quaLetturaNew = integerFromParsedString(node.get("QUA_LETTURA_NEW").asText());
            ltuGiornalereDto.quaLtuPrdNew = integerFromParsedString(node.get("QUA_LTU_PRD_NEW").asText());
            ltuGiornalereDto.quaLtuScsNew = integerFromParsedString(node.get("QUA_LTU_SCS_NEW").asText());
            ltuGiornalereDto.codTipoFonteLtuGioNew = node.get("COD_TIPO_FONTE_LTU_GIO_NEW").isNull() ? null : node.get("COD_TIPO_FONTE_LTU_GIO_NEW").asText();
            ltuGiornalereDto.codAnomaliaNew = node.get("COD_ANOMALIA_NEW").isNull() ? null : node.get("COD_ANOMALIA_NEW").asText();
            ltuGiornalereDto.codTipoStatoFinNew = node.get("COD_TIPO_STATO_FIN_NEW").isNull() ? null : node.get("COD_TIPO_STATO_FIN_NEW").asText();
            ltuGiornalereDto.codFlgRetPbl = node.get("COD_FLG_RET_PBL").isNull() ? null : node.get("COD_FLG_RET_PBL").asText();
            ltuGiornalereDto.datForzatura = dateFromParsedString(node.get("DAT_FORZATURA").asText());
            ltuGiornalereDto.codFlgForzata = node.get("COD_FLG_FORZATA").isNull() ? null : node.get("COD_FLG_FORZATA").asText();
        } catch (Exception e) {
            log.error("Exception while parsing raw message");
        }
        log.debug("\nReceived content:\n{}\nParsed Object:\n{}", node.toPrettyString(), ltuGiornalereDto.toString());
        return ltuGiornalereDto;
    }

    private Date dateFromParsedString(String dateStr) {
        if (dateStr.equals("null")) return null;
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(dateStr);

        } catch (ParseException e) {
            return null;
        }
    }

    private Integer integerFromParsedString(String intStr) {
        if (intStr.equals("null")) return null;
        return Integer.parseInt(intStr);
    }
}
