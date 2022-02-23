package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import com.fasterxml.jackson.core.JacksonException;
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

public class GuastoDeserializer extends JsonDeserializer<Guasto> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public Guasto deserialize(JsonParser jp, DeserializationContext dc) throws IOException {
        Guasto guasto = Guasto.builder().build();
        JsonNode node = jp.getCodec().readTree(jp);
        try {
            guasto.codPdf = node.get("COD_PDF").isNull() ? null : node.get("COD_PDF").asText();
            guasto.codTipoFornitura = node.get("COD_TIPO_FORNITURA").isNull() ? null : node.get("COD_TIPO_FORNITURA").asText();
            guasto.codPdm = node.get("COD_PDM").isNull() ? null : node.get("COD_PDM").asText();
            guasto.datFinGuasto = dateFromParsedString(node.get("DAT_FIN_GUASTO").asText());
            guasto.datInoGuasto = dateFromParsedString(node.get("DAT_INO_GUASTO").asText());
            guasto.codCausaleGst = node.get("COD_CAUSALE_GST").isNull() ? null : node.get("COD_CAUSALE_GST").asText();
            guasto.desNoteGuasto = node.get("DES_NOTE_GUASTO").isNull() ? null : node.get("DES_NOTE_GUASTO").asText();
            guasto.datCreazioneRec = dateFromParsedString(node.get("DAT_CREAZIONE_REC").asText());
            guasto.datUltAggRec = dateFromParsedString(node.get("DAT_ULT_AGG_REC").asText());
            guasto.codOperatore = node.get("COD_OPERATORE").isNull() ? null : node.get("COD_OPERATORE").asText();
        } catch (Exception e) {
            log.error("Exception while parsing raw message");
        }
        log.debug("\nReceived content:\n{}\nParsed Object:\n{}", node.toPrettyString(), guasto.toString());
        return guasto;
    }


    private Date dateFromParsedString(String dateStr) {
        if (dateStr.equals("null")) return null;
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }
}
