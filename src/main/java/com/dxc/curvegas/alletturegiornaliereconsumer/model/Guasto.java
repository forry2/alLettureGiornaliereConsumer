package com.dxc.curvegas.alletturegiornaliereconsumer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class Guasto {
    String codPdf;
    String codTipoFornitura;
    String codPdm;
    Date datInoGuasto;
    Date datFinGuasto;
    String codCausaleGst;
    String desNoteGuasto;
    Date datCreazioneRec;
    Date datUltAggRec;
    String codOperatore;
}
