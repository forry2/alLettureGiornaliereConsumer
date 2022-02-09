package com.dxc.curvegas.alletturegiornaliereconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class AlLettureGiornaliereConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlLettureGiornaliereConsumerApplication.class, args);
    }

}
